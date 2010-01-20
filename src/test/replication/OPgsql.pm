package OPgsql;

use strict;

use Carp;
use Cwd;
use File::Copy;
use File::Path;
use File::Spec;
use File::Temp 0.18 ();
use Proc::Background;
use Time::HiRes qw(usleep);

our $pgroot;

sub BEGIN {
	my $pgdata;

	# PGROOT environment variable, or test[pgroot] in config file, or hardcode
	# "/usr/local/pgsql"
	$OPgsql::pgroot = defined $ENV{PGROOT} ? $ENV{PGROOT} :
		defined $main::config->{test}{pgroot} ? $main::config->{test}{pgroot} :
		"/usr/local/pgsql";
	$OPgsql::database = defined $main::config->{test}{database} ? $main::config->{test}{database} :
		"mammoth";

	$OPgsql::tempinstall = File::Temp::tempdir(CLEANUP => 1);

	$pgdata = $OPgsql::tempinstall . "/inst";

	die "$OPgsql::pgroot is not a valid PostgreSQL installation"
		unless -d $OPgsql::pgroot and -x "$OPgsql::pgroot/bin/initdb";

	qx{$OPgsql::pgroot/bin/initdb -D $pgdata -A trust}
		or die "cannot exec initdb: $!";
	qx{echo "create database $OPgsql::database" | $OPgsql::pgroot/bin/postgres --single -D $pgdata template1}
		or die "cannot create database: $!";
}

sub DESTROY
{
	my $self = shift;

	$self->stop;
}

sub new
{
	my $class = shift;
	my $args = shift;

	my $self = {};
	$self->{name} = $args->{name};
	$self->{port} = $args->{port};
	$self->{datadir} = $args->{datadir};
	$self->{logfile} = $args->{logfile};

	foreach my $prop (qw(name port datadir logfile)) {
		confess "$prop not defined" unless defined $self->{$prop};
	}

	return bless $self, $class;
}

sub name
{
	my $self = shift;
	return $self->{name};
}

sub port
{
	my $self = shift;
	return $self->{port};
}

sub datadir
{
	my $self = shift;
	return $self->{datadir};
}

sub logfile
{
	my $self = shift;
	return $self->{logfile};
}

# prepare startup arguments for postgres
sub start_args
{
	my $self = shift;
	
	my @cmd = ();
	
	push @cmd, "$OPgsql::pgroot/bin/postmaster";
	push @cmd, "-D", $self->{datadir};
	push @cmd, "-p", $self->{port};
	push @cmd, qw(-c logging_collector=off);
	
	return @cmd; 
}

sub initdb
{
	my $self = shift;
	my $opts = shift;
	my $args = "";
	my ($sysid, $nextoid);

	confess "postmaster running in $self->{datadir}"
		if -e "$self->{datadir}/postmaster.pid";

	rmtree($self->{datadir}, 0, 1) or confess "rmtree $self->{datadir}: $!"
		if (-e $self->{datadir});

	if (defined $opts->{locale}) {
		$args .= "--locale=$opts->{locale}";
	}

	print "Initializing database $self->{name}\n";
	if ($args eq "") {
		qx|cp -r $OPgsql::tempinstall/inst $self->{datadir}|;
	} else {
		qx|$OPgsql::pgroot/bin/initdb -D $self->{datadir} -A trust $args|
			or die "cannot exec initdb: $!";
		qx(echo "create database $OPgsql::database" | $OPgsql::pgroot/bin/postgres --single -D $self->{datadir} template1)
			or die "cannot create database: $!";
	}

	foreach my $file ("server.crt", "server.key") {
		copy($file, $self->{datadir}) or die "cannot copy $file: $!";
		my $path = $self->{datadir} . "/" . $file;
		if ((chmod 0700, $path) == 0) {
			die "could not chmod $path";
		}
	}
	
	my @pgcontrolout = `pg_resetxlog -n $self->{datadir}`;
	foreach (@pgcontrolout) {
		$sysid = $1 if (/^Database system identifier:\s+(\d+)/);
		$nextoid = $1 if (/^Latest checkpoint's NextOID:\s+(\d+)/);
	}
	# randomize sysid and nextoid values
	$nextoid += int(rand(10000));
	$sysid += time;
	`pg_resetxlog -s $sysid -o $nextoid $self->{datadir}`;
}

sub start
{
	my $self = shift;
	my $startup = shift;
		
	my @cmd = $self->start_args($startup);

	print "Starting database '$self->{name}' ...";

	# Redirect stdout and stderr.  stderr goes to the logfile, stdout does not
	# matter.
	open my $oldout, ">&STDOUT"	or die "can't dup stdout: $!";
	open my $olderr, ">&STDERR" or die "can't dup stderr: $!";

	open STDERR, '>>', $self->{logfile} or die "can't redirect stderr: $!";
	open STDOUT, '>>', $self->{datadir}."/stdout" or die "can't redirect stdout: $!";

	# make both unbuffered
	select STDERR; $| = 1;
	select STDOUT; $| = 1;

	$self->{pmproc} = Proc::Background->new({ 'die_upon_destroy' => 1}, @cmd);

	open STDOUT, ">>&", $oldout or die "can't dup \$oldout: $!";
	open STDERR, ">>&", $olderr or die "can't dup \$olderr: $!";

	# Now sleep until the postmaster is able to serve requests
	my $cmd = $OPgsql::pgroot."/bin/psql $OPgsql::database -p $self->{port} -tA -c 'select 1' 2>/dev/null";

	# sleep at most 5 seconds waiting for the postmaster to start
	for (1 .. 50) {
		usleep 10_000;
		my $ret = `$cmd`;
		last if $ret =~ /^1/;
		if (!$self->{pmproc}->alive) {
			confess sprintf "postmaster stopped with exit status %d\nPlease see %s for the reason\n",
			($self->{pmproc}->wait >> 8),
			$self->{logfile};
		}
		print "."
	}

	print " done\n";
	sleep 2;
}

sub stop
{
	my $self = shift;

	return if not defined $self->{pmproc};
	print "Stopping database '$self->{name}': ";
	
	if (not $self->{pmproc}->alive) {
		print "not running\n";
		return;
	}

	$self->wait_background_jobs;

	kill 'INT', $self->{pmproc}->pid;
	$self->{pmproc}->wait;
	$self->{pmproc} = undef;
	print "Stopped\n";
}

# Execute a SQL script against a running server, and waits until it has
# completed.  The second and third arguments are file names that, if present,
# receive the output of stdout and stderr, respectively.
sub execute
{
	my $self = shift;
	my $file = shift;
	my $stdout = shift;
	my $stderr = shift;

	my ($v, $d, $f) = File::Spec->splitpath($file);

	print "executing $f in $self->{name}\n";

	confess "$file not found or not readable" unless -f $file and -r $file;
	$stdout = "/dev/null" unless defined $stdout;
	$stderr = "/dev/null" unless defined $stderr;
	
	my $cmd = "$OPgsql::pgroot/bin/psql -e -p $self->{port} $OPgsql::database -f $file > $stdout 2> $stderr";

	qx/$cmd/;
}

sub execute_result
{
	my $self = shift;
	my $query = shift;
	my $tmph = File::Temp->new(UNLINK => 0);
	my $tmp = $tmph->filename;

	my $result;
	my $cmd = qq|$OPgsql::pgroot/bin/psql -tA -p $self->{port} $OPgsql::database -c "$query" 2> $tmp|;

	$result = qx/$cmd/;

	die "$tmp not empty executing \"$query\"" if not -z $tmp;

	unlink $tmp;
	chomp $result;
	return $result;
}

sub execute_string
{
	my $self = shift;
	my $string = shift;
	my $stdout = shift;
	my $stderr = shift;
	my $name = shift;

	print "executing string $name in $self->{name}\n";

	$stdout = "/dev/null" unless defined $stdout;
	$stderr = "/dev/null" unless defined $stderr;
	
	my $cmd = qq|$OPgsql::pgroot/bin/psql -e -p $self->{port} $OPgsql::database -c "$string" > $stdout 2> $stderr|;

	qx/$cmd/;
}

# Execute a SQL script against a running server, in background mode.  stdout
# is saved to the named file, stderr is discarded.
sub background_execute
{
	my $self = shift;
	my $file = shift;
	my $output = shift;

	my ($v, $d, $f) = File::Spec->splitpath($file);

	print "executing $f in background in $self->{name}\n";

	my $cmd = "$OPgsql::pgroot/bin/psql -e -p $self->{port} $OPgsql::database -f $file > $output 2> /dev/null";

	push @{$self->{bgjobs}}, Proc::Background->new($cmd);
}

sub wait_background_jobs
{
	my $self = shift;
	my $waited = 0;
	my $printed = 0;

	while (my $job = shift @{$self->{bgjobs}}) {
		if (not $printed) {
			print "waiting for job";
			$printed = 1;
		}
		print " ", $job->pid;
		my $wait = $job->wait;
		printf " (code %d)", $wait << 8;
		$waited++;
	}

	print "\t$waited jobs done\n" if $waited;
}


# Execute the given script file in a standalone backend, in the
# $OPgsql::database database.  Note: on the output, we save the stderr rather
# than stdout, because stdout is the useless prompting and stderr contains all
# the logs about created stuff, etc.
sub standalone_execute
{
	my $self = shift;
	my $file = shift;
	my $output = shift;
	my $stdin;

	if (defined $self->{pmproc}) {
		confess "standalone_execute called with running postmaster";
	}

	$output = "/dev/null" unless defined $output;

	my $cmd = sprintf "%s/bin/postgres --single -D %s %s < %s > /dev/null 2> %s",
	$OPgsql::pgroot, $self->{datadir}, $OPgsql::database, $file, $output;

	qx/$cmd/;
}

# Append a file to this node's config file.
sub append_config
{
	my $self = shift;
	my $config = shift || $self->name.".conf";
	
	my $dir = $self->{datadir};

	qx{cat $config >> $dir/postgresql.conf};
}

1;
