#!/usr/bin/perl -w
#
# $Id: test.pl 2172 2009-06-02 23:19:11Z alvherre $

use strict;

BEGIN {
	use Config::File;

	delete $ENV{LANG};
	$ENV{LC_ALL} = 'C';
	$main::config = Config::File::read_config_file('test.conf');
}

use Test::Cmd;

use OPgsql;


my ($v,$d,$f) = File::Spec->splitpath(Cwd::abs_path($0));
my $pwd = File::Spec->catpath($v, $d, '');
my @failed = ();

my $SSL = 0;
my $testdb = $OPgsql::database;

my $sched = Schedule::init();

while (my $testname = $sched->next_item) {

	# The tests call chdir() into whatever they want -- we should fix that.  In
	# the meantime, we just move to the correct directory before each test.
	chdir $pwd;
	my $module = $testname . ".pm";
	die "$module not present" unless -f $module;

	require $module;	
	
	read_replace_write('master.conf.tmpl', 'master.conf', {
			'DATABASE' => $testdb,
			'PORT' => $main::config->{master}{port}
		});

	my $mcpkey = 'foobar';
	
	read_replace_write('forwarder.conf.tmpl', 'forwarder.conf', {
			'AUTHKEY' => $mcpkey,
			'PORT' => $main::config->{forwarder}{port}
		});

	foreach my $slave (qw(0 1 2)) {
		my $slaveid = "slave$slave";
		read_replace_write('slave.conf.tmpl', "slave${slave}.conf", {
				'SLAVE_ID' => $slave,
				'PORT' => $main::config->{$slaveid}{port},
				'AUTHKEY' => $mcpkey,
				'DATABASE' => $testdb
			});
	}

	foreach my $node (qw(master slave0 slave1 slave2)) {
		read_replace_write('fw_settings.sql.tmpl', "${node}.sql", {
					'AUTHKEY' => $mcpkey,
					'SSL' => $SSL
		});
	}

	# And finally, run the test
	my $test = $testname->new('basedir' => $pwd);
	eval {
		$test->run();
	};
	if ($@) {
		push @failed, $testname;
		print $@;
	}
}

if (@failed) {
	print "\033[31mFailed tests:\033[0m @failed\n";
	exit(1);
}

# read a file, replace some tokens from it, and write it back.
# First param is the file name to read; the second is the file to write.
# The third param is a hash ref where the keys are the tokens to replace,
# and the hash values are the values with which to replace them.
sub read_replace_write {
	my $input = shift;
	my $output = shift;
	my $repl = shift;
	my $contents;
	my $k;
	my $test;

	# Note hack: we must pass absolute paths, because otherwise Test::Cmd
	# qualifies the filename with the workdir of the test (which is a temp
	# dir), so it fails to find the actual file.
	$test = Test::Cmd->new();
	$input = $pwd."/".$input;
	$output = $pwd."/".$output;

	$test->read(\$contents, $input)
		or die "can't read $input: $!";
	foreach $k (keys %$repl) {
		$contents =~ s/$k/$repl->{$k}/;
	}
	$test->write($output, $contents)
		or die "can't write $output: $!";
}

package Schedule;

sub init {
	if (defined $ARGV[0]) {
		return Schedule::Cmdline->init;
	}

	foreach my $sched ("$pwd/test_schedule.custom", "$pwd/test_schedule") {
		if (-f "$sched") {
			return Schedule::File->init($sched);
		}
	}

	die "no schedule found";
}

sub next_item {
	die "must use a subpackage";
}

package Schedule::Cmdline;

sub init {
	my $class = shift;
	my $self = {};
	$self->{count} = 0;
	return bless $self, $class;
}

sub next_item {
	my $self = shift;
	if (defined $ARGV[$self->{count}]) {
		return $ARGV[$self->{count}++];
	}
	return undef;
}

package Schedule::File;

sub init {
	my $class = shift;
	my $file = shift;
	my $self = {};
	bless $self, $class;
	open $self->{SCHED}, $file or die "cannot open schedule file $file: $!";

	return $self;
}

sub next_item {
	my $self = shift;
	
	my $file = $self->{SCHED};

	while (<$file>) {
		# skip comments
		next if (m/^\s*#/);
		# skip empty lines
		next if (m/^$/);
		# strip comments from remaining lines
		s/([^\s]*)\s*#.*/$1/;

		chomp;

		if (!m{(^[[:alnum:]_]+$)}) {
			die "invalid line {$_}\n";
		}

		return $_;
	}

	return undef;
}

