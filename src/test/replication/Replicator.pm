package Replicator;

use OPgsql;
use Carp;

our @ISA = qw(OPgsql);

sub BEGIN
{
	die "$OPgsql::pgroot is not a valid Replicator installation"
		unless -d $OPgsql::pgroot and -x "$OPgsql::pgroot/bin/init-mammoth-database";
}

sub new
{
	my $class = shift;
	my $args = shift;
	
	my $self = $class->SUPER::new($args);
	
	# don't set forwarder connection options in the forwarder process itself
	if ($self->name eq "forwarder") {
		$self->{forwarder_conf} = undef;
	} else {
		$self->{forwarder_conf} = $args->{forwarder_conf};
	
		# check whether the options file exists
		confess "forwarder_conf is not defined" 
			unless defined $self->{forwarder_conf};
		confess "$self->{forwarder_conf} does not exist" 
			unless -r $self->{forwarder_conf};
	}
	return $self;
}

sub initdb
{
	my $self = shift;

	$self->SUPER::initdb(@_);

	qx|$OPgsql::pgroot/bin/init-mammoth-database -D $self->{datadir} $OPgsql::database|
		or die "cannot exec initdb: $!";
}

# prepare startup arguments for replicator
sub start_args
{
	my $self = shift;
	my $args = shift;
	
	my $replication = $args->{replication} || 0;
	
	my @cmd = $self->SUPER::start_args;
	
	if (defined $replication and $replication) {
		push @cmd, qw(-c replication_enable=true);
	}
		
	return @cmd; 
}

# start replicator node and set forwarder connection parameters
sub start
{
	my $self = shift;
	my $replication = shift || 0;
	
	# start PostgreSQL node
	$self->SUPER::start({replication => $replication});
	
	# add forwarder connection settings
	if (defined $self->{forwarder_conf})  {
		my $file = $self->{forwarder_conf};
		my $errfile = "$self->{datadir}/${file}.err";
		
		$self->execute($file, "$self->{datadir}/${file}.out",
		 			   $errfile);		
		if (not -z $errfile) {
			die "stderr $errfile not empty while executing $file";
		}
		$self->{forwarder_conf} = undef;
	}
}


sub replication_mode
{
	my $self = shift;
	return $self->execute_result("SHOW replication_mode");
}

1;
