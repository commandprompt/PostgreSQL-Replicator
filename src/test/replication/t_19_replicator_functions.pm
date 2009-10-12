package t_19_replicator_functions;

# test replicator functions from $PGDATA/share/replicator-functions.sql

use MammothTest;
use BaseTest;
use Carp;

use strict;

our @ISA = qw(BaseTest);

use constant NUMCHECKS => 5;

sub run {
	my $self = shift;
	$self->SUPER::run;
	
	eval {
		my ($master);
		
		$self->clean_output;
		$master = MammothTest::new_pg_node($self, 'master');
		$master->initdb;
		$master->append_config('master.conf');
		
		$master->start(1);
		
		
		# create test tables 
		$self->execute({
				file => 'prepare.sql',
				servers => [$master],
				stderr_match => 'prepare.sql.expected'
		});

		# load replication functions
		(-f "$self->{dirname}/replicator-functions.sql") or 
		`ln -s $OPgsql::pgroot/share/replicator-functions.sql "$self->{dirname}/replicator-functions.sql"`;

		$self->execute({
			file => 'replicator-functions.sql',
			servers => [$master]
		});
		
		$self->compare_result({
			node => $master,
			file => 'check.sql',
			name => 'check-initial'
		});
		
		for (my $i = 1; $i <= NUMCHECKS; $i++) {
			$self->execute({
					file => "execute-$i.sql",
					servers => [$master]
			});
		
			$self->compare_result({
					node => $master,
					file => 'check.sql',
					name => "check-$i"
			});
		}		
	};
	
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}

sub compare_result {
	my $self = shift;
	
	my $opt = shift;
	
	my $node = $opt->{node};
	my $file = $opt->{file};
	my $name = $opt->{name} || $file;
	
	$self->execute({
			file => $file,
			name => $name,
			servers => [$node]
	});
	
	my $result = "$name.".$node->name;
	my $expected = "../$name".".expected";
	
	$self->compare($result, $expected)
		|| confess "$result differs from $expected";	
}

1;
