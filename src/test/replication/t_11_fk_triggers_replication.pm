package t_11_fk_triggers_replication;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;
	$self->SUPER::run;
	
	eval {
		
		my ($master, $slave0, $mcp);
		$self->clean_output;

		$master = MammothTest::new_pg_node($self, 'master');
		$slave0 = MammothTest::new_pg_node($self, 'slave0');
		
		$master->initdb;
		$slave0->initdb;

		$master->append_config('master.conf');
		$slave0->append_config('slave0.conf');

		$self->standalone_execute($master, "prepare.sql");
		$self->standalone_execute($slave0, "prepare.sql");
		
		$mcp = MammothTest::run_new_mcp($self);
	
		$master->start(1);
		$self->execute({
				file => 'prepare_master.sql',
				servers => [$master], 
				stderr_match => 'prepare_master.err.expected'
			});
		$slave0->start(1);
	
		$self->execute({
				file => "execute.sql",
				servers => [$master]
		    });
		$self->background_execute($master, "execute2.sql");

		$master->wait_background_jobs;
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0]
			});
	
		foreach (1..4) {
			$self->execute_and_compare({
					file => "check$_.sql",
					servers => [$master, $slave0]
				});
		}
		$self->execute_and_compare_not_equal("check5.sql", $master, $slave0);
		
		# we don't replicate table test5, because it doesn't have a primary key
		# the fact that data in table5 is equal on master and on the slave would
		# would mean that triggers are called during replication, which is a bug
	};
	
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
