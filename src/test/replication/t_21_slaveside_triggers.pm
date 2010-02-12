package t_21_slaveside_triggers;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
		my ($master, $slave, $mcp);

		$self->clean_output;

		$master = MammothTest::init_new_pg_node($self, {
				name => 'master',
				init => 1,
				additional_config => 'master.conf',
				prepare_script => 'prepare.sql'
			});

		$slave = MammothTest::init_new_pg_node($self, {
				name => "slave0",
				init => 1,
				additional_config => 'slave0.conf',
				prepare_script => 'prepare.sql'
			});

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$self->background_execute($master, 'prepare_master.sql');
		
		$slave->start(1);
		$self->background_execute($slave, 'prepare_slave.sql');

		$master->wait_background_jobs;
		$slave->wait_background_jobs;

		$self->execute({
			servers => [$master],
			file => 'execute.sql'
		});

		MammothTest::wait_replication_complete({
			master => $master,
			slaves => [$slave]
		});
		
		$self->execute_and_compare({
			file => 'select_main_table.sql',
			servers => [$master, $slave]
		});
		
		$self->execute_and_compare_not_equal(
			"select_aux_table.sql", 
			$master, $slave
		);
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
