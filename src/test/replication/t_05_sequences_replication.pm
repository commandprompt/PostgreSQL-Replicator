package t_05_sequences_replication;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

# This test checks sequence replication, both as a standalone sequence
# and that of a SERIAL column.

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
		my ($master, $slave0, $slave1, $slave2, $mcp);

		$self->clean_output;

		$master = MammothTest::new_pg_node($self, 'master');
		$slave0 = MammothTest::new_pg_node($self, 'slave0');
		$slave1 = MammothTest::new_pg_node($self, 'slave1');
		$slave2 = MammothTest::new_pg_node($self, 'slave2');

		$master->initdb;
		$slave0->initdb;
		$slave1->initdb;
		$slave2->initdb;

		$master->append_config;
		$slave0->append_config;
		$slave1->append_config;
		$slave2->append_config;

		$self->standalone_execute($master, "prepare.sql");
		$self->standalone_execute($slave0, "prepare.sql");
		$self->standalone_execute($slave1, "prepare.sql");
		$self->standalone_execute($slave2, "prepare.sql");

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$self->execute({
				file => "prepare_master.sql",
				servers => [$master]
		    });
		$slave0->start(1);
		$slave1->start(1);
		$slave2->start(1);

		$self->execute({
				file => "execute.sql",
				servers => [$master]
		    });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1, $slave2]
			});
		$self->execute_and_compare({
				file => 'check.sql',
				servers => [$master, $slave0, $slave1, $slave2]
			});

		print "Checking if sequence data is propagated to the slave after rollback:\n";
		$self->execute({
				file => "execute_rollback.sql",
				servers => [$master]
		    });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1, $slave2]
			});
		$self->execute_and_compare({
				file => 'check.sql',
				servers => [$master, $slave0, $slave1, $slave2]
			});
	};
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
