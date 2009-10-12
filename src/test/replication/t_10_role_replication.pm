package t_10_role_replication;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

# This tests for GRANT/REVOKE replication

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval
	{
		my ($master, $slave0, $slave1, $mcp);
		
		$self->clean_output;

		$master = MammothTest::new_pg_node($self, 'master');
		$slave0 = MammothTest::new_pg_node($self, 'slave0');
		$slave1 = MammothTest::new_pg_node($self, 'slave1');

		$master->initdb;
		$slave0->initdb;
		$slave1->initdb;

		$master->append_config('master.conf');
		$slave0->append_config('slave0.conf');
		$slave1->append_config('slave1.conf');

		$self->standalone_execute($master, "prepare.sql");
		$self->standalone_execute($slave0, "prepare.sql");
		$self->standalone_execute($slave1, "prepare.sql");

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$slave0->start(1);
		$slave1->start(1);

# check if create role replication works
		print "Checking if newly created roles are replicated...\n";

		$self->execute({
				file => "execute1.sql",
				servers => [$master]
		    });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		$self->execute_and_compare({
				file => 'select1.sql',
				servers => [$master, $slave0, $slave1]
			});

# check if we replicate grant/revoke on roles/relations
		print "Checking replication of role membership and GRANT/REVOKE ".
				"statements on relations...\n";

		$self->execute({
				file => "execute2.sql",
				servers => [$master]
		    });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		$self->execute_and_compare({
				file => 'select2.sql',
				servers => [$master, $slave0, $slave1]
			});

# check ALTER ROLE replication
		print "Checking if ALTER ROLE results are replicated...\n";

		$self->execute({
				file => "execute3.sql",
				servers => [$master]
		    });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		$self->execute_and_compare({
				file => 'select3.sql',
				servers => [$master, $slave0, $slave1]
			});

# check if REVOKE ALL on relation works
		print "Checking if REVOKE ALL on relation is replicated...\n";

		$self->execute({
				file => "execute4.sql",
				servers => [$master]
		    });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		$self->execute_and_compare({
				file => 'select4.sql',
				servers => [$master, $slave0, $slave1]
			});
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
