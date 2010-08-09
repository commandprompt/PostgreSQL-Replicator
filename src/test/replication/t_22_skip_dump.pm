# Test that a slave that is in-sync can skip full dump.
package t_22_skip_dump;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

sub run
{
    my $self = shift;

    $self->SUPER::run;

    eval {
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

        $self->standalone_execute($master, 'prepare.sql');
        $self->standalone_execute($slave0, 'prepare.sql');
        $self->standalone_execute($slave1, 'prepare.sql');

		$master->start(1);
		$self->execute({
			file => 'prepare_master.sql',
			servers => [$master]
		});
		
		$mcp = MammothTest::run_new_mcp($self);
		$slave0->start(1);
		$slave1->start(1);
		
		MammothTest::wait_replication_complete({
			master => $master,
			slaves => [$slave0, $slave1]
		});
		
		$slave0->stop;
		$slave1->stop;
		
		# start slaves in offline mode
		$slave0->start(0);
		$slave1->start(0);
		
		$self->execute({
			string => 'INSERT INTO test22 VALUES(2)',
			name => 'insert_slave0',
			servers => [$slave0]
		});
		$self->execute({
			string => 'INSERT INTO test22 VALUES(3)',
			name => 'insert_slave1',
			servers => [$slave1]
		});
		
		$slave0->stop;
		$slave1->stop;
		
		$slave0->start(1);
		$slave1->start(1);
		
		$self->execute({
			string => 'INSERT INTO test22 VALUES(2)',
			name => 'insert_master',
			servers => [$master]
		});
		
		$self->execute({
			string => 'ALTER SLAVE REQUEST DUMP',
			name => 'slave0_request_dump',
			servers => [$slave0]
		});
		
		$self->execute({
			string => 'ALTER SLAVE RESUME RESTORE',
			name => 'slave0_resume_restore',
			servers => [$slave0]
		});
		
		MammothTest::wait_replication_complete({
			master => $master,
			slaves => [$slave0, $slave1]
		});
		
		$self->execute_and_compare({
			file => 'select_test22.sql',
			servers => [$master, $slave0]
		});
		$self->execute_and_compare_not_equal('select_test22.sql', $master, $slave1);
	};
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
1;
