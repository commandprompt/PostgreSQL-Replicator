package t_14_slave_errors_handling;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
        my ($master, $mcp, $slave0, $slave1);

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
        # insert a couple of records into staff and presence
        $self->execute({
        		file => "prepare_master.sql",
        		servers => [$master]
            });
        # enable replication for staff and presence
		$self->execute({
				file => "enable_replication.sql",
				servers => [$master]
		    });

        $mcp = MammothTest::run_new_mcp($self);

        $slave0->start(1);
        $slave1->start(1);

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

        # check whether replication produces equal results on each node
		$self->execute_and_compare({
				file => "select_all.sql",
				servers => [$master, $slave0, $slave1]
			});

        # stop master and start it in 'offline' mode
        $master->stop;
        $master->start(0);

        # insert a couple more records in offline mode
        $self->execute({
        		file => "execute_master_offline.sql",
        		servers => [$master]
            });

        # stop master and start it in 'online' mode. Content of replicated
        # relations won't be equal due to the recent offline inserts.

        $master->stop;
        $master->start(1);

        # make sure slaves won't receive updates now
        $slave0->stop;
        $slave1->stop;

        # now do evil. Send updates for the tuples on master which slaves are
        # not aware of. This should raise errors on slaves and produce full
        # dump.
		$self->execute({
				servers => [$master],
				string => 'update presence set present = TRUE where present = FALSE',
				name => 'update_presence'
			});

        # insert the control records
        $self->execute({
				servers => [$master],
				string => "insert into staff values(4, 'alvherre')",
				name => 'add_new_staff4'
			});
        $self->execute({
				servers => [$master],
				string => "insert into staff values(5, 'alexk')",
				name => 'add_new_staff5'
			});
        $self->execute({
				servers => [$master],
				string => "insert into staff values(6, 'andrei')",
				name => 'add_new_staff6'
			});

        $self->execute({
        		file => "check_new_staff.sql",
        		servers => [$master]
            });

        # Wait for a couple of second. It usually takes much less to
        # replicate the recent updates from master to MCP. We can't use usual
        # checks since we don't have slaves yet.
        sleep(1);

        # now stop master and start slaves. They should raise an error and ask for a
        # full dump. That request won't be satisfied since we have no master.
        $master->stop;
        $slave0->start(1);
        $slave1->start(1);

        # the essence of this test. Check whether we don't have the latest record on the
        # slaves. It should be skipped because the queue is not in sync after full
        # dump request. Notice the sleep - can't use wait_repl_complete because it is not
        # expected to be completed (slaves should have frecno one less of master's).
        #
        sleep(3);
        $self->execute({
        		file => "check_new_staff.sql",
        		servers => [$slave0, $slave1]
            });

        # master should not be equal to the slave0
        die "check_new_staff.sql.master is equal to check_new_staff.sql.slave0" if
            $self->compare('check_new_staff.sql.master', 'check_new_staff.sql.slave0');

        # and slaves should be equal
        die "check_new_staff.sql.slave0 is not equal to check_new_staff.sql.slave1" unless
            $self->compare('check_new_staff.sql.slave0', 'check_new_staff.sql.slave1');

        $self->partial_success("check whether the data is not replicated on queue desync passed");

        # start master and make sure that the data is now equal
        $master->start(1);

        MammothTest::wait_replication_complete({
        		master => $master,
        		slaves => [$slave0, $slave1]
        	});
        
        # check whether replication produces equal results on each node
		$self->execute_and_compare({
				file => "select_all.sql",
				servers => [$master, $slave0, $slave1]
			});
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}

1;
