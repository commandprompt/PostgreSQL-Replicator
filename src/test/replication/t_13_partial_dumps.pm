# A test of partial dumps.
package t_13_partial_dumps;

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

        $self->standalone_execute($master, 'prepare_master.sql');
        $self->standalone_execute($slave0, 'prepare.sql');
        $self->standalone_execute($slave1, 'prepare.sql');

        # master should produce a full dump with replicated table data instead
        # of per-table dumps for every replicated tables, the data should be
        # already on master before MCP would ask for a dump.
        #
        $master->start(1);
        $self->execute({
        		file => "enable_tab1_all.sql",
        		servers => [$master]
            });

        print "Checking if full dump produces equal results on slaves...\n";
        # start MCP server and the slaves, full dump should happen there
        $mcp = MammothTest::run_new_mcp($self);
        $slave0->start(1);
        $slave1->start(2);

        # check that slave's contents of tab1 are equal to the master's
        MammothTest::wait_replication_complete({
        		master => $master,
        		slaves => [$slave0, $slave1]
        	});
        $self->background_execute($master, 'check_tab1.sql');
        $self->background_execute($slave0, 'check_tab1.sql');
        $self->background_execute($slave1, 'check_tab1.sql');

        foreach my $server ($master, $slave0, $slave1) {
            $server->wait_background_jobs;
        }

        $self->compare('check_tab1.sql.master', 'check_tab1.sql.slave0')
            or die 'check_tabl1.sql.master differs from check_tab1.sql.slave0';
        $self->compare('check_tab1.sql.master', 'check_tab1.sql.slave1')
            or die 'check_tabl1.sql.master differs from check_tab1.sql.slave1';

        # get tabl1 contents with oids for a slave0 
        $self->execute({
        		file => "check_tab1_oid.sql",
        		servers => [$slave0]
            });

        printf "Checking if per-table dump request for slave0 doesn't lead to the full dump...\n";
        # test per-table dump for tab2 
        # enable tab2 only for slave0
        $self->execute({
        		file => "enable_tab2_slave0.sql",
        		servers => [$master]
            });

        MammothTest::wait_replication_complete({
        		master => $master,
        		slaves => [$slave0, $slave1]
        	});
        # now check that tab2 is equal on master and slave0 but not on slave1
        $self->background_execute($master, 'check_tab2.sql');
        $self->background_execute($slave0, 'check_tab2.sql');
        $self->background_execute($slave1, 'check_tab2.sql');
        # get the contents of tab1 with oids from slave 0
        $self->background_execute($slave0, 'check_tab1_oid_latest.sql');

        foreach my $server ($master, $slave0, $slave1) {
            $server->wait_background_jobs;
        }

        # should be equal
        $self->compare('check_tab2.sql.master', 'check_tab2.sql.slave0')
             or die 'check_tab2.sql.master differs from check_tab2.sql.slave0';
        # should not be equal
        die 'check_tab2.sql.slave0 equals to check_tab2.sql.slave1' if
            $self->compare('check_tab2.sql.slave0', 'check_tab2.sql.slave1');

        # tab1 should not be touched by per-table dump for tab1
        $self->compare('check_tab1_oid.sql.slave0', 'check_tab1_oid_latest.sql.slave0')
            or die 'check_tab1_oid.sql.slave0 differs from check_tab1_oid_latest.sql.slave0';
        
        # check that per-table dump is cached on MCP
        print "Checking that per-table dump requests are cached on the MCP server...\n";
        # disconnect slave1
        $slave1->stop;
        
        # add table test3 for both slave0 and slave1
        $self->execute({
        		file => "enable_tab3_all.sql",
        		servers => [$master]
            });
        # wait until slave0 receives it
        MammothTest::wait_replication_complete({
        		master => $master,
        		slaves => [$slave0]
        	});

        # should be equal
        $self->background_execute($master, 'check_tab3.sql');
        $self->background_execute($slave0, 'check_tab3.sql');

        foreach my $server ($master, $slave0) {
            $server->wait_background_jobs;
        }

        $self->compare('check_tab3.sql.master', 'check_tab3.sql.slave0')
            or die 'check_tab3.sql.master differs from check_tab3.sql.slave0';

        # now stop master and connect slave1. Thus we check that a request for a new
        # table that will come from slave1 to MCP will be processed solely by MCP.
        my $target_recno = MammothTest::_master_get_latest_recno($master);
        $master->stop;
        $slave1->start(1);

        MammothTest::wait_replication_complete({
        		master_recno_target => $target_recno,
        		slaves => [$slave1]
        	});
        $self->execute({
        		file => "check_tab3.sql",
        		servers => [$slave1]
            });
        
        # should be equal to slave0
        $self->compare('check_tab3.sql.slave0', 'check_tab3.sql.slave1')
            or die ('check_tab3.sql.slave0 differs from check_tab3.sql.slave1');

    };
    if ($@) {
        $self->fail($@);
    } else {
        $self->succeed;
    }
}

1;

    


