package t_18_forwarder_restart;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

# Check that MCP server restarts don't break replication

sub run
{
	my $self = shift;
	
	$self->SUPER::run;
	
	eval {
		my ($master, $slave0, $mcp);
		
		$self->clean_output;
		
		$master = MammothTest::new_pg_node($self, "master");
		$slave0 = MammothTest::new_pg_node($self, "slave0");
		
		$master->initdb;
		$slave0->initdb;
		
		$master->append_config;
		$slave0->append_config;
		
		$self->standalone_execute($master, "prepare.sql");
		$self->standalone_execute($slave0, "prepare.sql");
		
		$mcp = MammothTest::run_new_mcp($self);
		
		$master->start(1);
# enable replication for table test.
		$self->execute({ file => "prepare_master.sql", servers => [$master]});
# bring the master down
		$master->stop;
# start the slave, wait for some time for it to ask table dump for test
		$slave0->start(1);
		sleep 3;
# stop mcp server, wait to make sure it stopped
		$mcp->stop;
		sleep 2;
# start mcp server and bring back master
		$mcp->start;
		$master->start(1);
# insert data into the test
		$self->execute({string => "INSERT INTO test VALUES(1)",
						name => "insert_master",
						servers => [$master]
					   });
# check - make sure that the slave received the data
		MammothTest::wait_replication_complete({
			master => $master,
			slaves => [$slave0]
		});
		
		$self->execute_and_compare({
			file => 'check.sql',
			servers => [$master, $slave0]
		});
	};
	
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
