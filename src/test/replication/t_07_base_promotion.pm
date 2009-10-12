package t_07_base_promotion;

use BaseTest;
use MammothTest;
use strict;

our @ISA = qw(BaseTest);
									
sub run
{
	my $self = shift;
	$self->SUPER::run;
	
	eval {
		my ($master, $slave0, $slave2, $mcp);	
		my $promote_query = "PROMOTE";
		my $select_query = "SELECT * FROM test1 ORDER BY id";
		my $insert_query = "INSERT INTO test1(data) values('master')";

		$self->clean_output;

		$master = MammothTest::new_pg_node($self, "master");
		$slave0 = MammothTest::new_pg_node($self, "slave0");
		$slave2 = MammothTest::new_pg_node($self, "slave2");

		$master->initdb;
		$slave0->initdb;
		$slave2->initdb;
		
		$master->append_config('master.conf');
		$slave0->append_config('slave0.conf');
		$slave2->append_config('slave2.conf');

		$self->standalone_execute($master, 'prepare.sql');
		$self->standalone_execute($slave0, 'prepare.sql');
		$self->standalone_execute($slave2, 'prepare.sql');

		$mcp = MammothTest::run_new_mcp($self);
		$master->start(1);
		$self->execute({
					   	file => "prepare_master.sql",
						servers => [$master]
					   });

		$slave0->start(1);
		$slave2->start(1);	
		
		$self->execute({
						 string => $insert_query,
						 name => 'insert1',
						 servers => [$master]
						});
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave2]
			});
		$self->execute_and_compare({
								    string => $select_query,
									name => 'select1',
									servers => [$master, $slave0, $slave2]
								   });

		print "promote slave0\n";
		# promote slave0 to master
		$self->execute({
					    string => $promote_query,
						name => 'promote_slave0',
						servers => [$slave0],
						stderr_match => 'promote.err.expected'
					   });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave2]
			});
		$self->check_promotion({
							    promoted => $slave0,
								demoted => $master
							   });
		# insert additional record and check if it replicates
		$self->execute({
					    string => $insert_query,
						name => 'insert2',
						servers => [$slave0]
					   });

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave2]
			});
		$self->execute_and_compare({
								   string => $select_query,
								   name => 'select2',
				 				   servers => [$slave0, $master, $slave2]
								  });				   
		
	
		print "reverse promotion of slave0\n";
		#promote old master to master back
		$self->execute({
					    string => $promote_query,
						name => 'promote_master',
						servers => [$master],
						stderr_match => 'promote.err.expected'
					   });
		
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave2]
			});
		$self->check_promotion({
							    promoted => $master,
								demoted => $slave0
							   });

		$self->execute({
				   		string => $insert_query,
						name => 'insert3',
				 		servers => [$master]
					   });
		
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave2]
			});
		$self->execute_and_compare({
								    string => $select_query,
									name => 'select3',
									servers => [$master, $slave0, $slave2]
								   });
			
		print "promote slave2 (not in mcp_promote_allow)\n";
		# try to promote slave2 - not allowed
		$self->execute({
					    string => $promote_query,
						name => 'promote_slave2',
						servers => [$slave2],
						stderr_match => 'promote.err.expected'
					   });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave2]
			});
		$self->check_promotion({
							    promoted => $slave2,
								demoted => $master,
								success => 0
							   });

	};
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	};
}

1;
