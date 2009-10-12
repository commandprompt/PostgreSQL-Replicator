package t_09_promote_back;

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

		my $insert_query = "INSERT INTO test1(data) VALUES('master')";
		my $select_query = "SELECT * FROM test1 ORDER BY id";
		my $promote_query = "PROMOTE";
		my $promote_back_query = "PROMOTE BACK";

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

		$mcp = MammothTest::run_new_mcp($self);
		
		$master->start(1);
		$self->background_execute($master, 'prepare_master.sql');
		$slave0->start(1);
		$slave1->start(1);

		$master->wait_background_jobs;

		#waiting for tablelist replication to complete
		$self->execute({
					   	string => $insert_query,
						name => 'insert1',
					   	servers => [$master]
					   });
		
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		$self->execute_and_compare({
								   	string => $select_query,
									name => 'select1',
									servers => [$master, $slave0, $slave1]
								   });
		
		#PROMOTE BACK with empty stack
	   	check_empty_promotion_stack($self, $master, $slave0, $slave1);
 
		print "promote slave0 \n";
		# promote slave0 to master
		$self->execute({
					   	string => $promote_query,
						name => 'promote1',
						servers => [$slave0],
						stderr_match => 'promote.err.expected'
					   });

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		$self->check_promotion({
						   		promoted => $slave0,
						 		demoted => $master
							   });

#Promote slave0 (old master) to master back from stack

		print "reverse promotion of slave0\n";
		$self->execute({
					    string => $promote_back_query,
						name => 'promote_back_slave0_1',
						servers => [$master],
						stderr_match => 'promote.err.expected'
					   });
						
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
	
		$self->check_promotion({
						   		promoted => $master,
						 		demoted => $slave0
							   });
	
		# check whether promotion stack is empty
		check_empty_promotion_stack($self, $master, $slave0, $slave1);	
		
		#Promote slave0 to master again

		print "promote slave0\n";
		$self->execute({
				   		string => $promote_query,
				 		name => 'promote_slave0',
						servers => [$slave0],
						stderr_match => 'promote.err.expected'
					   });

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		$self->check_promotion({
							    demoted => $master,
								promoted => $slave0
							   });

		# try back promote slave1 - not allowed
		print "promote slave1 which is not in mcp_promote_allowed list\n";
		$self->execute({
				   		string => $promote_back_query,
				 		name => 'promote_back_slave0_2',
						servers => [$slave1],
						stderr_match => 'promote.err.expected'
					   });
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		$self->check_promotion({
							    demoted => $slave0,
								promoted => $slave1,
								success => 0
							   });
		
		# promote slave0 (old master) to master back from stack
		print "reverse promotion of slave0\n";	
		$self->execute({
					    string => $promote_back_query,
						name => 'promote_back_master',
						servers => [$master],
						stderr_match => 'promote.err.expected'
					   });
		
		MammothTest::wait_replication_complete({
						master => $master,
						slaves => [$slave0, $slave1]
		});			
		
		$self->check_promotion({
						   		demoted => $slave0,
						 		promoted => $master
							   });		
	
	};
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}

sub check_empty_promotion_stack
{
	my $test = shift;
	my ($master, $slave0, $slave1) = @_;
	
	#PROMOTE BACK with empty stack
	print "check for back promotion with an empty promotion stack\n";
	$test->execute({
				   	string => 'PROMOTE BACK',
					name => 'promote_back_empty',
					servers => [$master],
					stderr_match => 'promote.err.expected'
				   });

	# XXX: usage of wait_replication complete is not justified
	# since this is promotion, not replication.

	MammothTest::wait_replication_complete({
			master => $master,
			slaves => [$slave0, $slave1]
		});	
	
	$test->check_promotion({
					   		promoted => $slave0,
					 		demoted=> $master,
							success => 0
						   });	
}    

