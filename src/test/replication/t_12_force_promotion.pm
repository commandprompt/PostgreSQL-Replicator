package t_12_force_promotion;

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
		
		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$self->background_execute($master, 'prepare_master.sql');
		$slave0->start(1);
		$slave1->start(1);
	
		$master->wait_background_jobs;
		$self->execute({
				file => "insert.sql",
				servers => [$master]
		    });	

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		$self->execute_and_compare({
				file => 'checktest1.sql',
				servers => [$master, $slave0, $slave1]
			});

		# call promote force with a connected master - should have no effect

		$self->execute({
				servers => [$slave0],
				file => 'promote_force.sql',
				stderr_match => 'promote.err.slave0.expected'
			});

		printf ("Waiting for the master check to complete...\n");

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		$self->execute({
				file => "insert.sql",
				servers => [$master]
		    });
		
		# should be prohibited
		$self->execute({
				servers => [$slave0],
				file => 'insert.sql',
				stderr_match => 'insert.sql.err.slave0.expected'
			});

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});
		
		# stop master and try to force promote slave0

		$master->stop;

		$self->execute({
			file => "promote_force.sql",
			servers => [$slave0],
			stderr_match => 'promote.err.slave0.expected'
		});

		print ("Waiting for the master check to complete...\n");
		sleep 2;

		$self->execute({
				file => "insert.sql",
				servers => [$slave0]
		    });
		MammothTest::wait_replication_complete({
				master => $slave0,
				slaves => [$slave1]
			});
		$self->partial_success("force promotion of slave0 passed"); 
		
		# try to promote back slave0 - not allowed, because promotion stack is 
		# cleared after force promotion.

		$self->execute({
				servers => [$slave0],
				file => "promote_back.sql",
				stderr_match => 'promote.err.slave0.expected'
			});
		sleep 2;

		$self->execute({
				file => "insert.sql",
				servers => [$slave0]
		    });

		#select * from test1 on master, slave0, slave1
		MammothTest::wait_replication_complete({
				master => $slave0,
				slaves => [$slave1]
			});
		$self->execute_and_compare({
				file => 'checktest1.sql',
				servers => [$slave0, $slave1]
			});

		#stop slave0, start master and slave0
		print "Restarting promoted slave... \n";

		$slave0->stop;
		$slave0->start(1);

		$self->execute({
				file => "insert.sql",
				servers => [$slave0]
		    });
		print "Checking if promoted slave still acts as master after restart...\n";
		MammothTest::wait_replication_complete({
				master => $slave0,
				slaves => [$slave1]
			});
		$self->execute_and_compare({
				file => 'checktest1.sql',
				servers => [$slave0, $slave1]
			});
	};
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
