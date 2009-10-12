package t_04_user_group_replication;

# This tests user replication

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

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
		
		my @slaves = ($slave0, $slave1, $slave2);

		$master->initdb;
		$slave0->initdb;
		$slave1->initdb;
		$slave2->initdb;

		$master->append_config;
		$slave0->append_config;		
		$slave1->append_config;
		$slave2->append_config;
		
		# create roles and add role members on master
		$self->standalone_execute($master, "prepare.sql");
		
		# create some roles on the slave, so we can what will happen if
		# roles from master will have the same name as the slave's ones.
		$self->standalone_execute($slave0, "prepare_slave0.sql");

		$master->start(1);
		$self->execute({file => "prepare_master.sql",
						servers => [$master]});

		# note that the order of node launches is important here
		# forwarder (mcp) is launched after the master to force
		# the roles to appear in the full dump.
		 
		$mcp = MammothTest::run_new_mcp($self);						
		$slave0->start(1);
		$slave1->start(1);
		$slave2->start(1);
		
		# wait for full dump completion.		
		MammothTest::wait_replication_complete({master => $master, 
											    slaves => \@slaves});
		
		# check that roles and members were replicated
		$self->check_roles_equal($master, @slaves);
		$self->check_members_equal($master, @slaves);

		# check whether revoke role produces equal resuls
		$self->exec_and_test_replication({file => "revoke.sql",
										  master => $master,
										  slaves => [@slaves],
										  check => "check_auth_members.sql"
										 });
		
		# set different role's parameters and check that they 
		# are replicated correctly.
		$self->exec_and_test_replication({file => "alter_role_foo.sql",
										  master => $master,
										  slaves => [@slaves],
										  check => "check_authid.sql"
										  });
										
		# check alter role set
		$self->exec_and_test_replication({string => 'ALTER ROLE baz SET '.
		 										  	'statement_timeout TO 500',
										  name => "alter_role_foo_set",
										  master => $master,
										  slaves => [@slaves],
										  check => "check_authid.sql",
										  });
		
		# try role renaming
		$self->exec_and_test_replication({string => 'ALTER ROLE foo RENAME TO ' 
										  			 .'\\"123foo.=5-3\\"',
										  name => "rename_role_foo",
										  master => $master,
										  slaves => [@slaves],
										  check => "check_authid.sql",
										  stderr_match =>
											'rename_role_foo.master.expected'
										  });
										
		$self->exec_and_test_replication({file => "drop_roles.sql",
										  master => $master,
										  slaves => [@slaves],
										  check => "check_authid.sql"
										  });
										
		# and check that auth_members is the same on all nodes
		$self->check_members_equal($master, @slaves);
		
		# XXX: check renaming a role on master to the role that already exists
		# on slave.									

	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}


sub check_roles_equal {
	my $self = shift;
	my $master = shift;
	my @slaves = shift;
	
	$self->execute_and_compare({ file => "check_authid.sql",
								 servers => [$master, @slaves]});
}

sub check_members_equal {
	my $self = shift;
	my $master = shift;
	my @slaves = shift;
	
	$self->execute_and_compare({file => "check_auth_members.sql",
								servers => [$master, @slaves]});
}
