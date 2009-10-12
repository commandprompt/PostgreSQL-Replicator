package t_15_inherited_tables_replication;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;
	$self->SUPER::run;
	
	eval {
		
		my ($master, $slave0, $mcp);
		$self->clean_output;

		$master = MammothTest::new_pg_node($self, 'master');
		$slave0 = MammothTest::new_pg_node($self, 'slave0');
		
		$master->initdb;
		$slave0->initdb;

		$master->append_config('master.conf');
		$slave0->append_config('slave0.conf');

		$self->standalone_execute($master, "prepare.sql");
		$self->standalone_execute($slave0, "prepare.sql");
		
		$mcp = MammothTest::run_new_mcp($self);
	
		
		$master->start(1);
		
		# capture the rebels and send them to slavery
		$self->execute({
				file => 'prepare_master.sql',
				servers => [$master] 
			});
		$slave0->start(1);

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0]
			});

		# make sure all rebels are delivered	
		$self->execute_and_compare({
				 file => 'check_rebels.sql',
		  		 servers => [$master, $slave0]
			});

		# now jedi try to appear, should raise an error because we are
		# trying to inherit a non-replicated table from a replicated one
		# (it's too dangerous yet for the jedi)
		
		$self->execute({
				 string => 'CREATE TABLE jedi(force INTEGER) INHERITS (rebels)',
				 name => 'create_jedi_inh',
				 servers => [$master, $slave0],
				 stderr_match => 'create_jedi_inh.expected'
				});
		
		# trick the evil empire - jedi are not rebels, they are just like
		# rebels. This would allow creation of a new table

		$self->execute({
	  			 file => 'create_jedi_like.sql',
				 servers => [$master, $slave0],
				 stderr_match => 'create_jedi_like.expected'
				});			 
		
		# train some jedi
		$self->execute({
				 file => 'train_jedi.sql',
				 servers => [$master]
				 });
		
		 
		# try to alter non-replicated to inherit from a replicated one.
		# should fail.(jedi reveal themself as rebels, but without success)
		$self->execute({
				file => "alter_jedi_inh.sql",
				servers => [$master, $slave0],
				stderr_match => 'alter_jedi_inh.expected'
		    });
	
		# replicate jedi to a set of slaves different from rebels.
		$self->execute({
				file => 'replicate_jedi1.sql',
				servers => [$master]
			});
		
		# try to inherit, fail. Note that the master and the slave
		# would produce a different error message, that's why we
		# are checking only master side
		$self->execute({
				file => "alter_jedi_inh.sql",
			    servers => [$master],
				stderr_match => 'alter_jedi_inh2.expected'
			});
		
		# send jedi to slave0
		$self->execute({
				string => 'ALTER TABLE jedi ENABLE REPLICATION ON SLAVE 0',
		 		name => 'replicate_jedi_slave0',
				servers => [$master]
			});
				
		# try to inherit once more, should be successfull
		$self->execute({
				file => 'alter_jedi_inh.sql',
				servers => [$master, $slave0],
			});

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0]
			});

		# all jedi should be enslaved. Hail the dark side!
		$self->execute_and_compare({
				file => 'check_jedi.sql',
				servers => [$master, $slave0],
			});
	};
	
	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
