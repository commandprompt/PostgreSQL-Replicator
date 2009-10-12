package t_06_replicate_non_existent_tables;

use MammothTest;
use BaseTest;
use strict;

# This tests that it works to create a table in the master, then enable
# replication for it, and only create it in the slave later.
#
our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
		my ($master, $mcp, $slave0);

		$self->clean_output;

		$master = MammothTest::new_pg_node($self, 'master');
		$slave0 = MammothTest::new_pg_node($self, 'slave0');

		$master->initdb;
		$slave0->initdb;

		$master->append_config('master.conf');
		$slave0->append_config('slave0.conf');

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$slave0->start(1);

		$self->execute({
				servers => [$master, $slave0],
				string => 'create table verify (a serial primary key)',
				name => 'verify_master', 
				stderr_match => 'create_verify.err.expected'
			});
		$self->execute({
				servers => [$master],
				string => 'alter table verify enable replication',
				name => 'enable_replication_master'
			});
		$self->execute({
				servers => [$master],
				string => 'alter table verify enable replication on slave 0',
				name => 'enable_replication_slave0'
			});
		$self->execute({
				servers => [$master],
				string => 'insert into verify values (1)',
				name => 'insert1'
			});

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0]
			});
		$self->execute_and_compare({
				file => 'check.sql',
				servers => [$master, $slave0]
			});

		# now create our problem table
		$self->execute({
				servers => [$master],
				file => "create.sql",
				stderr_match => 'create_test.err.expected'
			});

		$self->execute({
				servers => [$master],
				string => 'insert into verify values (2)',
				name => 'insert2'
			});

		# don't wait here just yet
		sleep 1;

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

