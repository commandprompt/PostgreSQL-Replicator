package t_08_restart_replication_after_ddl;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

# This test checks that it works correctly to disable replication for a table,
# change its definition, and enable replication again.

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
		my ($master, @slave, $mcp);

		$self->clean_output;

		$master = MammothTest::init_new_pg_node($self, {
				name => 'master',
				init => 1,
				additional_config => 'master.conf',
				prepare_script => '1-prepare.sql'
			});

		foreach my $id (qw(0 1 2)) {
			$slave[$id] = MammothTest::init_new_pg_node($self, {
					name => "slave$id",
					init => 1,
					additional_config => "slave$id.conf",
					prepare_script => '1-prepare.sql'
				});
		}

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$self->execute({
				file => "2-prepare-master.sql",
				servers => [$master]
		    });
		$slave[0]->start(1);
		$slave[1]->start(1);
		$slave[2]->start(1);

		$self->execute({
				file => "3-insert.sql",
				servers => [$master]
		    });

		# should fail
		$self->execute({
				name => 'alter-table-1',
				string => 'alter table test9 add column b int',
				servers => [$master, @slave],
				stderr_match => 'alter-table-expected'
		    });

		$self->execute({
				file => "4-disable.sql",
				servers => [$master]
		    });

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => \@slave
			});

		$self->execute({
				name => 'alter-table-2',
				string => 'alter table test9 add column b int',
				servers => [$master, @slave]
		    });

		$self->execute({
				file => "6-enable.sql",
				servers => [$master]
		    });
		$self->execute({
				file => "7-insert.sql",
				servers => [$master]
		    });

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => \@slave
			});

		$self->background_execute($master, '7a-check.sql');
		foreach my $slave (@slave) {
			$self->background_execute($slave, '7a-check.sql');
		}

		foreach my $server ($master, @slave) {
			$server->wait_background_jobs;
		}

		$self->compare('7a-check.sql.master', '7a-check.sql.slave0')
			or die "7a-check.sql.slave0 differs from 7a-check.sql.master";
		$self->compare('7a-check.sql.master', '7a-check.sql.slave1')
			or die "7a-check.sql.slave1 differs from 7a-check.sql.master";
		$self->compare('7a-check.sql.master', '7a-check.sql.slave2')
			or die "7a-check.sql.slave2 differs from 7a-check.sql.master";

		$self->execute({
				file => "8-update.sql",
				servers => [$master]
		    });

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => \@slave
			});

		$self->background_execute($master, '9-check.sql');
		foreach my $slave (@slave) {
			$self->background_execute($slave, '9-check.sql');
		}
		foreach my $server ($master, @slave) {
			$server->wait_background_jobs;
		}

		$self->compare('9-check.sql.master', '9-check.sql.slave0')
			or die "9-check.sql.slave0 differs from 9-check.sql.master";
		$self->compare('9-check.sql.master', '9-check.sql.slave1')
			or die "9-check.sql.slave1 differs from 9-check.sql.master";
		$self->compare('9-check.sql.master', '9-check.sql.slave2')
			or die "9-check.sql.slave2 differs from 9-check.sql.master";

	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
