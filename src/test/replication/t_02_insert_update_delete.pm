package t_02_insert_update_delete;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

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
				prepare_script => 'prepare.sql'
			});

		foreach my $id (qw(0 1 2)) {
			$slave[$id] = MammothTest::init_new_pg_node($self, {
					name => "slave$id",
					init => 1,
					additional_config => "slave$id.conf",
					prepare_script => 'prepare.sql'
				});
		}

		$mcp = MammothTest::run_new_mcp($self);

		$slave[0]->start(1);
		$master->start(1);
		$self->execute({
				file => "prepare_master.sql",
				servers => [$master]
		    });
		$self->background_execute($master, "insert.sql");
		$slave[1]->start(1);
		$self->background_execute($master, "update.sql");
		$self->background_execute($master, "delete.sql");
		$slave[2]->start(1);

		$master->wait_background_jobs;

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => \@slave
			});

		$self->execute_and_compare({
				file => 'check.sql',
				servers => [$master, @slave]
			});
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
