package t_01_datatypes_replication;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

use constant NUMSLAVES => 3;

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
		my ($master, $mcp);
		my @slaves;
		my $i;

		$self->clean_output;

		$master = MammothTest::new_pg_node($self, 'master');
		for ($i = 0; $i < NUMSLAVES; $i++) {
			$slaves[$i] = MammothTest::new_pg_node($self, "slave$i");
		}

		$master->initdb;
		foreach my $slave (@slaves) {
			$slave->initdb;
		}

		$master->append_config('master.conf');
		for ($i = 0; $i < NUMSLAVES; $i++) {
			$slaves[$i]->append_config("slave$i.conf");
		}

		$self->standalone_execute($master, "prepare.sql");
		foreach my $slave (@slaves) {
			$self->standalone_execute($slave, "prepare.sql");
		}

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$self->execute({
				file => "prepare_master.sql",
				servers => [$master]
		    });
		$self->background_execute($master, 'execute.sql');

		foreach my $slave (@slaves) {
			$slave->start(1);
		}

		$master->wait_background_jobs;
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => \@slaves
			});

		$self->background_execute($master, 'check.sql');
		foreach my $slave (@slaves) {
			$self->background_execute($slave, 'check.sql');
		}

		foreach my $server ($master, @slaves) {
			$server->wait_background_jobs;
		}
		
		for ($i = 0; $i < NUMSLAVES; $i++) {
			$self->compare('check.sql.master', "check.sql.slave$i")
				or die "check.sql.slave$i differs from check.sql.master";
		}
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}

1;
