package t_17_forwarder_encoding_conversion;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;

	$self->SUPER::run;

	eval {
		my ($master, $mcp);
		my $slave;
		my $i;

		$self->clean_output;

		$master = MammothTest::new_pg_node($self, 'master');
		$slave = MammothTest::new_pg_node($self, "slave0");

# initialize the master and the slave with different, but compatible locales
		$master->initdb({locale=>'ru_RU.CP1251'});;
		$slave->initdb({locale=>'ru_RU.KOI8-R'});

		$master->append_config('master.conf');
		$master->append_config('test18.conf');

		$slave->append_config('slave0.conf');
		$slave->append_config('test18.conf');

		$self->standalone_execute($master, "prepare.sql");
		$self->standalone_execute($slave, 'prepare.sql');

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$self->execute({
				file => "prepare_master.sql",
				servers => [$master]
		    });
		$self->background_execute($master, 'insert_win1251.sql');

		$slave->start(1);

		$master->wait_background_jobs;
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave]
			});

		$self->background_execute($master, 'check.sql');
		$self->background_execute($slave, 'check.sql');

		foreach my $server ($master, $slave) {
			$server->wait_background_jobs;
		}

		$self->compare('check.sql.master', "check.sql.slave0") 
			or die "check.sql.slave0 differs from check.sql.master";
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}

1;
