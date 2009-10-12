package dropped_columns;

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
				prepare_script => 'prepare_master.sql'
			});

		foreach my $id (qw(0 1 2)) {
			$slave[$id] = MammothTest::init_new_pg_node($self, {
					name => "slave$id",
					init => 1,
					additional_config => "slave$id.conf",
					prepare_script => 'prepare_slave.sql'
				});
		}
		`echo '123' > /tmp/test.data`;

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);
		$slave[0]->start(1);
		$slave[1]->start(1);
		$slave[2]->start(1);

		$self->execute({
				file => "enable.sql",
				servers => [$master]
		    });

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => \@slave
			});

		$self->execute_and_compare({
				file => 'check.sql',
				servers => [$master, @slave]
			});
		
		foreach my $id (0, 1, 2) {
			$self->execute({
					file => 'lo_export.sql',
					servers => [$slave[$id]]
			});
			(system("diff", "/tmp/test.data", "/tmp/test_slave.data") == 0) 
			 or die "large objects doesn't match for slave$id";
		}		
	};

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
