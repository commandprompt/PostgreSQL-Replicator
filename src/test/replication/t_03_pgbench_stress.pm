package t_03_pgbench_stress;

use MammothTest;
use BaseTest;
use strict;

our @ISA = qw(BaseTest);

our $scale = 10;
our $transactions = 100;
our $connections = $scale;

sub run
{
	my $self = shift;

	$self->SUPER::run;
	
	eval {
		
		my ($master, $slave0, $slave1, $slave2, $mcp);

		my $testdir = $self->dirname;
		my $testname = $self->testname;
		my $outputdir = $self->outputdir;
		my $master_port;

		my $pgbench = $OPgsql::pgroot."/bin/pgbench";

		$self->clean_output;

		if (! -e $pgbench)
		{
			print "pgbench not found at $pgbench.\n";
			print "Now skipping this test.\n";
			return;
		}

		$master = MammothTest::new_pg_node($self, 'master');
		$slave0 = MammothTest::new_pg_node($self, 'slave0');
		$slave1 = MammothTest::new_pg_node($self, 'slave1');
		$slave2 = MammothTest::new_pg_node($self, 'slave2');

		$master->initdb;
		$slave0->initdb;
		$slave1->initdb;
		$slave2->initdb;

		$master->append_config('master.conf');
		$slave0->append_config('slave0.conf');
		$slave1->append_config('slave1.conf');
		$slave2->append_config('slave2.conf');

		$master->start(0);
		$slave0->start(0);
		$slave1->start(0);
		$slave2->start(0);

		# XXX: make this consistent with output names from other tests
		printf "initializing pgbench with scale $scale:\n";
		my $cmdtmpl = "$pgbench -i -s $scale -d mammoth -p %d >>$outputdir/%d.prepare.log 2>&1";

		$master_port = $master->port;

		foreach ($master_port, $slave0->port, $slave1->port, $slave2->port) {
			my $cmd = sprintf($cmdtmpl, $_, $_);
			print "creating pgbench tables in server at $_ ... ";
			`$cmd`;
			print "done\n";
		}

		$master->stop;
		$slave0->stop;
		$slave1->stop;
		$slave2->stop;

		$mcp = MammothTest::run_new_mcp($self);

		$master->start(1);

		$slave0->start(1);
		$slave1->start(1);
		$slave2->start(1);

		print "Storing initial data with pgbench\n";
		`$pgbench -I -s $scale -p $master_port mammoth >> $outputdir/"${master_port}.init.log" 2>&1`;

		print "Enabling replication of pgbench tables\n";
		$self->execute({
				file => 'prepare_master.sql',
				servers => [$master]
			});

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1, $slave2]
			});
		$self->execute_and_compare({
				file => 'check.sql',
				servers => [$master, $slave0, $slave1, $slave2]
			});

		print "Running pgbench with $connections connections/$transactions transactions\n";
		`$pgbench -c $connections -s $scale -t $transactions -p $master_port mammoth \
			>>${outputdir}/${master_port}.store.log 2>&1`;

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1, $slave2]
			});
		$self->execute_and_compare({
				file => 'check.sql',
				servers => [$master, $slave0, $slave1, $slave2]
			});
	};	

	if ($@) {
		$self->fail($@);
	} else {
		$self->succeed;
	}
}
