package t_16_large_objects;

use MammothTest;
use BaseTest;
use strict;
use warnings;

our @ISA = qw(BaseTest);

sub run
{
	my $self = shift;

	$self->SUPER::run;

	my $failed_code;
	my $failed_no;

	# Test parameters
	my $stop_on_fail = 1; # set it to 1 to stop and not remove results after the first failure
	my $total = 10;
	my $smallcount = $total/2;
	my $smallsize = 1024;
	my $bigsize = $smallsize * 100;
	my $dir = "/tmp";
	my $sqldir = $self->{dirname};
	my $target = "repl_lo_test_dest";
	my $source = "repl_lo_test_origin";
	my $total_slaves = 2;

	# queries
	my $lo_count = "select count(distinct loid) from pg_catalog.pg_largeobject";
	my $lo_unlink = qq{SELECT lo_unlink((select loid from
		pg_catalog.repl_master_lo_refs GROUP BY loid LIMIT 1))
		};

	makedata({
			total => $total,
			num_slaves => $total_slaves,
			smallsize => $smallsize,
			bigsize => $bigsize,
			smallcount => $smallcount,
			dir => $dir,
			sqldir => $sqldir,
			source => $source,
			target => $target
		});
		
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

		$self->execute({
				servers => [$master],
				file => 'insert.sql'
			});

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		$self->execute({
				servers => [$master],
				file => "select_master.sql"
			});
		$self->execute({
				servers => [$master],
				file => "select_slave0.sql"
			});
		$self->execute({
				servers => [$master],
				file => "select_slave1.sql"
			});

		check_lo_result($total, $dir, $target, $total_slaves);
		$self->partial_success("LO_INSERT: PASSED");

		`rm -rf $dir/$target/master/*`;
		for my $i (0 .. $total_slaves - 1) {
		    `rm -rf $dir/$target/slave$i/*`;
		}

		$self->execute({
				servers => [$master],
				file => "update.sql"
			});

		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		$self->execute({
				servers => [$master],
				file => "select_master.sql"
			});
		$self->execute({
				servers => [$slave0],
				file => "select_slave0.sql"
			});
		$self->execute({
				servers => [$slave1],
				file => "select_slave1.sql"
			});

		check_lo_result($total, $dir, $target, $total_slaves);
		$self->partial_success("LO_UPDATE: slave PASSED");

		`rm -rf $dir/$target/master/*`;

		for my $i (0 .. $total_slaves - 1) {
		    `rm -rf $dir/$target/slave$i/*`;
		}

		$self->execute({
				servers => [$master],
				string => $lo_unlink,
				name => 'lo_unlink1',
				stderr_match => 'lo_unlink.expected'
			});
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		my $out = $self->execute({
				servers => [$master],
				string => $lo_count,
				name => 'lo_count1'
			});
		my $expected = 'lo_count.expected.1';
		die "could not match $out to $expected"
			unless $self->match($out, $expected);
		$out = $self->execute({
				servers => [$slave0],
				string => $lo_count,
				name => 'lo_count1'
			});
		die "could not match $out to lo_count.expected.1"
			unless $self->match($out, $expected);

		$self->partial_success("LO_DELETE NON_EMPTY: PASSED");

		$self->execute({
				servers => [$master],
				string => "delete from test1",
				name => 'delete'
			});
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		$self->execute({
				servers => [$master],
				string => $lo_unlink,
				name => 'lo_unlink2'
			});
		MammothTest::wait_replication_complete({
				master => $master,
				slaves => [$slave0, $slave1]
			});

		$out = $self->execute({
				servers => [$master],
				string => $lo_count,
				name => 'lo_count2'
			});
		$expected = 'lo_count.expected.2';
		die "could not match $out to $expected"
			unless $self->match($out, $expected);
		$out = $self->execute({
				servers => [$slave0],
				string => $lo_count,
				name => 'lo_count2'
			});
		die "could not match $out to $expected"
			unless $self->match($out, $expected);

		$self->partial_success("LO_DELETE: PASSED");

		# truncate should fail because lo is enabled for a column
		$self->execute({
				servers => [$master],
				file => "clean_master.sql",
				stderr_match => 'clean_master.expected'
		});
			
		$self->execute({
				servers => [$master],
				file => 'disable_lo.sql',
		});
		
		# truncate should succeed. 
		MammothTest::wait_replication_complete({
			master => $master,
			slaves => [$slave0, $slave1]
		});
		
		$self->execute({
			servers => [$master],
			file => "clean_master.sql"
		});
		
		$self->partial_success("LO_TRUNCATE: PASSED");

		`rm -rf $dir/$source $dir/$target`;
		`rm -rf $sqldir/insert.sql $sqldir/update.sql $sqldir/select_master.sql`;
		for my $i (0 .. $total_slaves - 1) {
		    `rm -rf $sqldir/select_slave$i.sql`;
		    `rm -rf $dir/$target/slave$i/*`;
		}
	};

    if ($@) {
        $self->fail($@);
    } else {
        $self->succeed;
    }
}

sub makedata
{
	my $opts = shift;

	my $total = $opts->{total};
	my $total_slaves = $opts->{num_slaves};
	my $smallsize = $opts->{smallsize};
	my $bigsize = $opts->{bigsize};
	my $smallcount = $opts->{smallcount};
	my $dir = $opts->{dir};
	my $sqldir = $opts->{sqldir};
	my $source = $opts->{source};
	my $target = $opts->{target};

	die unless defined $dir and $dir ne "";

	`rm -rf $dir/$source $dir/$target >/dev/null 2>/dev/null`;

	mkdir "$dir/$source" or die "can't mkdir $dir/$source: $!";
	mkdir "$dir/$target" or die "can't mkdir $dir/$target: $!";
	mkdir "$dir/$target/master" or die "can't mkdir $dir/$target/master: $!";

	for my $i (0 .. $total_slaves - 1) {
		mkdir "$dir/$target/slave$i" or die "can't mkdir $dir/$target/slave$i: $!";
	}

	foreach my $file (qw(insert.sql update.sql select_master.sql)) {
		next unless -e "$sqldir/$file";
		unlink "$sqldir/$file" or die "can't unlink $sqldir/$file: $!";
	}

	for my $i (0 .. $total_slaves - 1) {
		my $file = "sqldir/select_slave$i.sql";
		next unless -e $file;
		unlink $file or die "can't unlink $file: $!";
	}

	makedatapart({
			from => 0,
			to => $smallcount,
			size => $smallsize,
			slaves => $total_slaves,
			dir => $dir,
			source => $source,
			target => $target,
			sqldir => $sqldir});

	makedatapart({
			from => $smallcount + 1,
			to => $total - 1,
			size => $bigsize,
			slaves => $total_slaves,
			dir => $dir,
			source => $source,
			target => $target,
			sqldir => $sqldir});
}

sub makedatapart
{
	my $opts = shift;

	my $dir = $opts->{dir};
	my $source = $opts->{source};
	my $target = $opts->{target};
	my $sqldir = $opts->{sqldir};

	for my $i ($opts->{from} .. $opts->{to}) {
		`dd if=/dev/urandom bs=1 count=$opts->{size} of=$dir/$source/file$i.dat 2> /dev/null`;
		`echo "INSERT INTO test1 values($i, lo_import('$dir/$source/file$i.dat'));" >> $sqldir/insert.sql`;
		`echo "SELECT lo_export((select col2 from test1 where col1 = $i), '$dir/$target/master/file$i.dat');" >> $sqldir/select_master.sql`;
		`echo "SELECT lowrite((select lo_open((select col2 from test1 where col1=$i), 393216)), 'test write #$i'::bytea);" >>$sqldir/update.sql`;

		for my $j (0 .. $opts->{slaves} - 1) {
			`echo "SELECT lo_export((select col2 from test1 where col1 = $i), '$dir/$target/slave$j/file$i.dat');" >> $sqldir/select_slave$j.sql`;
		}
	}
}

sub check_lo_result
{
	my $total = shift;
	my $testdir = shift;
	my $target = shift;
	my $total_slaves = shift;

	for my $i (0 .. $total - 1) {
		my $masterfile = "$testdir/$target/master/file$i.dat";

		if (!-e $masterfile) {
			die "master result failed ($masterfile does not exist)";
		}

		for my $j (0 .. $total_slaves - 1) {
			my $file = "$testdir/$target/slave$j/file$i.dat";

			if (not -e $file) {
				die "file $i slave $j result failed ($file does not exist)";
			}

			my $diff = `diff $masterfile $file`;
			chomp($diff);
			if ($diff ne "") {
				die "file $i slave $j result failed ($file differs from $masterfile)";
			}
		}
	}
}
