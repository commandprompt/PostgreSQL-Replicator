package MammothTest;

use strict;
use Carp;

use Replicator;
use File::Copy;
use Time::HiRes qw(usleep);

sub new_pg_node
{
	my $test = shift;
	my $node = shift;

	my $testname = $test->testname;
	my $dir = $test->dirname;

	# port number is derived from the node name
	my $port = $main::config->{$node}{port};
	die "can't find port spec for node $node" unless defined $port;

	if (!-d "datadir") {
		mkdir "datadir" or die "can't mkdir \"datadir\": $!";
	}

	Replicator->new({
			name => $node,
			port => $port,
			datadir => "datadir/$node",
			logfile => "$dir/output/$port.log",
			forwarder_conf => "${node}.sql",
		});
}

sub init_new_pg_node
{
	my $test = shift;
	my $opts = shift;
	my $pg;

	foreach (qw(name)) {
		if (not defined $opts->{$_}) {
			die "mandatory parameter $_ missing";
		}
	}

	$pg = MammothTest::new_pg_node($test, $opts->{name});

	$pg->initdb if $opts->{init};

	$pg->append_config($opts->{additional_config})
		if defined $opts->{additional_config};

	$test->standalone_execute($pg, $opts->{prepare_script}) 
		if defined $opts->{prepare_script};

	return $pg;
}

sub run_new_mcp
{
	my $test = shift;
	my $pg;

	$pg = MammothTest::new_pg_node($test, "forwarder");
	$pg->initdb;
	$pg->append_config;

	$pg->start;

	return $pg;
}

sub _master_get_latest_recno
{
	my $master = shift;

	return $master->execute_result("select lrecno from replication_status()");
}

sub _slave_get_latest_frecno
{
	my $slave = shift;

	return $slave->execute_result("select frecno from replication_status()");
}

sub wait_replication_complete
{
	my $opts = shift;

	my $master = $opts->{master} if defined $opts->{master};
	my @slaves = @{$opts->{slaves}};

	my $master_lrecno;
	my @origslaves = @slaves;
	my %previous_recnos;

	if (defined $opts->{master_recno_target}) {
		$master_lrecno = $opts->{master_recno_target};
	} else {
		$master_lrecno = _master_get_latest_recno($master);
		while (1) {
			usleep 100_000;
			my $new_lrecno = _master_get_latest_recno($master);
			last if $master_lrecno == $new_lrecno;
			$master_lrecno = $new_lrecno;
		}
	}

	print "waiting for replication to complete, master lrecno: $master_lrecno\n";
	print "slaves: ",
		(join ", ", map { $_->name."/".$_->port} @slaves),
		"\n";

	my $times = 0;

	# initialize the hash that we use to determine whether each slaves has
	# progressed restored since the last time we checked.
	foreach my $slave (@slaves) {
		$previous_recnos{$slave->name} = -1;
	}

	# the main loop: for each slave, check whether the frecno is the same as
	# the master's lrecno + 1.  Return when they all are.  There's a cap on the
	# amount of times we check -- the test fails if the slaves are not done
	# before that.  This is full of "heuristics".
	while (scalar(@slaves) > 0) {
		my @newslaves;
		my $progress = 0;

		print "master: $master_lrecno ";

		while (my $slave = shift @slaves) {
			die "$slave is the master -- fix test" if defined $master and $slave == $master;

			my $recno = _slave_get_latest_frecno($slave);
			if ($recno == -1) {
				print "frecno not known for slave ", $slave->name, "\n";
				push @newslaves, $slave;
				next;
			}

			printf "%s=>%d ", $slave->name, $recno;

			# We start counting again if one slave has progressed since the
			# last time we checked.  Note that we purposedly don't move the
			# counter backwards, if one slave for some reason restores a
			# previous recno!  This is so that if one slave requests a dump
			# due to an error, we don't loop indefinitely.
			if ($recno > $previous_recnos{$slave->name}) {
				$progress = 1;
				$previous_recnos{$slave->name} = $recno;
			}

			# hasn't finished -- put this slave back in the list to check
			push @newslaves, $slave if $recno <= $master_lrecno;
		}
		if ($progress == 1) {
			print " (progress)";
			$times = 3;
		}
		print "\n";

		if ($times > $main::config->{test}{max_wait_times}) {
			die "waited too long for slaves to finish";
		} else {
			$times++;
		}

		# final check.  If the master has advanced in lrecno, then put all
		# the slaves back in the list.  Otherwise, put only those slaves that
		# didn't pass the test.  But first, sleep a bit more.  FIXME this is
		# crude, improve.
		# Note that we can only do this if we were passed a master node.  It is
		# possible that the caller didn't because we can't obtain its lrecno
		# (say because it is down); we can trust the supplied lrecno in that
		# case, so there's no need to sleep.
		if (not defined $opts->{master_recno_target}) {
			if ($times == 1 || $times == 2) {
				sleep 1;
			} elsif ($times < 10) {
				sleep 5;
			} else {
				sleep 10;
			}
			my $orig_lrecno = $master_lrecno;
			$master_lrecno = _master_get_latest_recno($master);

			if ($master_lrecno != $orig_lrecno) {
				@slaves = @origslaves;
			} else {
				@slaves = @newslaves;
			}
		} else {
			@slaves = @newslaves;
		}
	}
}

sub suspend
{
	my $time = shift;
	my $fpath = '/tmp/suspend.tmp';
	`touch $fpath`;
	print "Test suspended\n";
	while ((-f $fpath) || ($time == 0)) {
		sleep 1;
		$time-- unless time < 0;
	}
	`rm -f $fpath` unless (-f $fpath);
}


1;
