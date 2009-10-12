package BaseTest;

use Carp;
use strict;

use File::Find;

sub new
{
	my $class = shift;

	my $self = {};
	$self->{name} = $class;
	$self->{dirname} = sprintf "%s/%s", Cwd->getcwd, $self->{name};

	return bless $self, $class;
}

sub run
{
	my $self = shift;
    printf "\033[32m%s starting\033[0m\n", $self->testname;
}

# Removes all files in the test output directory, and make sure it exists
sub clean_output
{
	my $self = shift;
	my $outputdir = $self->outputdir;

	mkdir $outputdir unless -d $outputdir;
	find(sub { -f && unlink }, $outputdir);
}

# Returns the test name
sub testname
{
	my $self = shift;
	return $self->{name};
}

# Returns the test base directory
sub dirname
{
	my $self = shift;
	return $self->{dirname}
}

# Returns the test output directory
sub outputdir
{
	my $self = shift;
	return $self->{dirname}."/output";
}

# Executes an SQL file in standalone mode, specified by name relative to the
# test base directory, and saves the output in output/$file.$node
sub standalone_execute
{
	my $self = shift;
	my $node = shift;
	my $file = shift;

	croak "invalid node of class ". ref $node unless $node->isa('OPgsql');

	$node->standalone_execute($self->dirname . "/$file",
		$self->outputdir . "/$file." . $node->name);
}

# Executes a series of SQL commands in normal mode, and saves the output in
# output/$file.$node.  Returns this name.
#
# This can be either a file, specified by name relative to the test base
# directory, or a string.
#
# The resulting file must match stderr_match, if specified.  If a match file
# was not specified, then stderr must be empty.
sub execute
{
	my $self = shift;
	my $opts = shift;
	
	my @servers = @{$opts->{servers}};
	my $file = $opts->{file};
	my $string = $opts->{string};
	my $outfile;
	my $name;

	# when executing a string a name must be passed.  When executing a file,
	# it can be omitted; the file name will be used as name in that case.
	if (defined $opts->{'name'}) {
		$name = $opts->{'name'};
	} elsif (defined $file) {
		$name = $file;
	}
	
	croak "must pass either a file or a string to execute"
		unless defined $file or defined $string;
	croak "cannot execute both a file and a string"
		if defined $file and defined $string;
	croak "executing name not provided" unless defined $name;

	foreach my $node (@servers) {
		croak "invalid node of class ". ref $node unless $node->isa('OPgsql');

		$outfile = $self->outputdir . "/$name." . $node->name;
		my $errfile = "$name.err." . $node->name;
		my $perrfile = $self->outputdir . "/$errfile";

		if (defined $string) {
			$node->execute_string($string, $outfile, $perrfile, $name);
		} else {
			$node->execute($self->dirname."/$file", $outfile, $perrfile);
		}

		if (defined $opts->{'stderr_match'}) {
			if ($self->match($perrfile, $opts->{stderr_match}) != 1) {
				confess "couldn't match $errfile to $opts->{stderr_match}";
			} else {
				$self->partial_success("matched $errfile to $opts->{stderr_match}");
			}
		} elsif (not -z $perrfile) {
			confess "stderr $errfile not empty while executing $name";
		}
	}

	return $outfile;
}

sub check_promotion
{
	my $self = shift;
	my $opts = shift;

	my $old_master = $opts->{'demoted'};
	my $new_master = $opts->{'promoted'};
	my $success = $opts->{'success'};

	$success = 1 unless defined $success;
	croak "demonted node was not specified" unless defined $old_master;
	croak "promoted node was not specified" unless defined $new_master;

	my $promoted_mode = $new_master->replication_mode;
	my $demoted_mode = $old_master->replication_mode;

	if ($success) {
		if ($promoted_mode ne "master" || $demoted_mode ne "slave") {
			confess sprintf "%s mode (%s) should be %s and %s mode (%s) should be %s",
			$new_master->{name}, $promoted_mode, "master",
			$old_master->{name}, $demoted_mode, "slave";
		}
	} else {
		if ($promoted_mode ne "slave" || $demoted_mode ne "master") {
			confess sprintf "%s mode (%s) should be %s and %s mode (%s) should be %s",
			$new_master->{name}, $promoted_mode, "slave",
			$old_master->{name}, $demoted_mode, "master";
		}
	}
	$self->partial_success("promotion check passed");	
}


# Executes an SQL file in background mode, specified by name relative to the
# test base directory, and saves the output in output/$file.$node.  This
# function may return before the execution is completed.
sub background_execute
{
	my $self = shift;
	my $node = shift;
	my $file = shift;

	croak "invalid node of class ". ref $node unless $node->isa('OPgsql');

	$node->background_execute($self->dirname."/$file",
		$self->outputdir . "/$file." . $node->name);
}

# Compare two files, passed as filenames relative to the test' output dir,
# and return true if they are equal, false otherwise.
sub compare
{
	my $self = shift;
	my $file1 = shift;
	my $file2 = shift;

	my $cmd = sprintf 'cmp %1$s/%2$s %1$s/%3$s > /dev/null',
		$self->outputdir, $file1, $file2;

	qx/$cmd/;

	return $? == 0;
}

# Executes the given file or query in all the given server, runs a "compare" on
# the produced output, and dies if any of them is different.
sub execute_and_compare
{
	my $self = shift;
	my $opts = shift;
	my $name;

	my @servers = @{$opts->{servers}};
	if (defined($opts->{name})) {
		$name = $opts->{name};
	} else {
		$name = $opts->{file};
	}

	# Run the file on all nodes
	$self->execute($opts);

	my $first = shift @servers;
	my $firstfile = sprintf "%s.%s", $name, $first->name;
	foreach my $server (@servers) {
		my $thisfile = sprintf "%s.%s", $name, $server->name;
		$self->compare($firstfile, $thisfile)
			or confess "$thisfile differs from $firstfile";
	}

	$self->partial_success("$name produces identical output on ".
		join(", ", (map {$_->name} @{$opts->{servers}})));
}

# Same as above but dies if result of comparison is 'equal'
# Only 2 nodes are allowed.
sub execute_and_compare_not_equal
{
	my $self = shift;
	my $file = shift;
	my $firstnode = shift;
	my $secondnode = shift;

	$self->execute({
			file => $file,
			servers => [$firstnode, $secondnode]
		});

	my $firstfile = sprintf "%s.%s", $file, $firstnode->name;
	my $secondfile = sprintf "%s.%s", $file, $secondnode->name;
	confess "$secondfile equals to $firstfile" if
		$self->compare($firstfile, $secondfile);

	$self->partial_success("$file produces different output on ".
		join(", ", (map {$_->name} $firstnode, $secondnode)));
}

# Read a file, replace some tokens from it, and write it back.  The file
# is assumed to be in the test basedir, inside the "in" directory.  It will
# be written to a file with the specified name in the base test directory.
# Tokens to be replaced are passed as a hash reference, keys being the pattern
# to replace, and values being the values with which to replace them.  Note:
# the keys must be surrounded by @ in the in-file (but not in the pattern).
#
# Note: files being written to the test basedir is a probably a mistake.
# Change it someday.
sub convert
{
	my $self = shift;
	my $file = shift;
	my $outfile = shift;
	my $conversions = shift;

	my $inf = $self->dirname."/in/$file";
	open my $IN, "<", $inf
		or croak "can't open $inf for reading: $!";
	my $outf = $self->dirname."/$outfile";
	open my $OUT, ">", $outf
		or croak "can't open $outf for writing: $!";

	while (<$IN>) {
		foreach my $k (keys %$conversions) {
			s/\@${k}@/$conversions->{$k}/g;
		}
		print $OUT $_;
	}
	close $IN;
	close $OUT;
}

# Shamelessly stolen from Test::Cmd
sub _make_array {
	my $lines = shift;
	my @line_ary;
	if (ref $lines) {
		chomp(@line_ary = @$lines);
	} else {
		@line_ary = split(/\n/, $lines, -1);
		pop(@line_ary);
	}

	return \@line_ary;
}

# ditto
sub _matcher {
	my $lines = shift;
	my $matches = shift;
	my $sub = shift;

	$lines = _make_array($lines);
	$matches = _make_array($matches);

	return 0 if @$lines != @$matches;
	my $i;
	for ($i = 0; $i < $#{ $matches } + 1; $i++) {
		if (! $sub->($lines->[$i], $matches->[$i])) {
			printf "{{{$lines->[$i]}}} did not match {{{$matches->[$i]}}}\n";
			return 0;
		}
	}
	return 1;
}

# ditto
sub match_regex {
	my $self = shift;
	my $lines = shift;
	my $regexes = shift;

	_matcher($lines, $regexes, sub {$_[0] =~ m/^$_[1]$/});
}

# Given a text file and a file containing regular expressions, returns true
# if every line in the first file matches the regular expression in the
# corresponding line in the second file; returns false otherwise.
#
# The matches file lives in the base test directory.
sub match {
	my $self = shift;
	my $lines_path = shift;
	my $matches_path = shift;

	my $matches_file = "$self->{dirname}/$matches_path";
	croak "match file $matches_file does not exist" unless -f $matches_file and -r _;
	croak "output file $lines_path does not exist" unless -f $lines_path and -r _;

	$self->match_regex(scalar `cat $lines_path`, scalar `cat $matches_file`);
}

sub fail
{
	my $self = shift;
	my $format = shift;

    die sprintf "\033[31m%s failed: \033[0m$format\n", $self->{name}, @_;
}

sub succeed
{
	my $self = shift;
    my $format = shift;

	$format = defined $format ? ": $format" : "";

    printf "\033[32m%s succeeded\033[0m$format\n", $self->{name}, @_;
}

sub partial_success
{
	my $self = shift;
	my $format = shift;

	$format = defined $format ? ": $format" : "";
	 
	printf "\033[32m%s$format\033[0m\n", $self->{name}, @_;
}

# Execute SQL commands on master, wait for replication to complete,
# execute the check SQL and verify that the output is the same in the
# master as in all the slaves.
sub exec_and_test_replication {
	my $self = shift;
	my $opts = shift;
	
	my $master = $opts->{master};
	my @slaves = @{$opts->{slaves}};
	my $check = $opts->{check};
	
	$opts->{servers} = [$master];
	
	$self->execute($opts);

	MammothTest::wait_replication_complete({
			master => $master,
			slaves => \@slaves
		});

	$self->execute_and_compare({
								servers => [$master, @slaves],
								file => $check
							   });
}

1;
