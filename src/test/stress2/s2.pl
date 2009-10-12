#!/usr/bin/perl
#################################
#
#tests
#
##################################
use DBI;

use s2_init;
use s2_tests;

# Change the below to your liking

$user = "jd";
$base_dir = "/home/jd/replicator_tests/";
$svn_dir = "/home/jd/replicator_tests/REPLICATOR-8.0-1.7/";

# you shouldn't need to touch anything below here. unless you add a test, in which case see run_tests()

sub start_engines() {
    print "Starting master0\n";
    system("$base_dir/bin/pg_ctl -w -D $base_dir/master0 start");
    print "Starting slave0\n";
    system("$base_dir/bin/pg_ctl -w -D $base_dir/slave0 start");
    print "Starting slave1\n";
    system("$base_dir/bin/pg_ctl -w -D $base_dir/slave1 start");
    print "Starting MCP\n";
    system("$base_dir/bin/mcp_server -c $base_dir/etc/mcp_server.conf -l $base_dir/mcp_server.log&");
    print "Done\n";
    }
    
sub stop_engines() {
    print "Starting master0\n";
    system("$base_dir/bin/pg_ctl -w -D $base_dir/master0 stop");
    print "Starting slave0\n";
    system("$base_dir/bin/pg_ctl -w -D $base_dir/slave0 stop");
    print "Starting slave1\n";
    system("$base_dir/bin/pg_ctl -w -D $base_dir/slave1 stop");
    print "Starting MCP\n";
    system("$base_dir/bin/mcp_ctl -D $base_dir/mcp_data stop");
    print "Done\n";
    }    

sub cleanup() {
    system("rm -f $base_dir/*log* $base_dir/master0/rlog/* $base_dir/slave0/rlog/* $base_dir/slave1/rlog/*");
    system("rm -f $base_dir/master0/pg_log/* $base_dir/slave0/pg_log/*  $base_dir/slave0/pg_log/*");
    system("rm -f $base_dir/mcp_data/*");
}

sub help() {
    print "Your options are: \n\n";
    print "   s2.pl clean	-- remove all logs, text and transaction\n";
    print "   s2.pl drop	-- drop all tables, will need init before run\n";
    print "   s2.pl init	-- initialize the tables for the test\n";
    print "   s2.pl repeat 	-- run a test, assumes a previous \"run\"\n";
    print "   s2.pl run		-- run a test, assumes init required\n";
    print "   s2.pl start	-- start the daemons\n";
    print "   s2.pl stop	-- stop all daemons\n";
    print "\n\n";
}
    
sub run_tests() {
    &obench_async();
    &truncate();
    &insert_bulk();
    &insert_prepare();
    &obench_sync();
    &truncate();
    &insert_bulk();
    &obench_async();
    &insert_prepare();
}                    


# Start tests

if (@ARGV[0] eq 'init') {
   &init_tables();
} elsif (@ARGV[0] eq 'repeat') {
   &run_tests();
} elsif (@ARGV[0] eq 'run') {
   &init_replication();
   &run_tests();
} elsif (@ARGV[0] eq 'stop') {
   &stop_engines();
} elsif (@ARGV[0] eq 'start') {
   &start_engines();   
} elsif (@ARGV[0] eq 'clean') {
   &cleanup();      
} elsif (@ARGV[0] eq 'drop') {
   &drop_tables();         
} elsif (@ARGV[0] eq 'help') {
   &help();
} else {
   &help();
}   

