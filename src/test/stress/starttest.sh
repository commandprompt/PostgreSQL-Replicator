#!/bin/sh
##
#
#

BASE_DIR="/home/jd/replicator_tests/"
USER_NAME="jd"

echo "Clean out old diffs"
cd $BASE_DIR
rm -f master0.accounts
rm -f slave0.accounts
rm -f slave0/mammoth.promoted
rm -f master0/mammoth.promoted
rm -f mcp_data/core*
rm -f *diff*

echo "Set ulimit"
ulimit -c unlimited

echo "Sleeping for 5 seconds"
sleep 5

echo "Remove old master0 logs"
cd $BASE_DIR/master0/rlog
rm -f *
cd $BASE_DIR/master0/pg_log
rm -f *

echo "REMOVE old slave0 logs"
cd $BASE_DIR/slave0/rlog
rm -f *
cd $BASE_DIR/slave0/pg_log
rm -f *

echo "REMOVE old slave1 logs"
cd $BASE_DIR/slave1/rlog
rm -f *
cd $BASE_DIR/slave1/pg_log
rm -f *

echo "REMOVE old mcp logs"
cd $BASE_DIR/mcp_data
rm -f *

echo "Remove mcp-server log"
rm -f $BASE_DIR/mcp_server.log

echo "Start mcp"
$BASE_DIR/bin/mcp_server -c $BASE_DIR/etc/mcp_server.conf -l $BASE_DIR/mcp_server.log&

echo "Sleeping a moment"
sleep 5

cd $BASE_DIR
echo "Start master"
$BASE_DIR/bin/pg_ctl -w -D master0 start

echo "Start slave0"
$BASE_DIR/bin/pg_ctl -w -D slave0 start

echo "Start slave1"
$BASE_DIR/bin/pg_ctl -w -D slave1 start


echo "Verify that replication is turned on"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts enable replication"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts enable replication on slave 0"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts enable replication on slave 1"

echo "Initialize mcp_refresh"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "mcp refresh"

echo "Run pgbench"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench

echo "Turn do some wacky stuff"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE branches enable replication"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE branches enable replication on slave 0"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE branches enable replication on slave 1"


echo "Run pgbench"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench

echo "Turn off accounts replication"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts disable replication"

echo "Run pgbench"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench

echo "Turn on tellers replication":
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE tellers enable replication"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE tellers enable replication on slave 0"

echo "Run pgbench"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench

echo "Turn back on accounts"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts enable replication"

echo "Run pgbench"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench

echo "Sleeping for 5 minutes"
sleep 150
echo "Promoting:"
$BASE_DIR/bin/psql -U $USER_NAME -p5501 -d bench -c "promote"

echo "Sleep for 1 minute"
sleep 60

echo "Run pgbench against promoted slave"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5501 -c 10 -t 1000 bench

echo "Sleep for 5 minutes"
sleep 150
$BASE_DIR/bin/psql -U $USER_NAME -p5501 -d bench -c "promote back"

echo "sleep for 5 minutes"
sleep 150

echo "Try and run pgbench against master again"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench

echo "Now try and add a column"

$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts DISABLE REPLICATION"
echo "Screw with the accounts table quite a bit"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "CREATE TEMP TABLE accounts_temp AS SELECT * FROM accounts;DROP TABLE accounts CASCADE;CREATE TABLE accounts as SELECT * FROM accounts_temp"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts ADD PRIMARY KEY (aid)"
echo "Now slave0"
$BASE_DIR/bin/psql -U $USER_NAME -p5501 bench -c "CREATE TEMP TABLE accounts_temp AS SELECT * FROM accounts;DROP TABLE accounts CASCADE;CREATE TABLE accounts as SELECT * FROM accounts_temp"
$BASE_DIR/bin/psql -U $USER_NAME -p5501 bench -c "ALTER TABLE accounts ADD PRIMARY KEY (aid)"
echo "Now slave1"
$BASE_DIR/bin/psql -U $USER_NAME -p5502 bench -c "CREATE TEMP TABLE accounts_temp AS SELECT * FROM accounts;DROP TABLE accounts CASCADE;CREATE TABLE accounts as SELECT * FROM accounts_temp"
$BASE_DIR/bin/psql -U $USER_NAME -p5502 bench -c "ALTER TABLE accounts ADD PRIMARY KEY (aid)"
echo "Now adding a column"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts ADD COLUMN test text"
$BASE_DIR/bin/psql -U $USER_NAME -p5501 bench -c "ALTER TABLE accounts ADD COLUMN test text"
$BASE_DIR/bin/psql -U $USER_NAME -p5502 bench -c "ALTER TABLE accounts ADD COLUMN test text"
echo "Now enabling replication again"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "BEGIN; ALTER TABLE accounts ENABLE REPLICATION; ALTER TABLE accounts ENABLE REPLICATION ON SLAVE 0;ALTER TABLE accounts ENABLE REPLICATION ON SLAVE 1; COMMIT"

echo "Run pgbench again"
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench

echo "Now try and DROP a column"

$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts DISABLE REPLICATION"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts DROP COLUMN test"
$BASE_DIR/bin/psql -U $USER_NAME -p5501 bench -c "ALTER TABLE accounts DROP COLUMN test"
$BASE_DIR/bin/psql -U $USER_NAME -p5502 bench -c "ALTER TABLE accounts DROP COLUMN test"
$BASE_DIR/bin/psql -U $USER_NAME -p5500 bench -c "ALTER TABLE accounts ENABLE REPLICATION"

echo "Run pgbench again"
sleep 180
$BASE_DIR/bin/pgbench -U $USER_NAME -p5500 -c 10 -t 1000 bench


echo "Now watch for a while before you actually stop test"
