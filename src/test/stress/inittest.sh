#!/bin/sh

#
# This program assumes that you have a complete install of replication 
#
#

MAMMOTH_ROOT="/home/jd/replicator_tests/81"
MASTER_HOME="/home/jd/replicator_tests/81/master0"
SLAVE0_HOME="/home/jd/replicator_tests/81/slave0"
SLAVE1_HOME="/home/jd/replicator_tests/81/slave1"
TEST_HOME="/home/jd/replicator_tests/81/postgresql/src/test/stress"
PGBIN="/home/jd/replicator_tests/81/bin"
DBASE="bench"
USER="jd"
PATH="$PATH:$PGBIN"

#initdb
$PGBIN/initdb -D $MASTER_HOME
$PGBIN/initdb -D $SLAVE0_HOME
$PGBIN/initdb -D $SLAVE1_HOME

#createdb
echo "create database $DBASE" | $PGBIN/postgres -D $MASTER_HOME template1
echo "create database $DBASE" | $PGBIN/postgres -D $SLAVE0_HOME template1
echo "create database $DBASE" | $PGBIN/postgres -D $SLAVE1_HOME template1

$PGBIN/init-mammoth-database -D $MASTER_HOME $DBASE 
$PGBIN/init-mammoth-database -D $SLAVE0_HOME $DBASE 
$PGBIN/init-mammoth-database -D $SLAVE1_HOME $DBASE 

# copy postgresql.conf files
echo "Copying postgresql.conf source files"
cp $TEST_HOME/master0.postgresql.conf $MASTER_HOME/postgresql.conf
cp $TEST_HOME/slave0.postgresql.conf $SLAVE0_HOME/postgresql.conf
cp $TEST_HOME/slave1.postgresql.conf $SLAVE1_HOME/postgresql.conf

# Initialize pgbench
echo "initializing pgbench"
$PGBIN/pg_ctl -w -D $MASTER_HOME start
$PGBIN/pgbench -U $USER -p5500 -h localhost -d bench -s10 -i
$PGBIN/pg_ctl -w -D $MASTER_HOME stop

$PGBIN/pg_ctl -w -D $SLAVE0_HOME start
$PGBIN/pgbench -U $USER -p5501 -h localhost -d bench -s10 -i
$PGBIN/pg_ctl -w -D $SLAVE0_HOME stop

$PGBIN/pg_ctl -w -D $SLAVE1_HOME start
$PGBIN/pgbench -U $USER -p5502 -h localhost -d bench -s10 -i
$PGBIN/pg_ctl -w -D $SLAVE1_HOME stop

echo "Now you can run your tests"    
