#!/bin/sh
##
#
#

BASE_DIR="/home/jd/replicator_tests/"
SVN_DIR="/home/jd/replicator_tests/REPLICATOR-8.0-1.7"
USER_NAME="jd"

cd $BASE_DIR

echo "Compare"
$BASE_DIR/bin/psql -U $USER_NAME -p 5500 bench -c "select * from accounts order by aid" > $BASE_DIR/master0.accounts
$BASE_DIR/bin/psql -U $USER_NAME -p 5501 bench -c "select * from accounts order by aid" > $BASE_DIR/slave0.accounts
$BASE_DIR/bin/psql -U $USER_NAME -p 5502 bench -c "select * from accounts order by aid" > $BASE_DIR/slave1.accounts

$BASE_DIR/bin/psql -U $USER_NAME -p 5500 bench -c "select * from branches order by bid" > $BASE_DIR/master0.branches
$BASE_DIR/bin/psql -U $USER_NAME -p 5501 bench -c "select * from branches order by bid" > $BASE_DIR/slave0.branches
$BASE_DIR/bin/psql -U $USER_NAME -p 5502 bench -c "select * from branches order by bid" > $BASE_DIR/slave1.branches

$BASE_DIR/bin/psql -U $USER_NAME -p 5500 bench -c "select * from tellers order by tid" > $BASE_DIR/master0.tellers
$BASE_DIR/bin/psql -U $USER_NAME -p 5501 bench -c "select * from tellers order by tid" > $BASE_DIR/slave0.tellers
$BASE_DIR/bin/psql -U $USER_NAME -p 5502 bench -c "select * from tellers order by tid" > $BASE_DIR/slave1.tellers



cd $BASE_DIR
diff master0.accounts slave0.accounts >> accounts.slave0.diff
diff master0.accounts slave1.accounts >> accounts.slave1.diff

diff master0.branches slave0.branches >> branches.slave0.diff
diff master0.branches slave1.branches >> branches.slave1.diff

diff master0.tellers slave0.tellers >> tellers.slave0.diff
diff master0.tellers slave1.tellers >> tellers.slave1.diff

cd $BASE_DIR
echo "Stop master"
$BASE_DIR/bin/pg_ctl -w -D master0 stop

echo "Stop slave0"
$BASE_DIR/bin/pg_ctl -w -D slave0 stop

echo "Stop slave1"
$BASE_DIR/bin/pg_ctl -w -D slave1 stop

echo "Stop mcp"
$BASE_DIR/bin/mcp_ctl -D $BASE_DIR/mcp_data stop

echo "Let's get the logs"
cd $BASE_DIR
LOG_DATE=`date +%Y%m%d-%H%M%S`
tar -czvf logs-$LOG_DATE.tar.gz mcp_server.log accounts.*.diff master0/pg_log/ slave0/pg_log/ slave1/pg_log/

echo
echo Sending log: http://www.commandprompt.com/files/logs-$LOG_DATE.tar.gz
echo
#scp logs-$LOG_DATE.tar.gz admin@www.commandprompt.com:/home/web/cmd4/files
echo "Send notification"

echo >> /tmp/rep_test.stat

echo "==============================================================" >> /tmp/rep_test.stat
echo "Running Report for SVN" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
cd $SVN_DIR
/usr/bin/svn info >> /tmp/rep_test.stat
cd $BASE_DIR
echo >> /tmp/rep_test.stat

echo "===============================================================" >> /tmp/rep_test.stat
echo "Generating MCP status" >> /tmp/rep_test.stat
$BASE_DIR/bin/mcp_stat $BASE_DIR/mcp_data >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat

echo "===============================================================" >> /tmp/rep_test.stat
echo "Generating Slave status" >> /tmp/rep_test.stat
echo "Slave0" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
$BASE_DIR/bin/slave_stat $BASE_DIR/slave0/rlog >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "Slave1" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
$BASE_DIR/bin/slave_stat $BASE_DIR/slave1/rlog >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat

echo "===============================================================" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "File Checks: Only Slave1.tellers should be different" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat

echo "Diff check" >> /tmp/rep_test.stat
echo "Slave0.accounts" >> /tmp/rep_test.stat
diff -q master0.accounts slave0.accounts >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "Slave1.accounts" >> /tmp/rep_test.stat
diff -q master0.accounts slave1.accounts >> /tmp/rep_test.stat

echo >> /tmp/rep_test.stat
echo "Slave0.branches" >> /tmp/rep_test.stat
diff -q master0.branches slave0.branches >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "Slave1.branches" >> /tmp/rep_test.stat
diff -q master0.branches slave1.branches >> /tmp/rep_test.stat

echo >> /tmp/rep_test.stat
echo "Slave0.tellers" >> /tmp/rep_test.stat
diff -q master0.tellers slave0.tellers >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "Slave1.tellers" >> /tmp/rep_test.stat
diff -q master0.tellers slave1.tellers >> /tmp/rep_test.stat

echo >> /tmp/rep_test.stat
echo "Master Error" >> /tmp/rep_test.stat
grep -A 10 -B 10 ERROR master0/pg_log/postgresql* >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "Slave0 Error" >> /tmp/rep_test.stat
grep -A 10 -B 10 ERROR slave0/pg_log/postgresql* >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "Slave1 Error" >> /tmp/rep_test.stat
grep -A 10 -B 10 ERROR slave1/pg_log/postgresql* >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "MCP Error" >> /tmp/rep_test.stat
grep -A 10 -B 10 ERROR $BASE_DIR/mcp_server.log >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat

echo >> /tmp/rep_test.stat
echo "===============================================================" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "BT" >> /tmp/rep_test.stat

core=$(find $BASE_DIR/mcp_data -name "core*")
if [ -f $core ]; then
  gdb --core=$core -ex "thread apply all bt" -se $BASE_DIR/bin/mcp_server --batch >> /tmp/rep_test.stat
fi

echo "===============================================================" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
echo "Detailed Log:" >> /tmp/rep_test.stat
echo  "http://www.commandprompt.com/files/logs-$LOG_DATE.tar.gz" >> /tmp/rep_test.stat
echo >> /tmp/rep_test.stat
mailx -s "Test Run: $LOG_DATE" offshore@mail.commandprompt.com < /tmp/rep_test.stat
cat /tmp/rep_test.stat
