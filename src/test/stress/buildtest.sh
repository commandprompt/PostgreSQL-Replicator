#!/bin/sh
##
#
#

SVN_DIR="/home/jd/replicator_tests/REPLICATOR-8.0-1.7"
BASE_DIR="/home/jd/replicator_tests/"

echo "Clean out old diffs"
cd $BASE_DIR
rm -f master0.accounts
rm -f slave0.accounts


echo "Update SVN"
cd $SVN_DIR
svn update

echo "Clean out repo"
make clean

echo "Set ulimit"
ulimit -c unlimited

echo "Configure Replicator"
autoconf
./configure --prefix=$BASE_DIR --enable-replication --enable-debug --enable-cassert

echo "Make Install"
make -j2 install

echo "Sleeping for 5 seconds"
sleep 5
