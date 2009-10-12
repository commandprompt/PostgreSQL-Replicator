#!/bin/sh

BASE_DIR="/home/jd/replicator_tests"
echo "Building"
$BASE_DIR/bin/buildtest.sh
echo "Running"
$BASE_DIR/bin/starttest.sh
echo "Sleeping"
sleep 300
echo "Stopping"
$BASE_DIR/bin/stoptest.sh
