#!/bin/sh
#
#
# Use $1 to specify the postgresql version, use $2 to specify the distribution version

echo "Building non-standard platform"
echo "Building demo replicator"
make clean
autoconf
./configure --with-perl --prefix=/opt/pgsql --with-openssl --with-readline --enable-replication --with-limitconn=5 --with-mcp-openssl
make install
make -C contrib
make -C contrib install
echo "Packaging"
tar -czvPf /opt/mammoth-replicator-$1-$2-demo.tar.gz /opt/pgsql
rm -rf /opt/pgsql
make clean

echo "Building Deluxe Replicator"
make clean
autoconf
./configure --with-perl --prefix=/opt/pgsql --with-openssl --with-readline --enable-replication --with-mcp-openssl
make install
make -C contrib
make -C contrib install
echo "Packaging"
tar -czvPf /opt/mammoth-replicator-$1-$2-deluxe.tar.gz /opt/pgsql
rm -rf /opt/pgsql
make clean
echo "Done"
