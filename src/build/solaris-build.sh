#!/bin/bash
export PATH=/usr/local/bin:$PATH
VERSION=`cat configure.in | grep AC_INIT | sed -e "s/.*SQL\]\, \[//" | sed -e "s/\].*//"`
BUILD=$(date +%Y%m%d)
rm -r /usr/local/pgsql/*
./configure --with-perl --with-openssl --with-readline --with-libraries=/opt/csw/lib --with-includes=/opt/csw/include --with-template=solaris  --enable-replication --with-mcp-openssl

make clean
make install >/dev/null
make -C contrib/tsearch install
make -C contrib/dblink install
make -C contrib/tablefunc install

cp -r ~/testcase-solaris /usr/local/pgsql/testcase
cp -r testcase_perl /usr/local/pgsql/

(cd /usr/local/; tar -cf ~/mammoth-replicator-$VERSION-$BUILD.tar pgsql)

rm -r /usr/local/pgsql/*

./configure --with-perl --with-openssl --with-readline --with-libraries=/opt/csw/lib --with-includes=/opt/csw/include --with-template=solaris  --enable-replication --with-limitconn=5 --with-mcp-openssl
make clean
make install >/dev/null
make -C contrib/tsearch install
make -C contrib/dblink install
make -C contrib/tablefunc install

cp -r ~/testcase-solaris /usr/local/pgsql/testcase
cp -r testcase_perl /usr/local/pgsql/

(cd /usr/local/; tar -cf ~/mammoth-replicator-demo-$VERSION-$BUILD.tar pgsql)
rm -r /usr/local/pgsql/*

./configure --with-perl --with-openssl --with-readline --with-libraries=/opt/csw/lib --with-includes=/opt/csw/include --with-template=solaris  

make clean
make install >/dev/null
make -C contrib/tsearch2 install
make -C contrib/dblink install
make -C contrib/tablefunc install


(cd /usr/local/; tar -cf ~/mammoth-$VERSION-$BUILD.tar pgsql)


rm -r /usr/local/pgsql/*

./configure --with-perl --with-openssl --with-readline --with-libraries=/opt/csw/lib --with-includes=/opt/csw/include --with-limitconn=5 --with-template=solaris  

make clean
make install >/dev/null
make -C contrib/tsearch2 install
make -C contrib/dblink install
make -C contrib/tablefunc install

(cd /usr/local/; tar -cf ~/mammoth-demo-$VERSION-$BUILD.tar pgsql)
