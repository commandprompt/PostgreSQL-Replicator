DROP TABLE test1;
CREATE TABLE test1(col1 int4 PRIMARY KEY, col2 oid);
DROP TABLE pgr_slave_lo CASCADE;
CREATE TABLE pgr_slave_lo(master oid, slave oid);
