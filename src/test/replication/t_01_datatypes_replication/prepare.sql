CREATE LANGUAGE plpgsql;
CREATE TABLE test1(col1 int4 PRIMARY KEY, col2 varchar);
CREATE TABLE test2(col1 int4 PRIMARY KEY, col2 varchar);
CREATE TABLE test3(col1 int4 PRIMARY KEY, col2 varchar);
CREATE TABLE test4(col1 int4 PRIMARY KEY, col2 varchar);

CREATE OR REPLACE FUNCTION test4_insert_sql() RETURNS void AS 'INSERT INTO test4 VALUES(1,''one'')' LANGUAGE sql;
CREATE OR REPLACE FUNCTION test4_update_sql() RETURNS void AS 'UPDATE test4 SET col1=3,col2=''one-updated'' WHERE col1=1' LANGUAGE sql;
CREATE OR REPLACE FUNCTION test4_delete_sql() RETURNS void AS 'DELETE FROM test4 WHERE col1=1' LANGUAGE sql;


CREATE OR REPLACE FUNCTION test4_insert_plpgsql() RETURNS void AS 'BEGIN INSERT INTO test4 VALUES(2,''two''); RETURN; END' LANGUAGE 'plpgsql';
CREATE OR REPLACE FUNCTION test4_update_plpgsql() RETURNS void AS 'BEGIN UPDATE test4 SET col1=4,col2=''two-updated'' WHERE col1=2; RETURN; END' LANGUAGE 'plpgsql';
CREATE OR REPLACE FUNCTION test4_delete_plpgsql() RETURNS void AS 'BEGIN DELETE FROM test4 WHERE col1=2; RETURN; END' LANGUAGE 'plpgsql';

CREATE TABLE test5( a int4 PRIMARY KEY, b name, c text, d float8, e float4, f int2, g polygon, h abstime, i char, j abstime[], k int4, l tid, m xid, n oidvector, q point, r lseg, s path, t box, u tinterval, v timestamp, w interval, x float8[], y float4[], z int2[]);

CREATE TABLE test6( a int4 PRIMARY KEY, b name, c text, d float8, e float4, f int2, g polygon, h abstime, i char, j abstime[], k int4, l tid, m xid, n oidvector, q point, r lseg, s path, t box, u tinterval, v timestamp, w interval, x float8[], y float4[], z int2[]);

CREATE TABLE test7( a int4 PRIMARY KEY, b name, c text, d float8, e float4, f int2, g polygon, h abstime, i char, j abstime[], k int4, l tid, m xid, n oidvector, q point, r lseg, s path, t box, u tinterval, v timestamp, w interval, x float8[], y float4[], z int2[]); 

CREATE TABLE test8( a int4 PRIMARY KEY, b name, c text, d float8, e float4, f int2, g polygon, h abstime, i char, j abstime[], k int4, l tid, m xid, n oidvector, q point, r lseg, s path, t box, u tinterval, v timestamp, w interval, x float8[], y float4[], z int2[]);
CREATE OR REPLACE FUNCTION test8_delete_sql() RETURNS void AS 'DELETE FROM test8 WHERE d=6.1' LANGUAGE sql;
CREATE OR REPLACE FUNCTION test8_update_plpgsql() RETURNS void AS 'BEGIN UPDATE test8 SET a=5 WHERE a=4; RETURN; END' LANGUAGE 'plpgsql';

CREATE TABLE test9(col1 int4 PRIMARY KEY, col2 varchar, col3 timestamp);

CREATE TABLE test10(col1 int4 PRIMARY KEY, col2 varchar, col3 timestamp);

CREATE TABLE test11(col1 int4 PRIMARY KEY, col2 varchar, col3 timestamp);

CREATE TABLE test12(col1 int4 PRIMARY KEY, col2 varchar, col3 timestamp);

CREATE OR REPLACE FUNCTION test12_update_1000_rows() RETURNS  void AS ' BEGIN UPDATE test12 SET col1=col1+1000, col2=''updated'', col3=now(); RETURN; END; ' LANGUAGE 'plpgsql';
CREATE TABLE test13(col1 int4 PRIMARY KEY, col2 varchar, col3 timestamp);

CREATE OR REPLACE FUNCTION test13_delete_999_rows() RETURNS  void AS ' BEGIN DELETE FROM test13 where col1 < 1000; RETURN; END; ' LANGUAGE 'plpgsql';

CREATE TABLE test14(col1 int4 PRIMARY KEY, col2 varchar DEFAULT 'default-value', col3 int4 DEFAULT 1);
CREATE TABLE test15(col1 serial PRIMARY KEY, col2 varchar DEFAULT 'default-value', col3 int4);
CREATE TABLE test16(col1 bigserial PRIMARY KEY, col2 varchar DEFAULT 'default-value', col3 int4);
CREATE TABLE test17(col1 serial PRIMARY KEY, col2 varchar NULL, col3 int4 NULL);
CREATE TABLE test18(col1 serial PRIMARY KEY, col2 varchar, col3 timestamp);

CREATE TABLE test19(col0 int4 NOT NULL DEFAULT 1, col1 serial NOT NULL, col2 varchar NOT NULL, col3 text, col4 timestamp NOT NULL);
ALTER TABLE test19 ADD PRIMARY KEY(col0,col1,col2,col4);

CREATE TABLE test20_1(col1 int4 NOT NULL, col2 varchar, col3 timestamp NOT NULL) WITH OIDS;
ALTER TABLE  test20_1 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test20_2(col1 int4 NOT NULL, col2 varchar, col3 timestamp NOT NULL) WITH OIDS;
ALTER TABLE  test20_2 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test20_3(col1 int4 NOT NULL, col2 varchar, col3 timestamp NOT NULL) WITH OIDS;
ALTER TABLE  test20_3 ADD PRIMARY KEY(col1,col3);

CREATE SEQUENCE test20_seq;

CREATE OR REPLACE FUNCTION test20_insert_1000_rows() RETURNS  void AS ' BEGIN FOR i IN 1 .. 1000 LOOP INSERT INTO test20_1(col1,col2,col3) VALUES(nextval(''test20_seq''::text)::int4, ''test20_1'',  now()); INSERT INTO test20_2(col1,col2,col3) VALUES(nextval(''test20_seq''::text)::int4, ''test20_2'',  now()); INSERT INTO test20_3(col1,col2,col3) VALUES(nextval(''test20_seq''::text)::int4, ''test20_3'',  now()); END LOOP; RETURN; END; ' LANGUAGE 'plpgsql';

CREATE TABLE test21(col1 serial NOT NULL, col2 varchar, col3 timestamp NOT NULL);
ALTER TABLE  test21 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test22(col1 serial NOT NULL, col2 varchar, col3 timestamp NOT NULL);
ALTER TABLE  test22 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test23(col1 serial NOT NULL, col2 varchar, col3 timestamp NOT NULL);
ALTER TABLE  test23 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test24(col1 serial NOT NULL, col2 varchar, col3 timestamp NOT NULL);
ALTER TABLE  test24 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test25(col1 serial NOT NULL, col2 varchar NOT NULL, col3 timestamp NOT NULL);
ALTER TABLE  test25 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test26(col1 serial NOT NULL, col2 varchar NOT NULL, col3 int4 NOT NULL);
ALTER TABLE  test26 ADD PRIMARY KEY(col1,col3);

CREATE TABLE test27(col1 serial NOT NULL, col2 varchar NOT NULL, col3 int4 NOT NULL);
ALTER TABLE  test27 ADD PRIMARY KEY(col1,col3);


CREATE TABLE test28_1 (col1 int PRIMARY KEY);
CREATE TABLE test28_2 (col1 int PRIMARY KEY);
ALTER TABLE test28_2 ADD FOREIGN KEY(col1) references test28_1(col1);
CREATE TABLE test29 (col1 serial PRIMARY KEY, col2 character varying, col3 int4);
CREATE TABLE test30 (col1 serial PRIMARY KEY, col2 character varying, col3 int4);
CREATE TABLE test31 (col1 serial PRIMARY KEY, col2 character varying, col3 int4);
CREATE TABLE test32 (col1 serial PRIMARY KEY, col2 character varying, col3 int4);

CREATE TABLE test33 (col1 serial PRIMARY KEY, col2 character varying, col3 int4);
CREATE OR REPLACE FUNCTION "test33_master_after_update" () RETURNS "trigger" AS ' begin insert into test33 (col2, col3) values (''master_after_update insert'', 10); insert into test33 (col2, col3) values (''master_after_update insert'', 11); delete from test33 where col3=11; RETURN NEW; END' LANGUAGE plpgsql;

CREATE TRIGGER "test33_master_after_update" BEFORE UPDATE ON test1 FOR EACH ROW EXECUTE PROCEDURE "test33_master_after_update"();
CREATE TABLE test34 (col1 serial PRIMARY KEY, col2 character varying, col3 int4);
CREATE OR REPLACE FUNCTION "test34_master_before_delete" () RETURNS "trigger" AS ' begin insert into test34 (col2, col3) values(''master_before_delete insert'', 1); insert into test34 (col2, col3) values(''master_before_delete insert'', 2); update test34 set col2=''master_before_delete update'' where col3=2; RETURN NEW; END ' LANGUAGE plpgsql;
CREATE TRIGGER "test34_master_before_delete" BEFORE DELETE ON test34 FOR EACH ROW EXECUTE PROCEDURE "test34_master_before_delete"();
CREATE TABLE test35 (col1 serial PRIMARY KEY, col2 character varying, col3 int4);
CREATE OR REPLACE FUNCTION "test35_master_after_delete" () RETURNS "trigger" AS ' begin insert into test35 (col2, col3) values(''master_after_delete insert'', 1); insert into test35 (col2, col3) values(''master_after_delete insert'', 2); update test35 set col2=''test35_master_after_delete update'' where col3=2; RETURN NEW; END ' LANGUAGE plpgsql;
CREATE TRIGGER "test35_master_after_delete" AFTER DELETE ON test35 FOR EACH ROW EXECUTE PROCEDURE "test35_master_after_delete"();
CREATE TABLE test36 (col1 int4 PRIMARY KEY, col2 character varying);
CREATE TABLE test37 (col1 serial PRIMARY KEY, col2 character varying);
CREATE TABLE test38 (col1 serial PRIMARY KEY, col2 bytea);
CREATE TABLE test39(col1 int4 PRIMARY KEY, col2 varchar);
CREATE TABLE test_notreplicated(col1 int4 PRIMARY KEY, col2 varchar);

CREATE SEQUENCE sequence_name;
CREATE OR REPLACE FUNCTION deleteme() RETURNS text as 'DECLARE blah int; begin select into blah nextval(''sequence_name''::text) % 5::bigint; RETURN ''digonex1'';END;'language 'plpgsql';
CREATE TABLE test40(id serial primary key, data text default deleteme(), dummy varchar);
