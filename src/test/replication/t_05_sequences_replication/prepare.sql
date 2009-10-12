DROP SEQUENCE test_sequence;
CREATE SEQUENCE test_sequence;
DROP TABLE test1 CASCADE;
CREATE TABLE test1(id serial primary key, data varchar, seq serial);
