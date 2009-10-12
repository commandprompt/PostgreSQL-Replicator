CREATE LANGUAGE plpgsql;

CREATE TABLE test1(id serial, test1_no integer PRIMARY KEY, data varchar);
CREATE TABLE test2(id serial, test1_no integer PRIMARY KEY REFERENCES test1 ON DELETE CASCADE, data varchar);
CREATE TABLE test3(id serial, test1_no integer PRIMARY KEY REFERENCES test1 ON UPDATE CASCADE, data varchar);
CREATE TABLE test4(id serial PRIMARY KEY, data varchar);
CREATE TABLE test5(id integer, data varchar);

# Should be a single line for a standalone process
CREATE OR REPLACE FUNCTION trig() RETURNS trigger AS $$ BEGIN INSERT INTO test4(data) values('I++'); INSERT INTO test5(data) values('--I'); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER test_insert BEFORE INSERT ON test1 FOR EACH ROW EXECUTE PROCEDURE trig();
