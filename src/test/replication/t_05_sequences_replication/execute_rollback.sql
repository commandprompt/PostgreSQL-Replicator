BEGIN;
INSERT INTO test1(data) VALUES('first');
ROLLBACK;

BEGIN;
INSERT INTO test1(data) VALUES('second');
COMMIT;

