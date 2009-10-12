BEGIN;
UPDATE test1 SET test1_no=101 WHERE test1_no=1;
UPDATE test1 SET test1_no=102 WHERE test1_no=2;
UPDATE test1 SET test1_no=103 WHERE test1_no=3;
END;

BEGIN;
DELETE FROM test1 WHERE test1_no=10;
DELETE FROM test1 WHERE test1_no=9;
DELETE FROM test1 WHERE test1_no=8;
END;
