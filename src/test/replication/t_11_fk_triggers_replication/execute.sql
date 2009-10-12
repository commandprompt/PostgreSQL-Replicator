BEGIN;
INSERT INTO test1(test1_no, data) values(1, 'I');
INSERT INTO test1(test1_no, data) values(2, 'II');
INSERT INTO test1(test1_no, data) values(3, 'III');
INSERT INTO test1(test1_no, data) values(4, 'IV');
INSERT INTO test1(test1_no, data) values(5, 'V');
INSERT INTO test1(test1_no, data) values(6, 'VI');
INSERT INTO test1(test1_no, data) values(7, 'VII');
INSERT INTO test1(test1_no, data) values(8, 'VIII');
INSERT INTO test1(test1_no, data) values(9, 'IX');
INSERT INTO test1(test1_no, data) values(10, 'X');
END;

BEGIN;
INSERT INTO test2(test1_no, data) values(10, '|');
INSERT INTO test2(test1_no, data) values(9, '||');
INSERT INTO test2(test1_no, data) values(8, '|||');
INSERT INTO test2(test1_no, data) values(7, '||||');
INSERT INTO test2(test1_no, data) values(6, '|||||');
END;

BEGIN;
INSERT INTO test3(test1_no, data) values(1, '|');
INSERT INTO test3(test1_no, data) values(2, '||');
INSERT INTO test3(test1_no, data) values(3, '|||');
INSERT INTO test3(test1_no, data) values(4, '||||');
INSERT INTO test3(test1_no, data) values(5, '|||||');
END;
