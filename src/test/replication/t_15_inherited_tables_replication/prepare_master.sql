INSERT INTO rebels VALUES(1, 'Han Solo');
INSERT INTO rebels VALUES(2, 'Leia Organa');

-- now evil empire sends all of them to slavery ---
BEGIN;
ALTER TABLE rebels ENABLE REPLICATION;
ALTER TABLE rebels ENABLE REPLICATION ON SLAVE 0;
COMMIT;

