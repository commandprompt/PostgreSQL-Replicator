BEGIN;
ALTER TABLE test ENABLE REPLICATION;
ALTER TABLE test ENABLE REPLICATION ON SLAVE 0;
COMMIT;

