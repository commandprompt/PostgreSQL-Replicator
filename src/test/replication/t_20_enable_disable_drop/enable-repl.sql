BEGIN;
ALTER TABLE test_13 ENABLE REPLICATION;
ALTER TABLE test_13 ENABLE REPLICATION ON SLAVE 0;
COMMIT;
