BEGIN;
ALTER TABLE tab3 ENABLE REPLICATION;
ALTER TABLE tab3 ENABLE REPLICATION ON SLAVE 0;
ALTER TABLE tab3 ENABLE REPLICATION ON SLAVE 1;
COMMIT;
