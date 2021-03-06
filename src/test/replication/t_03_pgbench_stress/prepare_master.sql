ALTER TABLE pgbench_branches ENABLE REPLICATION;
ALTER TABLE pgbench_accounts ENABLE REPLICATION;
ALTER TABLE pgbench_tellers ENABLE REPLICATION;
-- ALTER TABLE pgbench_history ENABLE REPLICATION;

ALTER TABLE pgbench_branches ENABLE REPLICATION ON SLAVE 0;
ALTER TABLE pgbench_accounts ENABLE REPLICATION ON SLAVE 0;
ALTER TABLE pgbench_tellers ENABLE REPLICATION ON SLAVE 0;
-- ALTER TABLE pgbench_history ENABLE REPLICATION ON SLAVE 0;

ALTER TABLE pgbench_branches ENABLE REPLICATION ON SLAVE 1;
ALTER TABLE pgbench_accounts ENABLE REPLICATION ON SLAVE 1;
ALTER TABLE pgbench_tellers ENABLE REPLICATION ON SLAVE 1;
-- ALTER TABLE pgbench_history ENABLE REPLICATION ON SLAVE 1;

ALTER TABLE pgbench_branches ENABLE REPLICATION ON SLAVE 2;
ALTER TABLE pgbench_accounts ENABLE REPLICATION ON SLAVE 2;
ALTER TABLE pgbench_tellers ENABLE REPLICATION ON SLAVE 2;
-- ALTER TABLE pgbench_history ENABLE REPLICATION ON SLAVE 2;
