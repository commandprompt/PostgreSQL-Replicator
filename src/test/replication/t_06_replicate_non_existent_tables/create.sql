create table test6 (a serial primary key);

begin;
alter table test6 enable replication;
alter table test6 enable replication on slave 0;
commit;
