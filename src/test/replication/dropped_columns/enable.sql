begin;
alter table bar enable replication;
alter table bar alter column i enable lo;
alter table bar enable replication on slave 0;
alter table bar enable replication on slave 1;
alter table bar enable replication on slave 2;
end;
insert into bar values (2, 3, 4, 5, 6, lo_import('/tmp/test.data'));
update bar set g = 9 where a = 2;
