select aid % 21, sum(abalance) from pgbench_accounts where aid % 5 = 0 group by 1 order by 1;
select * from pgbench_branches order by bid;
select * from pgbench_tellers order by tid;
-- select sum(delta), aid % 31, max(mtime) from pgbench_history group by 2 order by 2;
