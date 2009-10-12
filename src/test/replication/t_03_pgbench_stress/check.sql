select aid % 21, sum(abalance) from accounts where aid % 5 =0 group by 1 order by 1;
select * from branches order by bid;
select * from tellers order by tid;
select sum(delta), aid % 31, max(mtime) from history group by 2 order by 2;
