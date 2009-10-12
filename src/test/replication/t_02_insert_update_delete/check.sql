select count(*), sum(col1) as sum_col1, sum(col3) as sum_col3 from test_1;
select count(*), col3 % 10 from test_1 group by col3 % 10;
