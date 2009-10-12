select setval('test_sequence', 10);
select nextval('test_sequence');
copy test1 (data) from stdin with delimiter ' ';
row1
row2
row3
\.

