SELECT lo_export((select i from bar where a = 2), '/tmp/test_slave.data');
