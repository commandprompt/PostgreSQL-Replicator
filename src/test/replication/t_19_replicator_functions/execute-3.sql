SELECT disable_replication_by_schema('command prompt');
SELECT enable_replication_by_schema('123'' junk', ARRAY[1, 2]);