SELECT disable_replication('"123'' junk"."root passwords"');
SELECT enable_replication('"123'' junk"."root passwords"', ARRAY[0]);