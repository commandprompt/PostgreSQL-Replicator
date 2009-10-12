SELECT is_replicated('"command prompt"."public projects"');
SELECT is_replicated('"command prompt"."secret projects"');
SELECT is_replicated('"123'' junk"."root passwords"');

SELECT get_replication_status('"command prompt"."public projects"');
SELECT get_replication_status('"command prompt"."secret projects"');
SELECT get_replication_status('"123'' junk"."root passwords"');
