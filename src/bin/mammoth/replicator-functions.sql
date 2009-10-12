-------------------------------------------------------------------------------
-- Mammoth Replicator 8.1-1.7 functions
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION is_replicated(REGCLASS) RETURNS BOOL AS $$
  DECLARE
    slave   RECORD;
  BEGIN
      SELECT
        sr.slave INTO slave
      FROM
        pg_catalog.repl_slave_relations sr,
        pg_catalog.repl_relations r
      WHERE
        r.relid = sr.relid AND
        r.enable = TRUE AND
        sr.enable = TRUE AND
        sr.relid = $1 LIMIT 1;
    IF FOUND THEN
      RETURN TRUE;
    ELSE
      RETURN FALSE;
    END IF;
  END;
$$ LANGUAGE plpgsql;

-- 
CREATE OR REPLACE FUNCTION get_replication_status(REGCLASS) RETURNS TEXT AS $$
  DECLARE
    slave   RECORD;
    I       INTEGER;
    ret     INTEGER[];
    answer  TEXT;
  BEGIN
    -- get all slaves
    I := 1;
    FOR slave IN SELECT
                   sr.slave
                 FROM
                   pg_catalog.repl_slave_relations sr,
                   pg_catalog.repl_relations r
                 WHERE
                   r.relid = sr.relid AND
                   r.enable = TRUE AND
                   sr.enable = TRUE AND
                   sr.relid = $1
    LOOP
      ret[I] := slave.slave;
      I := I + 1;
    END LOOP;

    IF FOUND THEN
      answer := 'Replication enabled on slaves: ' || array_to_string(ret, ', ');
    ELSE
      answer := 'Replication disabled';
    END IF;

    RETURN answer;
  END;
$$ LANGUAGE plpgsql;

-- enable_replication
CREATE OR REPLACE FUNCTION enable_replication(NAME, INTEGER[]) RETURNS VOID AS $$
  DECLARE
    I INTEGER;
  BEGIN
    I := 1;
    WHILE 0 <= $2[I] LOOP
        EXECUTE 'ALTER TABLE ' || $1 || ' ENABLE REPLICATION ON SLAVE ' || $2[I];
        I := I + 1;
    END LOOP;
    EXECUTE 'ALTER TABLE ' || $1 || ' ENABLE REPLICATION';

    RETURN;
  END;
$$ LANGUAGE plpgsql;

-- enable_replication on all relation within a schema
CREATE OR REPLACE FUNCTION enable_replication_by_schema(NAME, INTEGER[]) RETURNS VOID AS $$
  DECLARE
    table RECORD;
    res   RECORD;
    
  BEGIN
    -- if no schema specified - report an error
    IF $1 IS NULL OR (0 = char_length($1))
    THEN
      RAISE EXCEPTION 'Schema name can\'t be an empty string.';
    END IF;

	SELECT 1 INTO res
	  WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_namespace where nspname = $1);

    -- if no schema found
	IF NOT FOUND THEN
      RAISE EXCEPTION 'Schema does not exist.';
    END IF;

    -- for every table with primary key
    FOR table IN SELECT
                   quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS t
                 FROM
                   pg_catalog.pg_namespace n,
                   pg_catalog.pg_class c
                 WHERE
                   n.oid = c.relnamespace AND n.nspname = $1 AND
                   n.nspname <> 'pg_catalog' AND c.relkind = 'r' AND
                   has_schema_privilege(n.oid, 'USAGE'::text) AND
                   EXISTS (SELECT 1 FROM pg_constraint i
                           WHERE i.conrelid = c.oid AND i.contype = 'p')
    LOOP
      SELECT enable_replication(table.t, $2) INTO res;
    END LOOP;

    RETURN;
  END;
$$ LANGUAGE plpgsql;

-- enable_replication_global
CREATE OR REPLACE FUNCTION enable_replication_global(INTEGER[]) RETURNS VOID AS $$
  DECLARE
    table RECORD;
    res   RECORD;
  BEGIN
    -- for every table with primary key
    FOR table IN SELECT
                   quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS t
                 FROM
                   pg_catalog.pg_namespace n,
                   pg_catalog.pg_class c
                 WHERE
                   n.oid=c.relnamespace AND
                   n.nspname <> 'pg_catalog' AND c.relkind = 'r' AND
                   has_schema_privilege(n.oid, 'USAGE'::text) AND
                   EXISTS (SELECT 1 FROM pg_constraint i
                           WHERE i.conrelid = c.oid AND i.contype = 'p')
    LOOP
      SELECT enable_replication(table.t, $1) INTO res;
    END LOOP;

    RETURN;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION disable_replication(REGCLASS) RETURNS VOID AS $$
  DECLARE
    slave   RECORD;
    rel     RECORD;
    table   VARCHAR;
  BEGIN
    SELECT
      quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS t
    INTO
      rel
    FROM
      pg_catalog.pg_namespace n,
      pg_catalog.pg_class c
    WHERE
      n.oid = c.relnamespace AND
      c.oid = $1::oid;

    IF FOUND THEN
      EXECUTE 'ALTER TABLE ' || rel.t || ' DISABLE REPLICATION';
      FOR slave IN SELECT
                     sr.slave
                   FROM
                     pg_catalog.repl_slave_relations sr
                   WHERE
                     sr.enable = TRUE AND sr.relid = $1::oid
      LOOP
        EXECUTE 'ALTER TABLE ' || rel.t || ' DISABLE REPLICATION ON SLAVE ' || slave.slave;
      END LOOP;
    END IF;
    
    RETURN;
  END;
$$ LANGUAGE plpgsql;

-- reverse of enable_replication_by_schema
CREATE OR REPLACE FUNCTION disable_replication_by_schema(NAME) RETURNS VOID AS $$
    DECLARE
    table RECORD;
    res   RECORD;
  BEGIN
    -- if no schema specified - report an error
    IF $1 IS NULL OR (0 = char_length($1))
    THEN
      RAISE EXCEPTION 'Schema name can\'t be an empty string.';
    END IF;

	SELECT 1 INTO res
	  WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_namespace where nspname = $1);
    -- if no schema found
	IF NOT FOUND THEN
      RAISE EXCEPTION 'Schema does not exist.';
    END IF;

    FOR table IN SELECT
                   relid
                 FROM
                   pg_catalog.repl_relations
                 WHERE
                   namespace = $1 AND
                   enable = TRUE
    LOOP
      SELECT disable_replication(table.relid) INTO res;
    END LOOP;

    RETURN;
  END;
$$ LANGUAGE plpgsql;

-- reverse of enable_replication_global
CREATE OR REPLACE FUNCTION disable_replication_global() RETURNS VOID AS $$
  DECLARE
    table RECORD;
    res   RECORD;
  BEGIN
    FOR table IN SELECT
                   relid
                 FROM
                   pg_catalog.repl_relations
                 WHERE
                   enable = TRUE
    LOOP
      SELECT disable_replication(table.relid) INTO res;
    END LOOP;

    RETURN;
  END;
$$ LANGUAGE plpgsql;