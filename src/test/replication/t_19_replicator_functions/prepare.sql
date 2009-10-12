CREATE LANGUAGE plpgsql;
CREATE SCHEMA "command prompt";
CREATE SCHEMA "123' junk";

CREATE TABLE "command prompt"."public projects"(id integer primary key, name varchar, active bool);
CREATE TABLE "command prompt"."secret projects"(id integer, name varchar, active bool);
CREATE TABLE "123' junk"."root passwords"(id integer primary key, pass varchar, fqdn varchar);

BEGIN;
ALTER TABLE "command prompt"."public projects" ENABLE REPLICATION;
ALTER TABLE "command prompt"."public projects" ENABLE REPLICATION ON SLAVE 0;
ALTER TABLE "command prompt"."public projects" ENABLE REPLICATION ON SLAVE 1;
ALTER TABLE "command prompt"."public projects" ENABLE REPLICATION ON SLAVE 2;
ALTER TABLE "command prompt"."public projects" ENABLE REPLICATION ON SLAVE 3;

ALTER TABLE "123' junk"."root passwords" ENABLE REPLICATION;
ALTER TABLE "123' junk"."root passwords" ENABLE REPLICATION ON SLAVE 0;
ALTER TABLE "123' junk"."root passwords" ENABLE REPLICATION ON SLAVE 1;
ALTER TABLE "123' junk"."root passwords" ENABLE REPLICATION ON SLAVE 2;
ALTER TABLE "123' junk"."root passwords" ENABLE REPLICATION ON SLAVE 3;
COMMIT;

