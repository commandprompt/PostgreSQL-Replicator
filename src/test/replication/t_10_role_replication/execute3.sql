ALTER ROLE admin WITH SUPERUSER CONNECTION LIMIT 5 VALID UNTIL 'May 1 00:00:00 2010 +3';
ALTER ROLE admin SET search_path TO 'public';
ALTER ROLE admin SET default_transaction_read_only=true;
ALTER ROLE admin RENAME TO administrator;
ALTER ROLE administrator RESET search_path;
