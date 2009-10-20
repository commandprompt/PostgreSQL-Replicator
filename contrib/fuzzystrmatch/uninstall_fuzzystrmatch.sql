/* $PostgreSQL$ */

-- Adjust this setting to control where the objects get dropped.
SET search_path = public;

DROP FUNCTION dmetaphone_alt (text);

DROP FUNCTION dmetaphone (text);

DROP FUNCTION difference(text,text);

DROP FUNCTION text_soundex(text);

DROP FUNCTION soundex(text);

DROP FUNCTION metaphone (text,int);

DROP FUNCTION levenshtein (text,text,int,int,int);

DROP FUNCTION levenshtein (text,text);
