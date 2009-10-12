GRANT admins TO admin;
GRANT ALL ON test1 TO admin;
REVOKE INSERT, UPDATE,DELETE ON test1 FROM admin;
REVOKE ALL ON test2 FROM admin;
GRANT ALL on test2 TO admin;


