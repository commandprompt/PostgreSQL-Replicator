CREATE TABLE tab1 (id SERIAL PRIMARY KEY, name VARCHAR) WITH OIDS;
CREATE TABLE tab2 (id SERIAL PRIMARY KEY, dateval DATE) WITH OIDS;
CREATE TABLE tab3 (id SERIAL PRIMARY KEY, value INTEGER) WITH OIDS;

INSERT INTO tab1 VALUES(1, 'Jean-Jacques Rousseau');
INSERT INTO tab1 VALUES(2, 'Denis Diderot');
INSERT INTO tab1 VALUES(3, 'Charles Montesquieu');

INSERT INTO tab2 VALUES(1, 'June-28-1712');
INSERT INTO tab2 VALUES(2, 'Oct-5-1713');
INSERT INTO tab2 VALUES(3, 'Jan-18-1689');

INSERT INTO tab3 VALUES(1, 66);
INSERT INTO tab3 VALUES(2, 70);
INSERT INTO tab3 VALUES(3, 66);

