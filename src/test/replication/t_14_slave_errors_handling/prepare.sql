CREATE TABLE staff(id SERIAL PRIMARY KEY, login VARCHAR);
CREATE TABLE presence(staff_id INTEGER PRIMARY KEY REFERENCES staff(id), present bool);

