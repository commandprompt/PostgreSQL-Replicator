CREATE LANGUAGE plpgsql;
CREATE FUNCTION copy_main_to_aux() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO aux VALUES(NEW.id);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER copy_main_to_aux_trigger AFTER INSERT ON main
FOR EACH ROW EXECUTE PROCEDURE copy_main_to_aux();

ALTER TABLE main ENABLE REPLICA TRIGGER copy_main_to_aux_trigger;


