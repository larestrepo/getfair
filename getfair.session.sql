-- DROP TABLE pictures;
-- DROP TABLE data;
-- DROP TABLE projects;

-- DROP TABLE vendors;

-- delete from projects;


-- ALTER TABLE data ADD COLUMN processed BOOLEAN;

ALTER TABLE data
    ALTER COLUMN processed SET DEFAULT FALSE;

-- UPDATE data SET processed = 'false'