CREATE TABLE IF NOT EXISTS data (
	id SERIAL PRIMARY KEY,
	project_id integer REFERENCES projects,
    start_date TIMESTAMP, 
	end_date TIMESTAMP,
	subscriberid bigint,
	deviceid text,
	Foto_Arbol text,
	PyeHassGps text,
	Evaluacion_de_aplicacion text,
	Tipo_de_plaga_Aplicacion text,
	Planta_afectada text,
	tipo_de_plaga text,
	Finca text,
	_id BIGINT,
	_uuid TEXT,
	_validation_status text
);

-- SELECT * FROM projects;

-- DROP TABLE data;
-- DROP TABLE projects;

-- CREATE TABLE IF NOT EXISTS projects (
-- 	id SERIAL PRIMARY KEY,
--     name varchar (80) NOT NULL,
-- 	country varchar (80),
-- 	sector varchar (80),
-- 	description text,
--     url text,
--     owner varchar(80),
--     uid text,
--     kind varchar(80),
--     asset_type varchar(80),
--     version_id text,
--     date_created TIMESTAMP
-- );

;