-- CREATE or replace VIEW data_view AS

-- SELECT
--     measurement.measurement, 
--     measurement."value", 
--     pictures.ipfshash, 
--     projects.country, 
--     projects.sector, 
--     projects."name", 
--     transactions.tx_hash, 
--     "data".submission
-- FROM
--     measurement
--     INNER JOIN
--     projects
--     ON 
--         measurement.project_id = projects."id"
--     INNER JOIN
--     pictures
--     ON 
--         projects."id" = pictures.project_id
--     INNER JOIN
--     "data"
--     ON 
--         pictures.data_id = "data"."id" AND
--         projects."id" = "data".project_id
--     INNER JOIN
--     transactions
--     ON 
--         "data"."id" = transactions.data_id

select * from data_view;