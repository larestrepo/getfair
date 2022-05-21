-- drop table transactions;
-- drop table pictures;
-- drop table data;
-- DROP TABLE projects;

-- select * from pictures;

-- ALTER TABLE transactions ADD COLUMN metadata1 JSON;



CREATE TABLE IF NOT EXISTS transactions (
            index SERIAL PRIMARY KEY,
            data_id INTEGER NOT NULL,
            tx_hash text,
            time TIMESTAMP,
            address_origin text,
            address_destin text,
            metadata json,
            fees BIGINT,
            FOREIGN KEY (data_id)
                REFERENCES data (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );