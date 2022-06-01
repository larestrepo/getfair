# Getfair project

> Este repositorio hace uso extensivo de una librería en python que se integra con Cardano para facilitar todo tipo de interacciones con la blockchain y es parte de un proceso de desarrollo más amplio y extensivo aunque de código abierto. [CardanoPython](https://github.com/AylluAcademy-org/CardanoPython).

La siguiente aplicación cubre los requerimientos a dos procesos principales:
1. ETL de una fuente de datos ubicada en la herramienta kobo utilizando un API para poblar una estructura de bases de datos en postgresql.
2. Monitoreo de la información que va a ser subida a la blockchain de Cardano con los datos (indicadores y archivos asociados) más relevantes como soportes para mejorar la transparencia y trazabilidad de un producto determinado.

#

## 1. ETL
El API de kobo permite obtener la información por proyecto/producto/protocolo con sus registros asociados https://kf.kobotoolbox.org/api/v2/assets/

El script principal que se ejecuta para extraer la información de Kobo, procesarla, organizarla y escribirla en una estructura definida de base de datos es <b>project_monitor.py</b>. Está por determinar la frecuencia a la cual se debe correr este script, inicialmente se ha propuesto cada 24 horas.

Las tablas que actualiza son las siguientes:

- Proyectos

```python
"""
            CREATE TABLE IF NOT EXISTS projects (
            id SERIAL PRIMARY KEY,
            name VARCHAR (255) NOT NULL,
            country VARCHAR (255),
            sector VARCHAR (255),
            url text,
            owner VARCHAR(255),
            uid text,
            kind VARCHAR(255),
            asset_type VARCHAR(255),
            version_id text,
            date_created TIMESTAMP
        );
        """
```
- Data
```python
        """
        CREATE TABLE IF NOT EXISTS data (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL,
            _id BIGINT,
            _uuid TEXT,
            validation TEXT,
            gpslocation TEXT,
            usuario TEXT,
            role TEXT,
            dlocation TEXT,
            mlocation TEXT,
            submission TIMESTAMP,
            FOREIGN KEY (project_id)
                REFERENCES projects (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
     """
```
- Pictures
```python
        """
        CREATE TABLE IF NOT EXISTS pictures (
            index SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL,
            data_id INTEGER NOT NULL,
            picture_id bigint NOT NULL,
            instance bigint NOT NULL,
            name VARCHAR (255),
            url text,
            ipfshash text,
            FOREIGN KEY (project_id)
                REFERENCES projects (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (data_id)
                REFERENCES data (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """
```
- Measurements
```python
        """
        CREATE TABLE IF NOT EXISTS measurement (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL,
            _id BIGINT,
            measurement TEXT,
            value NUMERIC,
            file_name TEXT,
            instance BIGINT,
            picture_id BIGINT,
            kobo_url TEXT,
            FOREIGN KEY (project_id)
                REFERENCES projects (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """
```

## 2. Blockchain

Una vez se tiene la información en base de datos, existen algunos registros que deben ser cargados a la blockchain de Cardano. Se ha escogido el identificador "Approved", disponible en la herramienta kobo para identificar cuáles registros deben subir a la blockchain. Un proceso de aprobación más elaborado debería considerarse en este punto. 

Los registros en el estado "Approved" son los que se cargan a la blockchain previo a la población de la siguiente tabla en la base de datos.

- Transactions
```python
"""
        CREATE TABLE IF NOT EXISTS transactions (
            index SERIAL PRIMARY KEY,
            data_id INTEGER NOT NULL,
            address_origin text,
            address_destin text,
            txin text,
            tx_cborhex json,
            tx_hash text,
            time TIMESTAMP,
            metadata json,
            fees BIGINT,
            network text,
            processed BOOLEAN,
            FOREIGN KEY (data_id)
                REFERENCES data (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """
```
El script principal que se ejecuta para poblar la tabla transacciones, crear la metadata, subir los archivos necesarios al IPFS y generar la transacción en la blockchain es <b>bc_monitor.py</b>. Está por determinar la frecuencia a la cual se debe correr este script, sin embargo, tal cual está diseñada la aplicación actualmente, los dos scripts deben ser ejecutados de forma secuencial (primero el script project_monitor.py y luego bc_monitor.py). Otros mecanismos se han previsto para una segunda iteración de esta aplicación. 

## Otros archivos auxiliares importantes

- dblib.py
- utils.py

#