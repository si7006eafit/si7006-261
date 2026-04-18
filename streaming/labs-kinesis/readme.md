# Universidad EAFIT
# Módulo: Arquitectura Streaming con AWS Kinesis
# Profesor: Edwin Montoya M. – emontoya@eafit.edu.co

# Amazon Kinesis Lab

# Parte 1 - Logs Agent -> Kinesis Firehose -> S3 -> Glue -> Athena:

1. crear un servicio kinesis firehose por la consola web:

        Create: Amazon Data Firehouse
        
        Source: Direct PUT
        Destination: Amazon S3

        Delivery stream name: purchaseLogs

        Destination settings/S3 bucket: si7006orderlogs     (escoja su propio nombre de bucket y realice todas las actualizaciones pertinentes)
        
Opciones avanzadas: Buffer hints, compression, file extension and encryption

        Buffer interval: 60 segs

Opciones avanzadas: Advanced settings:

        PermissionsInfo:
        (o) Choose existing IAM role: LabRole

        Click on: Create Firehose stream
        
2. crear una instancia EC2 AMI 2023 linux

Selecciones la VM EC2 -> Actions -> Security -> Modify IAM role:

Actualizar el IAM Role a 'LabInstanceProfile'

Este Role asociarlo a la instancia EC2 donde instalará el agent de kinesis.

[seguir-estas-instrucciones](instalar-vm-agente.txt)

3. instalar el agente kinesis

[seguir-estas-instrucciones](instalar-vm-agente.txt)

4. descargar los logs (OnlineRetail.csv) ejemplo y LogsGenerator.py (ya estan en el github)

## Nota: antes de Generar Logs, descomprima el archivo: OnlineRetail.csv.gz

        $ cd kinesis/OrderHistory
        $ gunzip OnlineRetail.csv.gz

5. cambiar permisos, crear directorios, etc:

        $ chmod a+x LogGenerator.py
        $ sudo mkdir /var/log/acmeco

### copie el archivo del repo github: agent.json-with-firehose hacia /etc/aws-kinesis/agent.json

        $ cd kinesis/

        $ sudo cp agent.json-with-firehose /etc/aws-kinesis/agent.json

        $ sudo vim /etc/aws-kinesis/agent.json

6. iniciar el agente:

        $ sudo systemctl restart aws-kinesis-agent

7. ejecutar un envio de logs:

        $ cd kinesis/OrderHistory

        $ sudo python3 LogGenerator.py 1000

8. chequee en unos minutos el 'bucket' si7006orderlogs o equivalente.

9. ejecute aws glue y consulte con aws athena los datos de S3 si7006orderlogs o equivalente.

# parte 2 - Logs Agent -> Kinesis Data Streams -> Lambda -> DynamoDB:

1. Crear un Kinesis Data Stream en AWS:

        Create data Stream:
        name: acmecoOrders
        Boton: Create data Stream

2. crear la tabla DynamoDB

        Table Name: acmecoOrders
        Partition Key: CustomerID / Number
        Sort key - optional: OrderID / String
        Use defaults setting
        boton: Create table

3. Configurar el kinesis-agent para enviar los logs al Kinesis Data Stream

### copie el archivo del repo github: agent.json-with-firehose-and-datastreams hacia /etc/aws-kinesis/agent.json

        $ sudo cp agent.json-with-firehose-and-datastreams /etc/aws-kinesis/agent.json

        $ sudo more /etc/aws-kinesis/agent.json

        se adiciona al archivo original de firehose: 

           "flows": [
                {
                "filePattern": "/var/log/acmeco/*.log",
                "kinesisStream": "acmecoOrders",
                "partitionKeyOption": "RANDOM",
                "dataProcessingOptions": [
                        {
                        "optionName": "CSVTOJSON",
                        "customFieldNames": ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "Customer", "Country"]
                        }
                ]
                },

4. reiniciar el servicio:

        $ sudo systemctl restart aws-kinesis-agent

5. generar logs de prueba:

        $ sudo python3 LogGenerator.py 500

## Configurar un Consumer del kinesis data stream, mediente un cliente standalone (Consumer.py)

6. ir a la instancia EC2 donde tenemos en kinesis-agent para consumirlos MANUALMENTE y almacenarlos en la base de datos DynamoDB

        $ sudo yum install -y python3-pip
        $ sudo pip3 install boto3

        actualizar las credenciales AWS en el linux

        $ mkdir .aws
        $ aws configure

Nota: Copy las credenciales de AWS ACADEMY en el archivo generado. tener en cuenta la region: us-east-1 y el formato: json

### edite el archivo /home/ec2-user/.aws/credentials para adicionar el aws_session_token

7. Configurar un Consumer del kinesis data stream, mediente un cliente standalone (Consumer.py)

        configurar 'Consumer.py'
        $ chmod a+x Consumer.py
        $ python3 Consumer.py

        en otra terminal, generar nuevos registros para que el consumer los adquiera de kinesis y los inserte en DynamoDB

## Configurar un Procesador de Flujo en AWS Lambda.

8. Crear una funcion aws lambda para consumir de kinesis data streams e insertar en una tabla DynamoDB:

        Actualizar el IAM Role = 'LabRole'

        Crear la function lambda 'Author from scratch':
        Function name: ProcessOrders
        Runtime: python 3.9
        Use an existing role: LabRole
        
        Botón: create function

        +Add Trigger: Kinesis Data Stream
        Kinesis stream: kinesis/acmecoOrders

        inserte el código de: lambda-function.txt del github
        haga Deploy

9. chequear en la base de datos DynamoDB la inserción de los registros.

# Análisis de datos con Kinesis Analytics (Flink)

1. Definir la tabla origen (kinesis source): Realice los ajustes necesarios en Nombre de campos o tipos

TABLA ORIGEN:

        CREATE TABLE source_orders (
                InvoiceNo STRING,
                StockCode STRING,
                Description STRING,
                Quantity INT,
                InvoiceDate STRING,
                UnitPrice DOUBLE,
                CustomerID BIGINT,
                Country STRING,
                arrival_time TIMESTAMP(3) METADATA FROM 'timestamp',
                proc_time AS PROCTIME()
        ) WITH (
                'connector' = 'kinesis',
                'stream' = 'acmecoOrders',
                'aws.region' = 'us-east-1',
                'scan.stream.initpos' = 'LATEST',
                'format' = 'json'
        );

2. Ejecutar el procesamiento continuo en ventana infinita: Adapte este ejemplo:

Flujo continuo:

        INSERT INTO output_stream
        SELECT
        sensor_id,
        AVG(temperature) AS avg_temp,
        TUMBLE_END(event_time, INTERVAL '10' SECOND) AS window_end
        FROM sensor_stream
        GROUP BY
        TUMBLE(event_time, INTERVAL '10' SECOND),
        sensor_id;

Ejemplos potenciales para este caso:

        CREATE TABLE source_orders (
        InvoiceNo STRING,
        StockCode STRING,
        Description STRING,
        Quantity INT,
        InvoiceDate STRING,
        UnitPrice DOUBLE,
        CustomerID BIGINT,
        Country STRING,
        arrival_time TIMESTAMP(3) METADATA FROM 'timestamp',
        proc_time AS PROCTIME()
        ) WITH (
        'connector' = 'kinesis',
        'stream' = 'acmecoOrders',
        'aws.region' = 'us-east-1',
        'scan.stream.initpos' = 'LATEST',
        'format' = 'json'
        );

        CREATE TEMPORARY VIEW proc_avg_sales_1min AS
        SELECT
        window_start,
        window_end,
        Country,
        StockCode,
        Description,
        COUNT(*) AS num_events,
        SUM(Quantity) AS total_units,
        AVG(line_total) AS avg_sale_value,
        SUM(line_total) AS total_revenue
        FROM TABLE(
        TUMBLE(TABLE orders_enriched, DESCRIPTOR(proc_time), INTERVAL '1' MINUTE)
        )
        GROUP BY
        window_start,
        window_end,
        Country,
        StockCode,
        Description;

Análisis:

* Esto te da un flujo continuo de agregados por ventanas de 1 minuto:

        - número de eventos
        - unidades vendidas
        - promedio del valor por línea
        - ingreso total

Variante: detectar “compra de stock” por devoluciones o anomalías

Como el dataset también puede contener devoluciones en retail, otra tabla de eventos útil sería una tabla de alertas operativas:

        RETURN_EVENT si Quantity < 0
        PRICE_ANOMALY si UnitPrice <= 0
        HIGH_VALUE_ORDER si Quantity * UnitPrice > umbral

        CREATE TEMPORARY VIEW proc_operational_events AS
        SELECT
        InvoiceNo,
        StockCode,
        Description,
        CustomerID,
        Country,
        Quantity,
        UnitPrice,
        Quantity * UnitPrice AS line_total,
        CASE
                WHEN Quantity < 0 THEN 'RETURN_EVENT'
                WHEN UnitPrice <= 0 THEN 'PRICE_ANOMALY'
                WHEN Quantity * UnitPrice > 500 THEN 'HIGH_VALUE_ORDER'
                ELSE 'NORMAL'
        END AS event_type
        FROM source_orders
        WHERE Quantity < 0
        OR UnitPrice <= 0
        OR (Quantity * UnitPrice) > 500;

3. Tabla destino (kinesis sink), adapta este ejemplo para los campos especificos de procesamiento de Ordenes:

TABLA DESTINO:

        CREATE TABLE output_stream (
        sensor_id STRING,
        avg_temp DOUBLE,
        window_end TIMESTAMP(3)
        )
        WITH (
        'connector' = 'kinesis',
        'stream' = 'output-stream-name',
        'aws.region' = 'us-east-1',
        'format' = 'json'
        );

Para este caso particular y de acuerdo a los analisis:

        CREATE TABLE sink_order_metrics (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        Country STRING,
        StockCode STRING,
        Description STRING,
        num_events BIGINT,
        total_units BIGINT,
        avg_sale_value DOUBLE,
        total_revenue DOUBLE
        ) WITH (
        'connector' = 'kinesis',
        'stream' = 'acmecoOrdersMetrics',
        'aws.region' = 'us-east-1',
        'format' = 'json'
        );

Y publicas los resultados así:

        INSERT INTO sink_order_metrics
        SELECT
        window_start,
        window_end,
        Country,
        StockCode,
        Description,
        num_events,
        total_units,
        avg_sale_value,
        total_revenue
        FROM proc_avg_sales_1min;



## Aspectos claves:

* Conector Kinesis: Se utiliza 'connector' = 'kinesis' para leer/escribir en AWS Kinesis Data Streams.

* Watermarks: Cruciales para el procesamiento de tiempo, definidos con WATERMARK FOR event_time.

* Ventanas (TUMBLE): Agrupa datos en ventanas de tamaño fijo (saltos de tamaño constante).

* Formato: Generalmente se usa JSON para la serialización de datos.

Usar los notebooks de Kinesis Analytics


