#!/usr/bin/env python3
"""
Spark Streaming Crime Processor with HDFS Integration
Procesa datos de criminalidad en tiempo real usando Kafka y Spark, almacenando en HDFS
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/spark_streaming_hdfs.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("=== INICIANDO SPARK STREAMING CRIME PROCESSOR WITH HDFS ===")
        
        # Crear sesión de Spark con configuración HDFS
        logger.info("Creando sesión de Spark con soporte HDFS...")
        spark = SparkSession.builder \
            .appName("GuayaquilCrimeAnalysis-HDFS") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/crime-processor") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Sesión de Spark creada exitosamente")
        
        # Configurar stream de Kafka
        logger.info("Configurando stream de Kafka...")
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "raw-crimes") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("Stream de Kafka configurado exitosamente")
        
        # Procesar los datos (igual que antes)
        logger.info("Configurando procesamiento de datos...")
        processed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("crime_data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset")
        )
        
        # Agregar timestamp de procesamiento
        processed_df = processed_df.withColumn("processing_time", current_timestamp())
        
        # Extraer información básica del JSON para particionado
        processed_df = processed_df.withColumn(
            "crime_type", 
            coalesce(get_json_object(col("crime_data"), "$.crime_type"), lit("Unknown"))
        ).withColumn(
            "district", 
            coalesce(get_json_object(col("crime_data"), "$.location.district"), lit("Unknown"))
        ).withColumn(
            "cedula_hash",
            coalesce(get_json_object(col("crime_data"), "$.reporter_info.cedula_hash"), lit("Unknown"))
        ).withColumn(
            "date_partition", 
            date_format(col("processing_time"), "yyyy-MM-dd")
        )
        
        # Filtrar registros con campos válidos
        processed_df = processed_df.filter(
            (col("crime_type").isNotNull()) & 
            (col("district").isNotNull()) & 
            (col("cedula_hash").isNotNull()) &
            (col("date_partition").isNotNull()) &
            (col("crime_type") != "") & 
            (col("district") != "") &
            (col("cedula_hash") != "")
        )
        
        logger.info("Procesamiento de datos configurado exitosamente")
        
        # Función para escribir a múltiples destinos
        def write_to_multiple_sinks(df, epoch_id):
            try:
                logger.info(f"Procesando batch {epoch_id}")
                
                # 1. Escribir a Kafka (mantener compatibilidad)
                kafka_output = df.select(
                    col("kafka_key").alias("key"),
                    to_json(struct(
                        col("crime_data"),
                        col("processing_time"),
                        col("kafka_timestamp"),
                        col("kafka_partition"),
                        col("kafka_offset")
                    )).alias("value")
                )
                
                kafka_output.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("topic", "processed-crimes") \
                    .mode("append") \
                    .save()
                
                logger.info(f"Batch {epoch_id}: Datos escritos a Kafka")
                
                # 2. Escribir a HDFS con particionado
                hdfs_output = df.select(
                    col("crime_data"),
                    col("processing_time"),
                    col("kafka_timestamp"),
                    col("crime_type"),
                    col("district"),
                    col("cedula_hash"),
                    col("date_partition")
                )
                
                hdfs_output.write \
                    .mode("append") \
                    .partitionBy("date_partition", "district") \
                    .option("compression", "gzip") \
                    .parquet("hdfs://namenode:9000/crime-data/processed")
                
                logger.info(f"Batch {epoch_id}: Datos escritos a HDFS")
                
                # 3. Escribir estadísticas básicas a HDFS
                stats_df = df.groupBy("date_partition", "district", "crime_type") \
                    .agg(
                        count("*").alias("crime_count"),
                        max("processing_time").alias("last_updated")
                    )
                
                stats_df.write \
                    .mode("overwrite") \
                    .partitionBy("date_partition") \
                    .option("compression", "gzip") \
                    .parquet("hdfs://namenode:9000/crime-data/statistics")
                
                logger.info(f"Batch {epoch_id}: Estadísticas escritas a HDFS")
                
            except Exception as e:
                logger.error(f"Error procesando batch {epoch_id}: {str(e)}")
                raise e
        
        # Iniciar el streaming con múltiples salidas
        logger.info("Iniciando streaming query...")
        query = processed_df.writeStream \
            .foreachBatch(write_to_multiple_sinks) \
            .outputMode("append") \
            .trigger(processingTime="6 seconds") \
            .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/crime-processor") \
            .start()
        
        logger.info("Streaming query iniciado exitosamente")
        logger.info("Procesando datos en tiempo real...")
        logger.info("Presiona Ctrl+C para detener")
        
        # Esperar a que termine el query
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Deteniendo aplicación por solicitud del usuario...")
    except Exception as e:
        logger.error(f"Error en la aplicación: {str(e)}")
        raise e
    finally:
        logger.info("Cerrando sesión de Spark...")
        if 'spark' in locals():
            spark.stop()
        logger.info("=== SPARK STREAMING TERMINADO ===")

if __name__ == "__main__":
    main()
