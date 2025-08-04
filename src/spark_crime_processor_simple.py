#!/usr/bin/env python3
"""
Spark Streaming Crime Processor - Simplified Version
Procesa datos de criminalidad en tiempo real usando Kafka y Spark
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
        logging.FileHandler('/app/logs/spark_streaming.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("=== INICIANDO SPARK STREAMING CRIME PROCESSOR ===")
        
        # Crear sesión de Spark
        logger.info("Creando sesión de Spark...")
        spark = SparkSession.builder \
            .appName("GuayaquilCrimeAnalysis-Simple") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
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
        
        # Procesar datos básicos
        processed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("crime_data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset")
        )
        
        # Agregar timestamp de procesamiento
        processed_df = processed_df.withColumn("processing_time", current_timestamp())
        
        logger.info("Configuración de procesamiento completada")
        
        # Query de escritura a consola (para debugging)
        console_query = processed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # Query de escritura a Kafka (processed-crimes)
        kafka_query = processed_df.select(
            col("kafka_key").alias("key"),
            to_json(struct(
                col("crime_data"),
                col("processing_time"),
                col("kafka_timestamp"),
                col("kafka_partition"),
                col("kafka_offset")
            )).alias("value")
        ).writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "processed-crimes") \
            .option("checkpointLocation", "/app/checkpoints/processed-crimes") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        logger.info("Queries de streaming iniciadas")
        logger.info("=== SPARK STREAMING ACTIVO - Procesando datos... ===")
        
        # Esperar terminación
        console_query.awaitTermination()
        kafka_query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error en la aplicación: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Sesión de Spark cerrada")

if __name__ == "__main__":
    main()
