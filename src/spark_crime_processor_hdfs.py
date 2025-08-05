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
from pyspark.sql.functions import hour, dayofweek, minute, floor, collect_list, countDistinct, when, lit, coalesce, concat, sum, avg

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
                
                # 3. Escribir estadísticas básicas a HDFS (mantenemos las existentes)
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
                
                logger.info(f"Batch {epoch_id}: Estadísticas básicas escritas a HDFS")
                
                # 4. MÉTRICAS AVANZADAS - Análisis Temporal
                logger.info(f"Batch {epoch_id}: Calculando métricas temporales avanzadas...")
                temporal_df = df.withColumn("hour", hour(col("processing_time"))) \
                    .withColumn("day_of_week", dayofweek(col("processing_time"))) \
                    .withColumn("minute_window", floor(minute(col("processing_time")) / 10) * 10)
                
                temporal_metrics = temporal_df.groupBy("date_partition", "district", "crime_type", "hour", "day_of_week") \
                    .agg(
                        count("*").alias("crimes_per_hour"),
                        avg("hour").alias("avg_hour"),
                        max("processing_time").alias("last_crime_time")
                    )
                
                temporal_metrics.write \
                    .mode("overwrite") \
                    .partitionBy("date_partition", "district") \
                    .option("compression", "gzip") \
                    .parquet("hdfs://namenode:9000/crime-data/temporal-analysis")
                
                logger.info(f"Batch {epoch_id}: Métricas temporales escritas a HDFS")
                
                # 5. MÉTRICAS AVANZADAS - Análisis de Densidad por Distrito
                logger.info(f"Batch {epoch_id}: Calculando densidad criminal por distrito...")
                density_df = df.groupBy("date_partition", "district") \
                    .agg(
                        count("*").alias("total_crimes"),
                        countDistinct("crime_type").alias("crime_type_variety"),
                        collect_list("crime_type").alias("crime_types_list"),
                        max("processing_time").alias("last_updated")
                    ) \
                    .withColumn("crime_density_score", col("total_crimes") * col("crime_type_variety"))
                
                density_df.write \
                    .mode("overwrite") \
                    .partitionBy("date_partition") \
                    .option("compression", "gzip") \
                    .parquet("hdfs://namenode:9000/crime-data/density-analysis")
                
                logger.info(f"Batch {epoch_id}: Análisis de densidad escrito a HDFS")
                
                # 6. MÉTRICAS AVANZADAS - Alertas en Tiempo Real
                logger.info(f"Batch {epoch_id}: Calculando alertas en tiempo real...")
                
                # Definir umbrales críticos por tipo de crimen
                critical_thresholds = {
                    "sicariato": 2,
                    "asesinato": 3,
                    "secuestro": 1,
                    "extorsión": 5,
                    "robo": 15,
                    "estafa": 8
                }
                
                # Calcular conteo por tipo y distrito para detectar alertas
                alerts_df = df.groupBy("date_partition", "district", "crime_type") \
                    .agg(
                        count("*").alias("crime_count"),
                        max("processing_time").alias("alert_time")
                    )
                
                # Crear expresión case-when para todos los tipos de crimen de una vez
                is_critical_expr = when(lit(False), lit(True))  # Comienza con un caso falso
                
                for crime_type, threshold in critical_thresholds.items():
                    is_critical_expr = is_critical_expr.when(
                        (col("crime_type") == crime_type) & (col("crime_count") >= threshold),
                        lit(True)
                    )
                
                # Aplicar la expresión y agregar columna is_critical
                alerts_df = alerts_df.withColumn("is_critical", is_critical_expr.otherwise(lit(False)))
                
                # Solo guardar alertas críticas
                critical_alerts = alerts_df.filter(col("is_critical") == True) \
                    .withColumn("alert_level", lit("CRITICAL")) \
                    .withColumn("alert_message", 
                        concat(lit("ALERTA: "), col("crime_count"), lit(" casos de "), 
                               col("crime_type"), lit(" en "), col("district")))
                
                critical_alerts.write \
                    .mode("overwrite") \
                    .partitionBy("date_partition", "district") \
                    .option("compression", "gzip") \
                    .parquet("hdfs://namenode:9000/crime-data/real-time-alerts")
                
                logger.info(f"Batch {epoch_id}: Alertas en tiempo real evaluadas y escritas a HDFS")
                
                # 7. MÉTRICAS AVANZADAS - Correlaciones entre Tipos de Crimen
                logger.info(f"Batch {epoch_id}: Analizando correlaciones entre tipos de crimen...")
                
                # Crear matriz de co-ocurrencia por distrito y ventana de tiempo
                correlation_df = df.groupBy("date_partition", "district", "crime_type") \
                    .agg(
                        count("*").alias("occurrence_count"),
                        collect_list("cedula_hash").alias("reporters"),
                        max("processing_time").alias("last_occurrence")
                    )
                
                # Calcular métricas de correlación
                correlation_summary = correlation_df.groupBy("date_partition", "district") \
                    .agg(
                        collect_list("crime_type").alias("crime_types_in_area"),
                        sum("occurrence_count").alias("total_crimes_in_area"),
                        countDistinct("crime_type").alias("unique_crime_types"),
                        max("last_occurrence").alias("last_updated")
                    ) \
                    .withColumn("crime_diversity_index", 
                        col("unique_crime_types") / lit(6.0))  # 6 tipos totales
                
                correlation_summary.write \
                    .mode("overwrite") \
                    .partitionBy("date_partition") \
                    .option("compression", "gzip") \
                    .parquet("hdfs://namenode:9000/crime-data/correlation-analysis")
                
                logger.info(f"Batch {epoch_id}: Análisis de correlaciones escrito a HDFS")
                
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
