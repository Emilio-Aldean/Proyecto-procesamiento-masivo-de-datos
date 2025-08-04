# Análisis de Crimen en Guayaquil y Samborondón

Sistema de procesamiento de datos de crimen en tiempo real para las ciudades de Guayaquil y Samborondón.

## Descripción del Proyecto

Este proyecto implementa un sistema completo de procesamiento de datos de crimen en tiempo real utilizando Apache Kafka para la ingesta de datos, Apache Spark Streaming para el procesamiento y HDFS para el almacenamiento persistente.

## Componentes Principales

- **Simulador de Datos de Crimen**: Genera datos sintéticos de crímenes y los envía a Kafka.
- **Procesador Spark Streaming**: Consume datos de Kafka, los procesa y los almacena en HDFS.
- **Almacenamiento HDFS**: Almacena los datos procesados y las estadísticas generadas.

## Características

- Procesamiento en tiempo real con latencia de 6 segundos
- Particionado de datos por fecha y distrito
- Generación de estadísticas por tipo de crimen y distrito
- Anonimización de datos personales mediante hash de cédula
- Despliegue completo en contenedores Docker

## Requisitos

- Docker y Docker Compose
- Python 3.8+
- Apache Spark 3.3.4
- Apache Kafka 3.5.1
- Hadoop HDFS

## Estructura del Proyecto

```
guayaquil_crime_analysis/
├── src/                      # Código fuente
│   ├── crime_data_simulator_docker.py  # Simulador de datos
│   ├── spark_crime_processor_hdfs.py   # Procesador Spark
│   └── verify_metrics.py     # Script de verificación
├── docker_scripts/           # Scripts para Docker
│   └── start_all_services.sh # Script de inicio
├── config/                   # Archivos de configuración
└── README.md                 # Este archivo
```

## Instalación y Ejecución

1. Clonar el repositorio
2. Ejecutar el script de inicio:
   ```
   ./docker_scripts/start_all_services.sh
   ```
3. Verificar los datos procesados en HDFS:
   ```
   docker exec namenode hdfs dfs -ls /crime-data/processed
   ```

## Estado Actual

- Sistema estable y funcional
- Implementados todos los requisitos básicos
- Pendiente: métricas avanzadas (en desarrollo)
