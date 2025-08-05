#!/bin/bash
# Script corregido con terminaciones de línea Unix (LF) para compatibilidad con Docker

echo "=== INICIANDO TODOS LOS SERVICIOS EN CONTENEDOR ÚNICO ==="

# Función para logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Función para verificar si un servicio está corriendo
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=30
    local attempt=1
    
    log "Esperando a que $service_name esté disponible en $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            log "$service_name está disponible!"
            return 0
        fi
        log "Intento $attempt/$max_attempts - $service_name no está listo, esperando..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log "ERROR: $service_name no se pudo iniciar después de $max_attempts intentos"
    return 1
}

# 1. Inicializar Kafka KRaft
log "Inicializando Kafka KRaft..."
cd $KAFKA_HOME
./bin/kafka-storage.sh format -t kraft-cluster-id-12345 -c config/kraft/server.properties

# 2. Iniciar Kafka en background
log "Iniciando Kafka..."
./bin/kafka-server-start.sh config/kraft/server.properties &
KAFKA_PID=$!

# Esperar a que Kafka esté listo
wait_for_service localhost 9092 "Kafka"
if [ $? -ne 0 ]; then
    log "ERROR: Kafka no se pudo iniciar"
    exit 1
fi

# 3. Crear tópicos
log "Creando tópicos de Kafka..."
./bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --topic raw-crimes
./bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --topic processed-crimes
./bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic crime-statistics

log "Tópicos creados exitosamente"

# 4. Iniciar Spark Master
log "Iniciando Spark Master..."
cd $SPARK_HOME
export SPARK_MASTER_HOST=localhost
./sbin/start-master.sh
wait_for_service localhost 8080 "Spark Master UI"

# 5. Iniciar Spark Worker
log "Iniciando Spark Worker..."
./sbin/start-worker.sh spark://localhost:7077

# 6. Iniciar Crime Data Simulator en background
log "Iniciando Crime Data Simulator..."
cd /app
python3 crime_data_simulator_docker.py &
SIMULATOR_PID=$!

# Esperar un poco para que el simulador se inicialice
sleep 5

# 7. Iniciar Spark Streaming Application (Advanced Version)
log "Iniciando Spark Streaming Advanced Application..."
spark-submit \
  --master local[*] \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.streaming.metricsEnabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --driver-memory 2g \
  --executor-memory 2g \
  spark_crime_processor_hdfs.py &
SPARK_APP_PID=$!

log "Todos los servicios iniciados exitosamente!"
log "PIDs: Kafka=$KAFKA_PID, Simulator=$SIMULATOR_PID, SparkApp=$SPARK_APP_PID"

# 8. Mostrar información de servicios
log "=== SERVICIOS DISPONIBLES ==="
log "Kafka:              localhost:9092"
log "Spark Master UI:    http://localhost:8080"
log "Spark Master:       spark://localhost:7077"

# 9. Función de limpieza
cleanup() {
    log "Deteniendo servicios..."
    kill $SPARK_APP_PID $SIMULATOR_PID 2>/dev/null
    $SPARK_HOME/sbin/stop-worker.sh
    $SPARK_HOME/sbin/stop-master.sh
    kill $KAFKA_PID 2>/dev/null
    log "Servicios detenidos"
    exit 0
}

# Capturar señales para limpieza
trap cleanup SIGTERM SIGINT

# 10. Monitorear servicios
log "Monitoreando servicios... (Ctrl+C para detener)"
while true; do
    # Verificar que los procesos principales estén corriendo
    if ! kill -0 $KAFKA_PID 2>/dev/null; then
        log "ERROR: Kafka se detuvo inesperadamente"
        cleanup
    fi
    
    if ! kill -0 $SIMULATOR_PID 2>/dev/null; then
        log "WARNING: Crime Simulator se detuvo, reiniciando..."
        python3 crime_data_simulator_docker.py &
        SIMULATOR_PID=$!
    fi
    
    if ! kill -0 $SPARK_APP_PID 2>/dev/null; then
        log "WARNING: Spark Application se detuvo, reiniciando..."
        spark-submit \
          --master spark://localhost:7077 \
          --conf spark.sql.adaptive.enabled=false \
          --conf spark.sql.adaptive.coalescePartitions.enabled=false \
          --driver-memory 1g \
          --executor-memory 1g \
          spark_crime_processor_simple.py &
        SPARK_APP_PID=$!
    fi
    
    sleep 30
done
