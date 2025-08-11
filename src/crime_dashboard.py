#!/usr/bin/env python3
"""
Dashboard de Visualización en Tiempo Real
Muestra crímenes de Guayaquil y Samborondón en mapa interactivo
"""

from flask import Flask, render_template_string, jsonify
import folium
from datetime import datetime, timedelta
import subprocess
import json
import threading
from threading import Thread
import time
import pandas as pd
import pyarrow.parquet as pq
import io
import tempfile
import os
import webbrowser
import signal
import sys
import shutil
import uuid

# Configuración del mapa
MAP_CENTER = [-2.15, -79.88]  # Centro entre Guayaquil y Samborondón
ZOOM_LEVEL = 11

# CORRECCIÓN GPT-5: Configuración de contenedores
HDFS_CONTAINER = os.getenv("HDFS_CONTAINER", "namenode")
OPEN_BROWSER = os.getenv("OPEN_BROWSER", "1") != "0"

# CONFIGURACIÓN SEGÚN REQUISITOS DEL PROYECTO
SPARK_INTERVAL = 6      # segundos - Requisito: Spark Streaming cada 6 segundos
MARKER_DURATION = 60    # segundos - Requisito: punto dure dibujado 1 minuto
POLLER_INTERVAL = 6     # segundos - sincronizado con Spark Streaming
MAX_CRIMES_PER_CYCLE = 20  # límite para rendimiento

# Crear app Flask
app = Flask(__name__)

# CORRECCIÓN GPT-5: Sistema de cache en background
LATEST_CRIMES = []
LATEST_LOCK = threading.Lock()

def check_hdfs_connection():
    """Valida conexión a HDFS antes de iniciar"""
    try:
        cmd = ["docker", "exec", HDFS_CONTAINER, "hdfs", "dfs", "-ls", "/"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.returncode == 0
    except Exception as e:
        print(f"[WARNING] No se puede conectar a HDFS: {e}")
        return False

def hdfs_poller():
    """Poller que obtiene crímenes FRESCOS de archivos RECIENTES en HDFS cada POLL_INTERVAL segundos"""
    global LATEST_CRIMES
    print(f"[POLLER] ===== INICIANDO POLLER HDFS REAL-TIME =====")
    print(f"[POLLER] Intervalo: {POLLER_INTERVAL}s")
    print(f"[POLLER] Leyendo archivos MÁS RECIENTES de HDFS")
    print(f"[POLLER] Max crímenes por ciclo: {MAX_CRIMES_PER_CYCLE}")
    print()
    
    while True:
        try:
            print(f"[POLLER] ===== NUEVO CICLO DE POLLING =====")
            print(f"[POLLER] Timestamp: {datetime.now().isoformat()}")
            print(f"[POLLER] Buscando archivos FRESCOS en HDFS...")
            
            # CORRECCIÓN GPT-5: Usar función optimizada en batch
            crimes = read_fresh_hdfs_files()
            
            with LATEST_LOCK:
                LATEST_CRIMES.clear()
                LATEST_CRIMES.extend(crimes)
            
            print(f"[POLLER] Cache actualizado con {len(crimes)} crimenes FRESCOS")
            print(f"[POLLER] Próximo ciclo en {POLLER_INTERVAL} segundos...")
            print()
            
        except Exception as e:
            print(f"[POLLER ERROR] Error critico en ciclo de polling:")
            print(f"[POLLER ERROR] Tipo: {type(e).__name__}")
            print(f"[POLLER ERROR] Mensaje: {str(e)}")
            import traceback
            print(f"[POLLER ERROR] Traceback: {traceback.format_exc()}")
        
        time.sleep(POLLER_INTERVAL)

# Template HTML para el mapa
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Dashboard Criminal - Guayaquil & Samborondón</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet-realtime@2.2.0/dist/leaflet-realtime.min.css"/>
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet-realtime@2.2.0/dist/leaflet-realtime.min.js"></script>
    <style>
        body { margin: 0; padding: 0; }
        #map { height: 100vh; width: 100vw; }
        .info-panel {
            position: absolute;
            top: 10px;
            right: 10px;
            background: white;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);
            z-index: 1000;
        }
        .crime-robo { color: #ff6b6b; }
        .crime-extorsion { color: #4ecdc4; }
        .crime-sicariato { color: #45b7d1; }
        .crime-asesinato { color: #f9ca24; }
        .crime-secuestro { color: #f0932b; }
        .crime-estafa { color: #eb4d4b; }
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="info-panel">
        <h3>CRIMENES EN TIEMPO REAL</h3>
        <p>Guayaquil & Samborondon</p>
        <p>Actualizacion: cada 6 segundos</p>
        <p>Duracion: 1 minuto por punto</p>
        <div id="stats">
            <p>Crimenes activos: <span id="active-crimes">0</span></p>
        </div>
    </div>

    <script>
        // Crear mapa
        var map = L.map('map').setView({{ MAP_CENTER }}, {{ ZOOM_LEVEL }});
        
        // Agregar capa base
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(map);

        // Función para obtener color por tipo de crimen
        function getCrimeColor(crimeType) {
            const colors = {
                'robo': '#ff6b6b',
                'extorsión': '#4ecdc4', 
                'sicariato': '#45b7d1',
                'asesinato': '#f9ca24',
                'secuestro': '#f0932b',
                'estafa': '#eb4d4b'
            };
            return colors[crimeType] || '#666666';
        }

        // Función para crear marcador personalizado
        function createCrimeMarker(feature, latlng) {
            return L.circleMarker(latlng, {
                radius: 8,
                fillColor: getCrimeColor(feature.properties.crime_type),
                color: "#000",
                weight: 1,
                opacity: 1,
                fillOpacity: 0.8
            });
        }

        // Sistema manual de actualización en tiempo real
        console.log('[DEBUG] Iniciando sistema manual de actualización...');
        
        var activeMarkers = new Map(); // Gestión manual de marcadores
        
        function updateCrimes() {
            console.log('[DEBUG] Fetching crimes from API...');
            
            fetch('/api/crimes/realtime')
                .then(response => {
                    console.log('[DEBUG] API Response status:', response.status);
                    return response.json();
                })
                .then(data => {
                    console.log('[DEBUG] API Data received:', data);
                    
                    if (data && data.features) {
                        console.log('[DEBUG] Processing', data.features.length, 'crimes');
                        
                        // AGREGAR nuevos marcadores (sin limpiar los existentes)
                        data.features.forEach(function(feature) {
                            var props = feature.properties;
                            var coords = feature.geometry.coordinates; // [lon, lat] formato GeoJSON
                            var crimeId = 'crime_' + Date.now() + '_' + Math.random(); // ID único
                            
                            // Leaflet usa [lat, lon], API devuelve [lon, lat]
                            var marker = createCrimeMarker(feature, [coords[1], coords[0]]);
                            
                            // Crear popup
                            var popup = `
                                <div>
                                    <h4>${props.crime_type.toUpperCase()}</h4>
                                    <p><strong>Distrito:</strong> ${props.district}</p>
                                    <p><strong>Hora:</strong> ${new Date(props.timestamp).toLocaleTimeString()}</p>
                                    <p><strong>Descripcion:</strong> ${props.description}</p>
                                </div>
                            `;
                            marker.bindPopup(popup);
                            marker.addTo(map);
                            
                            activeMarkers.set(crimeId, {
                                marker: marker,
                                props: props,
                                addedAt: Date.now()
                            });
                            console.log('[DEBUG] AGREGANDO punto nuevo:', crimeId);
                        });
                        
                        // Limpiar marcadores antiguos (después de agregar)
                        var now = Date.now();
                        var toRemove = [];
                        activeMarkers.forEach(function(markerData, crimeId) {
                            if (now - markerData.addedAt > 60000) { // 1 minuto
                                map.removeLayer(markerData.marker);
                                toRemove.push(crimeId);
                                console.log('[DEBUG] REMOVIENDO punto antiguo:', crimeId);
                            }
                        });
                        toRemove.forEach(function(crimeId) {
                            activeMarkers.delete(crimeId);
                        });
                        
                        // TERCERO: Contar SOLO marcadores realmente visibles en el mapa
                        var visibleCount = 0;
                        activeMarkers.forEach(function(markerData, crimeId) {
                            if (map.hasLayer(markerData.marker)) {
                                visibleCount++;
                            }
                        });
                        
                        document.getElementById('active-crimes').textContent = visibleCount;
                        console.log('[DEBUG] Marcadores visibles en mapa:', visibleCount);
                        console.log('[DEBUG] Total en activeMarkers:', activeMarkers.size);
                        
                    } else {
                        console.log('[WARNING] No se recibieron datos válidos');
                    }
                })
                .catch(error => {
                    console.error('[ERROR] Error fetching crimes:', error);
                });
        }
        
        // Actualizar cada 6 segundos
        updateCrimes(); // Primera carga
        setInterval(updateCrimes, 6000);

        // Agregar leyenda
        var legend = L.control({position: 'bottomleft'});
        legend.onAdd = function (map) {
            var div = L.DomUtil.create('div', 'info legend');
            div.innerHTML = `
                <h4>Tipos de Crimen</h4>
                <div><span style="color: #ff6b6b;">●</span> Robo</div>
                <div><span style="color: #4ecdc4;">●</span> Extorsión</div>
                <div><span style="color: #45b7d1;">●</span> Sicariato</div>
                <div><span style="color: #f9ca24;">●</span> Asesinato</div>
                <div><span style="color: #f0932b;">●</span> Secuestro</div>
                <div><span style="color: #eb4d4b;">●</span> Estafa</div>
            `;
            div.style.background = 'white';
            div.style.padding = '10px';
            div.style.borderRadius = '5px';
            div.style.boxShadow = '0 2px 5px rgba(0,0,0,0.2)';
            return div;
        };
        legend.addTo(map);
    </script>
</body>
</html>
"""

def read_fresh_hdfs_files():
    """Lee archivos Parquet MÁS RECIENTES que Spark acaba de escribir en HDFS usando WebHDFS"""
    print("[DEBUG] ===== LEYENDO ARCHIVOS FRESCOS DE HDFS VIA WEBHDFS =====")
    try:
        import requests
        crimes_data = []
        current_date = datetime.now().strftime('%Y-%m-%d')
        current_time = datetime.now()
        all_districts = ['Centro', 'Norte', 'Sur', 'Dur?n', 'Samborond?n', 'V?a a la Costa']
        
        print(f"[DEBUG] Fecha objetivo: {current_date}")
        print(f"[DEBUG] Buscando archivos frescos en {len(all_districts)} distritos...")
        
        for district in all_districts:
            if len(crimes_data) >= MAX_CRIMES_PER_CYCLE:
                break
                
            hdfs_path = f"/crime-data/processed/date_partition={current_date}/district={district}"
            try:
                # Usar WebHDFS REST API para listar archivos
                webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
                response = requests.get(webhdfs_url, timeout=10)
                
                if response.status_code == 200:
                    hdfs_data = response.json()
                    file_statuses = hdfs_data.get('FileStatuses', {}).get('FileStatus', [])
                    
                    # Buscar archivos Parquet más recientes
                    parquet_files = [f for f in file_statuses if f['pathSuffix'].endswith('.parquet')]
                    if parquet_files:
                        # Ordenar por tiempo de modificación (más reciente primero)
                        parquet_files.sort(key=lambda x: x['modificationTime'], reverse=True)
                        
                        # Procesar los 2 archivos más recientes por distrito
                        for file_info in parquet_files[:2]:
                            file_path = f"{hdfs_path}/{file_info['pathSuffix']}"
                            print(f"[DEBUG] Procesando archivo FRESCO: {file_path}")
                            
                            # Usar WebHDFS para leer el archivo
                            file_crimes = read_parquet_from_webhdfs(file_path)
                            if file_crimes:
                                crimes_data.extend(file_crimes[:10])  # Más crímenes por archivo fresco
                                print(f"[DEBUG] Agregados {len(file_crimes[:10])} crímenes FRESCOS de {district}")
                    else:
                        print(f"[DEBUG] No hay archivos Parquet en {district}")
                else:
                    print(f"[WARNING] WebHDFS error para {district}: HTTP {response.status_code}")
                            
            except Exception as e:
                print(f"[WARNING] Error procesando {district}: {e}")
                continue
        
        print(f"[DEBUG] Total crímenes FRESCOS obtenidos: {len(crimes_data)}")
        
        if len(crimes_data) > 0:
            return crimes_data[:MAX_CRIMES_PER_CYCLE]
        else:
            print("[ERROR] No hay archivos frescos en HDFS - verificar conexión y datos")
            return []
            
    except Exception as e:
        print(f"[ERROR] Error leyendo archivos frescos: {e}")
        print(f"[ERROR] Detalles del error: {str(e)}")
        return []

def read_latest_crimes_from_hdfs():
    """
    LECTURA DIRECTA DE HDFS: Conecta directamente sin Docker commands
    Lee datos reales del pipeline Kafka->Spark->HDFS que está funcionando al 100%
    """
    print("[DEBUG] ===== LECTURA DIRECTA DE HDFS =====")
    
    try:
        crimes_data = []
        # CORREGIR: Usar fecha actual para datos frescos del pipeline
        current_date = datetime.now().strftime('%Y-%m-%d')
        target_date = current_date  # 2025-08-11 para datos actuales
        
        # TODOS los distritos REALES en HDFS (con caracteres especiales)
        all_districts = ['Centro', 'Norte', 'Sur', 'Dur?n', 'Samborond?n', 'V?a a la Costa']
        print(f"[INFO] Procesando {len(all_districts)} distritos desde HDFS - Fecha: {target_date}")
        
        # ESTRATEGIA DIRECTA: Usar subprocess para conectar directamente a HDFS
        for district in all_districts:
            if len(crimes_data) >= MAX_CRIMES_PER_CYCLE:
                break
                
            print(f"[INFO] === Procesando distrito: {district} ===")
            hdfs_path = f"/crime-data/processed/date_partition={target_date}/district={district}"
            
            try:
                # USAR WebHDFS REST API para conectar directamente a namenode
                import requests
                webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
                
                response = requests.get(webhdfs_url, timeout=10)
                
                if response.status_code == 200:
                    hdfs_data = response.json()
                    file_statuses = hdfs_data.get('FileStatuses', {}).get('FileStatus', [])
                    
                    # Buscar archivos Parquet más recientes
                    parquet_files = [f for f in file_statuses if f['pathSuffix'].endswith('.parquet')]
                    if parquet_files:
                        # Ordenar por tiempo de modificación (más reciente primero)
                        parquet_files.sort(key=lambda x: x['modificationTime'], reverse=True)
                        latest_file = parquet_files[0]
                        file_path = f"{hdfs_path}/{latest_file['pathSuffix']}"
                        print(f"[INFO] Leyendo archivo de {district}: {file_path}")
                        
                        # Leer archivo Parquet usando WebHDFS
                        file_crimes = read_parquet_from_webhdfs(file_path)
                        if file_crimes:
                            crimes_data.extend(file_crimes[:5])  # Max 5 por distrito
                            print(f"[SUCCESS] {len(file_crimes)} crimenes de {district}")
                else:
                    print(f"[WARNING] Sin archivos recientes en {district}")
                    
            except Exception as e:
                print(f"[ERROR] Error procesando distrito {district}: {e}")
                continue
        
        if len(crimes_data) > 0:
            print(f"[SUCCESS] {len(crimes_data)} crimenes reales obtenidos de HDFS")
            print(f"[INFO] Datos de {len(set(c.get('district', 'N/A') for c in crimes_data))} distritos procesados")
            return crimes_data[:MAX_CRIMES_PER_CYCLE]
        else:
            print("[WARNING] HDFS no devolvio datos - usando fallback a Kafka")
            return get_recent_crimes_from_kafka()
        
    except Exception as e:
        print(f"[ERROR] Error en lectura HDFS: {e}")
        crimes = read_fresh_hdfs_files()
        return crimes

def try_fallback_hdfs_access():
    """
    Método de fallback: usar el simulador en vivo para obtener datos recientes
    """
    print("[INFO] === METODO FALLBACK: DATOS DEL SIMULADOR ===")
    
    try:
        # Verificar si el simulador está generando datos en Kafka
        print("[INFO] Verificando datos del simulador en Kafka...")
        
        cmd_kafka = [
            'docker', 'exec', 'crime-analysis-container', 
            '/opt/kafka/bin/kafka-console-consumer.sh',
            '--bootstrap-server', 'localhost:9092',
            '--topic', 'raw-crimes',
            '--from-beginning',
            '--max-messages', '5',
            '--timeout-ms', '10000'
        ]
        
        result = subprocess.run(cmd_kafka, capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0 and result.stdout.strip():
            print("[SUCCESS] Simulador activo - procesando datos de Kafka")
            
            # Parsear datos JSON de Kafka
            crimes_data = []
            lines = result.stdout.strip().split('\n')
            
            for line in lines:
                if line.strip() and line.startswith('{'):
                    try:
                        import json
                        crime_json = json.loads(line)
                        
                        # Convertir formato Kafka a formato esperado
                        crime = {
                            'crime_id': crime_json.get('crime_id', 'KAFKA-' + str(uuid.uuid4())[:8]),
                            'timestamp': crime_json.get('timestamp', datetime.now().isoformat()),
                            'lat': crime_json.get('location', {}).get('coordinates', {}).get('lat', -2.1969),
                            'lon': crime_json.get('location', {}).get('coordinates', {}).get('lon', -79.8804),
                            'crime_type': crime_json.get('crime_type', 'robo'),
                            'district': crime_json.get('location', {}).get('district', 'Centro'),
                            'description': f"[KAFKA-REAL] {crime_json.get('description', 'Crimen en tiempo real')}"
                        }
                        
                        crimes_data.append(crime)
                        
                    except Exception as e:
                        print(f"[WARNING] Error parseando JSON: {e}")
                        continue
            
            if crimes_data:
                print(f"[SUCCESS] {len(crimes_data)} crimenes obtenidos del simulador")
                return crimes_data
        
        print("[WARNING] No hay datos disponibles del simulador")
        return []
        
    except Exception as e:
        print(f"[ERROR] Error en metodo fallback: {e}")
        return []

def read_multiple_parquet_from_hdfs_batch(hdfs_file_paths):
    """
    OPTIMIZACIÓN: Lee múltiples archivos Parquet de HDFS en batch
    Reduce comandos Docker de 4-6 → 2-3 por ciclo
    """
    import uuid
    
    if not hdfs_file_paths:
        return []
    
    print(f"[BATCH] Leyendo {len(hdfs_file_paths)} archivos en batch...")
    
    try:
        # Crear directorio temporal único en contenedor
        batch_id = str(uuid.uuid4())[:8]
        container_temp_dir = f'/tmp/hdfs_batch_{batch_id}'
        
        # Crear directorio temporal en contenedor
        cmd_mkdir = ['docker', 'exec', HDFS_CONTAINER, 'mkdir', '-p', container_temp_dir]
        subprocess.run(cmd_mkdir, capture_output=True, text=True, timeout=10)
        
        # OPTIMIZACIÓN: Descargar todos los archivos en un solo comando
        container_paths = []
        for i, hdfs_path in enumerate(hdfs_file_paths):
            container_file = f'{container_temp_dir}/file_{i}.parquet'
            container_paths.append(container_file)
            
            # Descargar archivo individual
            cmd_get = ['docker', 'exec', HDFS_CONTAINER, 'hdfs', 'dfs', '-get', hdfs_path, container_file]
            result = subprocess.run(cmd_get, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                print(f"[WARNING] No se pudo descargar {hdfs_path}: {result.stderr}")
                continue
        
        # Crear directorio temporal local
        local_temp_dir = tempfile.mkdtemp()
        
        # OPTIMIZACIÓN: Copiar todos los archivos del contenedor en batch
        if container_paths:
            cmd_copy = ['docker', 'cp', f'{HDFS_CONTAINER}:{container_temp_dir}/.', local_temp_dir]
            result_copy = subprocess.run(cmd_copy, capture_output=True, text=True, timeout=30)
            
            if result_copy.returncode != 0:
                print(f"[WARNING] Error copiando batch: {result_copy.stderr}")
                return []
        
        # Procesar todos los archivos locales
        all_crimes = []
        for local_file in os.listdir(local_temp_dir):
            if local_file.endswith('.parquet'):
                local_path = os.path.join(local_temp_dir, local_file)
                try:
                    crimes = process_parquet_file(local_path)
                    all_crimes.extend(crimes)
                    print(f"[BATCH] Procesado {local_file}: {len(crimes)} crímenes")
                    
                    # Limitar total
                    if len(all_crimes) >= MAX_CRIMES_PER_CYCLE:
                        break
                        
                except Exception as e:
                    print(f"[WARNING] Error procesando {local_file}: {e}")
                    continue
        
        # Limpiar archivos temporales
        try:
            # Limpiar contenedor
            cmd_clean = ['docker', 'exec', HDFS_CONTAINER, 'rm', '-rf', container_temp_dir]
            subprocess.run(cmd_clean, capture_output=True, text=True, timeout=10)
            
            # Limpiar local
            import shutil
            shutil.rmtree(local_temp_dir, ignore_errors=True)
        except:
            pass
        
        return all_crimes[:MAX_CRIMES_PER_CYCLE]
        
    except Exception as e:
        print(f"[ERROR] Error en batch reading: {e}")
        return []

def process_parquet_file(temp_path):
    """
    Procesa un archivo Parquet local y devuelve lista de crímenes
    Extraído de read_parquet_from_hdfs para reutilización
    """
    crimes = []
    
    try:
        # GPT-5: Leer solo columnas necesarias para rendimiento
        required_cols = ["timestamp", "crime_type", "district", "lat", "lon", "description", "crime_data"]
        
        # Leer archivo Parquet completo (sin nrows que no existe)
        df = pd.read_parquet(temp_path)
        
        # Filtrar solo columnas necesarias si existen
        available_cols = [col for col in required_cols if col in df.columns]
        if available_cols:
            df = df[available_cols]
            
        print(f"[INFO] Archivo Parquet procesado: {len(df)} registros")
        
        # Convertir DataFrame a formato del dashboard - ADAPTABLE
        for _, row in df.iterrows():
            try:
                # CORRECCIÓN GPT-5: Parsing robusto con try/except individual
                if 'crime_data' in df.columns and pd.notna(row['crime_data']):
                    # Formato con JSON anidado - manejo robusto de tipos
                    try:
                        raw = row['crime_data']
                        if isinstance(raw, dict):
                            crime_json = raw
                        elif isinstance(raw, (bytes, bytearray)):
                            crime_json = json.loads(raw.decode('utf-8'))
                        else:
                            crime_json = json.loads(str(raw))
                            
                        location = crime_json.get('location', {})
                        coords = location.get('coordinates', {})
                        lat_val = float(coords.get('lat', coords.get('latitude', -2.1894)))
                        lon_val = float(coords.get('lon', coords.get('longitude', -79.889)))
                        timestamp = crime_json.get('timestamp', '') or row.get('timestamp', '')
                        crime_type = crime_json.get('crime_type', 'desconocido')
                        district = location.get('district', row.get('district', 'Centro'))
                        description = crime_json.get('description', f"{crime_type} en {district} [HDFS-REAL]")
                    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
                        print(f"[WARNING] Error parsing JSON en crime_data: {e}")
                        continue
                    
                    crime_data = {
                        'timestamp': timestamp,
                        'crime_type': crime_type,
                        'district': district,
                        'lat': lat_val,
                        'lon': lon_val,
                        'description': description
                    }
                else:
                    # Formato con columnas separadas
                    lat_val = float(row.get('lat', row.get('latitude', -2.1894)))
                    lon_val = float(row.get('lon', row.get('longitude', -79.889)))
                    timestamp = row.get('timestamp', '')
                    crime_type = row.get('crime_type', 'desconocido')
                    district = row.get('district', 'Centro')
                    description = row.get('description', f"{crime_type} en {district} [HDFS-REAL]")
                    
                    crime_data = {
                        'timestamp': timestamp,
                        'crime_type': crime_type,
                        'district': district,
                        'lat': lat_val,
                        'lon': lon_val,
                        'description': description
                    }
                
                crimes.append(crime_data)
                
            except Exception as e:
                print(f"[WARNING] Error parseando fila: {e}")
                continue
        
        return crimes
        
    except Exception as e:
        print(f"[ERROR] Error procesando archivo Parquet: {e}")
        return []

def read_parquet_from_hdfs(hdfs_file_path):
    """
    Lee un archivo Parquet específico desde HDFS
    """
    try:
        # Crear archivo temporal para descargar el Parquet
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_path = temp_file.name
        
        # Crear nombre único para archivo temporal en el contenedor
        import uuid
        unique_id = str(uuid.uuid4())[:8]
        container_temp_path = f'/tmp/hdfs_temp_{unique_id}.parquet'
        
        # Descargar archivo desde HDFS al contenedor
        cmd_get = ['docker', 'exec', HDFS_CONTAINER, 'hdfs', 'dfs', '-get', hdfs_file_path, container_temp_path]
        result_get = subprocess.run(cmd_get, capture_output=True, text=True, timeout=30)
        
        if result_get.returncode != 0:
            print(f"[ERROR] No se pudo descargar {hdfs_file_path}: {result_get.stderr}")
            return []
        
        # Copiar archivo del contenedor al sistema local
        cmd_copy = ['docker', 'cp', f'{HDFS_CONTAINER}:{container_temp_path}', temp_path]
        result_copy = subprocess.run(cmd_copy, capture_output=True, text=True, timeout=30)
        
        if result_copy.returncode != 0:
            print(f"[ERROR] No se pudo copiar archivo: {result_copy.stderr}")
            return []
        
        # Procesar archivo Parquet usando función compartida
        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
            crimes = process_parquet_file(temp_path)
            
            # Limpiar archivos temporales
            try:
                os.unlink(temp_path)
            except:
                pass
            
            # Limpiar archivo temporal en el contenedor
            cmd_cleanup = ['docker', 'exec', 'namenode', 'rm', '-f', container_temp_path]
            subprocess.run(cmd_cleanup, capture_output=True, text=True, timeout=10)
            
            return crimes
        else:
            print(f"[WARNING] Archivo temporal vacío o no existe")
            return []
            
    except Exception as e:
        print(f"[ERROR] Error leyendo Parquet {hdfs_file_path}: {e}")
        return []

def read_parquet_from_webhdfs(hdfs_file_path):
    """
    Lee un archivo Parquet desde HDFS usando WebHDFS REST API
    Conexión directa sin Docker commands
    """
    try:
        import requests
        import tempfile
        
        # Usar WebHDFS REST API para leer el archivo
        webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_file_path}?op=OPEN"
        
        print(f"[DEBUG] Descargando via WebHDFS: {hdfs_file_path}")
        response = requests.get(webhdfs_url, timeout=30)
        
        if response.status_code == 200:
            # Crear archivo temporal para procesar el Parquet
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
                temp_file.write(response.content)
                temp_path = temp_file.name
            
            # Procesar archivo Parquet usando función existente
            if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                crimes = process_parquet_file(temp_path)
                
                # Limpiar archivo temporal
                try:
                    os.unlink(temp_path)
                except:
                    pass
                
                print(f"[SUCCESS] WebHDFS: {len(crimes)} crímenes leídos de {hdfs_file_path}")
                return crimes
            else:
                print(f"[WARNING] WebHDFS: Archivo temporal vacío")
                return []
        else:
            print(f"[ERROR] WebHDFS: HTTP {response.status_code} para {hdfs_file_path}")
            return []
            
    except Exception as e:
        print(f"[ERROR] WebHDFS error leyendo {hdfs_file_path}: {e}")
        return []

def filter_recent_crimes(crimes_data):
    """
    ELIMINADO - No filtrar datos reales de HDFS
    Los datos de HDFS son válidos tal como están
    """
    print(f"[DEBUG] Función filter_recent_crimes DESHABILITADA - usando datos directos")
    return crimes_data

def get_recent_crimes_from_kafka():
    """
    SOLO DATOS REALES: Obtiene datos del simulador Kafka (también datos reales del sistema)
    NO genera datos DEMO - usa el simulador que ya está funcionando
    """
    print("[INFO] Obteniendo datos reales del simulador Kafka...")
    
    try:
        # Leer mensajes recientes del topic raw-crimes
        cmd_kafka = [
            'docker', 'exec', HDFS_CONTAINER, 'bash', '-c',
            'kafka-console-consumer --bootstrap-server localhost:9092 --topic raw-crimes --from-beginning --timeout-ms 5000 2>/dev/null | tail -20'
        ]
        
        result = subprocess.run(cmd_kafka, capture_output=True, text=True, timeout=8)
        
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            crimes = []
            current_time = datetime.now()
            
            for line in lines:
                try:
                    if line.strip():
                        crime_data = json.loads(line.strip())
                        
                        # Convertir formato Kafka a formato dashboard
                        crime = {
                            'timestamp': crime_data.get('timestamp', current_time.isoformat()),
                            'crime_type': crime_data.get('crime_type', 'robo'),
                            'district': crime_data.get('district', 'Centro'),
                            'lat': float(crime_data.get('lat', -2.15)),
                            'lon': float(crime_data.get('lon', -79.88)),
                            'crime_id': crime_data.get('crime_id', f'KAFKA-{int(time.time())}'),
                            'description': f"[KAFKA-REAL] {crime_data.get('description', 'Crimen reportado')}"
                        }
                        crimes.append(crime)
                        
                except Exception as e:
                    print(f"[WARNING] Error parseando mensaje Kafka: {e}")
                    continue
            
            print(f"[SUCCESS] {len(crimes)} crimenes reales obtenidos de Kafka")
            return crimes[-15:]  # Últimos 15 crímenes
        else:
            print("[ERROR] No se pudieron obtener datos de Kafka")
            return []
            
    except Exception as e:
        print(f"[ERROR] Error accediendo a Kafka: {e}")
        return []

@app.route("/")
def index():
    """Página principal con el mapa"""
    return render_template_string(HTML_TEMPLATE, 
                                MAP_CENTER=MAP_CENTER, 
                                ZOOM_LEVEL=ZOOM_LEVEL)

@app.route("/api/crimes/realtime")
def crimes_realtime():
    """API endpoint que devuelve crímenes REALES de HDFS - SIN FALLBACKS"""
    print("[API] ===== OBTENIENDO DATOS REALES DE HDFS =====")
    
    try:
        import requests
        import json
        import pyarrow.parquet as pq
        import io
        from datetime import datetime
        
        # Configuración
        current_date = datetime.now().strftime('%Y-%m-%d')
        districts = ['Centro', 'Norte', 'Sur']  # Empezar con 3 distritos
        crimes_data = []
        
        print(f"[API] Fecha objetivo: {current_date}")
        print(f"[API] Buscando en distritos: {districts}")
        
        for district in districts:
            if len(crimes_data) >= 15:  # Límite para respuesta rápida
                break
                
            print(f"[API] Procesando distrito: {district}")
            hdfs_path = f"/crime-data/processed/date_partition={current_date}/district={district}"
            
            try:
                # Listar archivos usando WebHDFS
                webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
                response = requests.get(webhdfs_url, timeout=10)
                
                if response.status_code == 200:
                    hdfs_data = response.json()
                    file_statuses = hdfs_data.get('FileStatuses', {}).get('FileStatus', [])
                    parquet_files = [f for f in file_statuses if f['pathSuffix'].endswith('.parquet')]
                    
                    if parquet_files:
                        # Tomar el archivo más reciente
                        latest_file = max(parquet_files, key=lambda x: x['modificationTime'])
                        file_path = f"{hdfs_path}/{latest_file['pathSuffix']}"
                        
                        print(f"[API] Descargando: {latest_file['pathSuffix'][:50]}...")
                        
                        # Descargar archivo usando WebHDFS
                        download_url = f"http://namenode:9870/webhdfs/v1{file_path}?op=OPEN"
                        download_response = requests.get(download_url, timeout=30)
                        
                        if download_response.status_code == 200:
                            # Parsear Parquet
                            parquet_data = io.BytesIO(download_response.content)
                            table = pq.read_table(parquet_data)
                            df = table.to_pandas()
                            
                            print(f"[API] ✅ Parseado: {len(df)} registros de {district}")
                            
                            # Procesar registros
                            for _, row in df.iterrows():
                                if len(crimes_data) >= 15:
                                    break
                                try:
                                    crime_data = json.loads(row['crime_data'])
                                    
                                    crime = {
                                        "crime_id": crime_data.get("crime_id", "unknown"),
                                        "timestamp": crime_data.get("timestamp", ""),
                                        "lat": crime_data["location"]["coordinates"]["lat"],
                                        "lon": crime_data["location"]["coordinates"]["lon"],
                                        "crime_type": crime_data.get("crime_type", "unknown"),
                                        "district": crime_data["location"].get("district", "unknown"),
                                        "description": f"{crime_data.get('crime_type', 'Crimen')} en {crime_data['location'].get('district', 'ubicación')} [HDFS-REAL]"
                                    }
                                    crimes_data.append(crime)
                                except Exception as e:
                                    print(f"[API] Error procesando registro: {e}")
                                    continue
                        else:
                            print(f"[API] Error descargando archivo: HTTP {download_response.status_code}")
                    else:
                        print(f"[API] No hay archivos Parquet en {district}")
                else:
                    print(f"[API] Error listando archivos: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"[API] Error procesando {district}: {e}")
                continue
        
        # Crear GeoJSON
        features = []
        for crime in crimes_data:
            try:
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [crime["lon"], crime["lat"]]  # [lon, lat] para GeoJSON
                    },
                    "properties": {
                        "crime_id": crime["crime_id"],
                        "timestamp": crime["timestamp"],
                        "crime_type": crime["crime_type"],
                        "district": crime["district"],
                        "description": crime["description"]
                    }
                })
            except Exception as e:
                print(f"[API] Error creando feature: {e}")
                continue
        
        print(f"[API] ✅ RETORNANDO {len(features)} CRÍMENES REALES DE HDFS")
        return jsonify({
            "type": "FeatureCollection",
            "features": features
        })
        
    except Exception as e:
        print(f"[API] ❌ ERROR CRÍTICO: {e}")
        return jsonify({
            "type": "FeatureCollection",
            "features": [],
            "error": f"Error accediendo a HDFS: {str(e)}"
        })

def open_browser():
    """Abre el navegador después de un delay"""
    time.sleep(2)
    webbrowser.open("http://localhost:5000")

def signal_handler(sig, frame):
    """Manejo limpio de señales de interrupción"""
    print("\n[INFO] Cerrando dashboard...")
    sys.exit(0)

if __name__ == "__main__":
    # Configurar manejo de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Iniciando Dashboard Criminal...")
    print("Mapa: Guayaquil & Samborondon")
    print("URL: http://172.21.0.7:5000 (Container IP)")
    print("Actualizacion: cada 6 segundos")
    print("Duracion por punto: 1 minuto")
    print(f"[INFO] Contenedor HDFS: {HDFS_CONTAINER}")
    
    # CORRECCIÓN GPT-5: Iniciar poller en background
    print("[INIT] Iniciando poller en background...")
    try:
        poller = Thread(target=hdfs_poller, daemon=True)
        poller.start()
        print("[INIT] Poller thread iniciado exitosamente")
    except Exception as e:
        print(f"[ERROR] Error iniciando poller thread: {e}")
        print(f"[ERROR] Tipo de error: {type(e).__name__}")
        print(f"[ERROR] Detalles: {str(e)}")
    
    # Abrir navegador solo si está configurado
    if OPEN_BROWSER:
        Thread(target=open_browser, daemon=True).start()
    
    try:
        # Iniciar servidor Flask en puerto 5000 (acceso via Container IP)
        app.run(host="0.0.0.0", port=5000, debug=False)
    except KeyboardInterrupt:
        print("\n[INFO] Dashboard cerrado por usuario")
    except Exception as e:
        print(f"\n[ERROR] Error en servidor Flask: {e}")
    finally:
        print("[INFO] Limpieza completada")
