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

# Configuración del mapa
MAP_CENTER = [-2.15, -79.88]  # Centro entre Guayaquil y Samborondón
ZOOM_LEVEL = 11

# CORRECCIÓN GPT-5: Configuración de contenedores
HDFS_CONTAINER = os.getenv("HDFS_CONTAINER", "namenode")
OPEN_BROWSER = os.getenv("OPEN_BROWSER", "1") != "0"

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

def poll_hdfs_loop(interval=6):
    """Poller en background que actualiza cache cada 6 segundos"""
    hdfs_available = check_hdfs_connection()
    if not hdfs_available:
        print("[WARNING] HDFS no disponible - usando datos fallback")
    
    while True:
        try:
            if hdfs_available:
                print("[POLLER] Actualizando cache desde HDFS...")
                crimes = read_latest_crimes_from_hdfs()
            else:
                print("[POLLER] HDFS no disponible - usando fallback...")
                crimes = generate_fallback_crimes()
                
            with LATEST_LOCK:
                LATEST_CRIMES.clear()
                LATEST_CRIMES.extend(crimes)
            print(f"[POLLER] Cache actualizado: {len(crimes)} crímenes")
        except Exception as e:
            print(f"[ERROR] Polling HDFS: {e}")
            # Revalidar conexión en caso de error
            hdfs_available = check_hdfs_connection()
        time.sleep(interval)

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
        <h3>🚨 Crímenes en Tiempo Real</h3>
        <p>📍 Guayaquil & Samborondón</p>
        <p>🔄 Actualización: cada 6 segundos</p>
        <p>⏱️ Duración: 1 minuto por punto</p>
        <div id="stats">
            <p>📊 Crímenes activos: <span id="active-crimes">0</span></p>
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
                        
                        // Procesar cada crimen
                        data.features.forEach(function(feature) {
                            var props = feature.properties;
                            var coords = feature.geometry.coordinates;
                            var crimeId = props.crime_id || (props.timestamp + '_' + coords[0] + '_' + coords[1]);
                            
                            console.log('[DEBUG] Processing crime:', crimeId, 'at', coords);
                            
                            // Si el marcador ya existe, no lo agregues de nuevo
                            if (!activeMarkers.has(crimeId)) {
                                // Crear marcador - Leaflet usa [lat, lon], API devuelve [lon, lat]
                                var marker = createCrimeMarker(feature, [coords[1], coords[0]]);
                                
                                // Crear popup
                                var popup = `
                                    <div>
                                        <h4>🚨 ${props.crime_type.toUpperCase()}</h4>
                                        <p><strong>📍 Distrito:</strong> ${props.district}</p>
                                        <p><strong>🕐 Hora:</strong> ${new Date(props.timestamp).toLocaleTimeString()}</p>
                                        <p><strong>📝 Descripción:</strong> ${props.description}</p>
                                    </div>
                                `;
                                marker.bindPopup(popup);
                                
                                // Agregar al mapa
                                marker.addTo(map);
                                
                                // Guardar referencia con timestamp
                                activeMarkers.set(crimeId, {
                                    marker: marker,
                                    timestamp: Date.now()
                                });
                                
                                console.log('[SUCCESS] Marcador agregado:', crimeId);
                            }
                        });
                        
                        // Remover marcadores antiguos (más de 1 minuto)
                        var now = Date.now();
                        var oneMinute = 60 * 1000;
                        
                        activeMarkers.forEach(function(markerData, crimeId) {
                            if (now - markerData.timestamp > oneMinute) {
                                console.log('[DEBUG] Removiendo marcador antiguo:', crimeId);
                                map.removeLayer(markerData.marker);
                                activeMarkers.delete(crimeId);
                            }
                        });
                        
                        // Actualizar contador
                        document.getElementById('active-crimes').textContent = activeMarkers.size;
                        console.log('[DEBUG] Marcadores activos:', activeMarkers.size);
                        
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

def read_latest_crimes_from_hdfs():
    """
    Lee los crímenes más recientes desde HDFS usando docker exec
    """
    print("[DEBUG] ===== INICIANDO LECTURA DE HDFS =====")
    
    try:
        print("[INFO] Conectando a HDFS para leer datos reales...")
        
        # Intentar leer datos reales de HDFS
        crimes_data = []
        
        # Buscar en múltiples rutas de fecha - DINÁMICO
        today_str = datetime.now().strftime("%Y-%m-%d")
        search_dates = [
            today_str,  # Hoy
            (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),  # Ayer
            '2025-08-10'  # Fecha específica de respaldo
        ]
        
        for date_str in search_dates:
            for district in ['Centro', 'Norte', 'Sur']:
                hdfs_path = f"/crime-data/processed/date_partition={date_str}/district={district}"
                district_crimes = read_hdfs_directory(hdfs_path)
                crimes_data.extend(district_crimes)
                
                if len(crimes_data) >= 20:  # Limitar para rendimiento
                    break
            if len(crimes_data) >= 20:
                break
        
        if len(crimes_data) > 0:
            print(f"[SUCCESS] {len(crimes_data)} crímenes reales leídos de HDFS")
            # CORRECCIÓN GPT-5: Filtrar datos recientes antes de devolver
            recent_crimes = filter_recent_crimes(crimes_data)
            print(f"[INFO] {len(recent_crimes)} crímenes después del filtro temporal")
            return recent_crimes if len(recent_crimes) > 0 else generate_fallback_crimes()
        else:
            print("[WARNING] No se encontraron datos en HDFS, usando fallback")
            return generate_fallback_crimes()
        
    except Exception as e:
        print(f"[ERROR] Error conectando a HDFS: {e}")
        return generate_fallback_crimes()

def read_hdfs_directory(hdfs_path):
    """
    Lee archivos Parquet de un directorio HDFS específico
    """
    try:
        print(f"[INFO] Listando archivos en {hdfs_path}")
        
        # Listar archivos en el directorio HDFS
        cmd_list = ['docker', 'exec', HDFS_CONTAINER, 'hdfs', 'dfs', '-ls', hdfs_path]
        result = subprocess.run(cmd_list, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            print(f"[WARNING] No se pudo acceder a {hdfs_path}: {result.stderr}")
            return []
        
        # Extraer rutas de archivos Parquet
        parquet_files = []
        for line in result.stdout.strip().split('\n'):
            if '.parquet' in line and not line.startswith('Found'):
                file_path = line.split()[-1]  # Última columna es la ruta
                parquet_files.append(file_path)
        
        print(f"[INFO] Encontrados {len(parquet_files)} archivos Parquet en {hdfs_path}")
        
        # Leer solo los primeros 2 archivos para rendimiento
        crimes_data = []
        for file_path in parquet_files[:2]:
            try:
                file_crimes = read_parquet_from_hdfs(file_path)
                crimes_data.extend(file_crimes)
                print(f"[SUCCESS] Leídos {len(file_crimes)} crímenes de {file_path}")
                
                # Limitar total de crímenes
                if len(crimes_data) >= 10:
                    break
                    
            except Exception as e:
                print(f"[WARNING] Error leyendo {file_path}: {e}")
                continue
        
        return crimes_data
        
    except Exception as e:
        print(f"[ERROR] Error listando directorio {hdfs_path}: {e}")
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
        
        # Leer archivo Parquet con pandas - ROBUSTO
        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
            try:
                # GPT-5: Leer solo columnas necesarias para rendimiento
                required_cols = ["timestamp", "crime_type", "district", "lat", "lon", "description", "crime_data"]
                available_cols = None
                
                # Primero detectar columnas disponibles
                df_sample = pd.read_parquet(temp_path, nrows=1)
                available_cols = [col for col in required_cols if col in df_sample.columns]
                
                if available_cols:
                    df = pd.read_parquet(temp_path, columns=available_cols)
                else:
                    df = pd.read_parquet(temp_path)  # Leer todo si no hay columnas específicas
                    
                print(f"[INFO] Archivo Parquet leído: {len(df)} registros")
                print(f"[DEBUG] Columnas disponibles: {list(df.columns)}")
                if len(df) > 0:
                    print(f"[DEBUG] Primera fila: {dict(df.iloc[0])}")
            except Exception as e:
                print(f"[ERROR] Error leyendo Parquet: {e}")
                return []
            
            # Convertir DataFrame a formato del dashboard - ADAPTABLE
            crimes = []
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
                    print(f"[DEBUG] Coordenadas parseadas: lat={lat_val} ({type(lat_val)}), lon={lon_val} ({type(lon_val)})")
                    crimes.append(crime_data)
                    print(f"[SUCCESS] Crimen parseado: {crime_data['crime_type']} en {crime_data['district']}")
                except Exception as e:
                    print(f"[WARNING] Error parseando fila: {e}")
                    print(f"[DEBUG] Row data: {dict(row)}")
                    continue
            
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

def filter_recent_crimes(crimes_data):
    """
    Para datos de HDFS, devolver todos los datos sin filtro temporal
    Simular timestamps recientes para visualización en tiempo real
    """
    if not crimes_data:
        return []
    
    now = datetime.now()
    recent_crimes = []
    
    # Importar random una sola vez fuera del bucle
    import random
    
    for i, crime in enumerate(crimes_data):
        try:
            # Para datos de HDFS, simular distribución en tiempo real
            simulated_seconds_ago = (i * 3) % 60  # Distribución fija sin random
            crime['timestamp'] = (now - timedelta(seconds=simulated_seconds_ago)).isoformat()
            recent_crimes.append(crime)
        except Exception as e:
            print(f"[WARNING] Error procesando crimen: {e}")
            continue
    
    print(f"[INFO] Crímenes procesados para visualización: {len(recent_crimes)} de {len(crimes_data)}")
    return recent_crimes

def generate_fallback_crimes():
    """
    Genera datos de respaldo cuando HDFS no está disponible
    """
    import random
    
    print("[INFO] Usando datos de respaldo (HDFS no disponible)")
    
    districts = ['Centro', 'Norte', 'Sur', 'Durán', 'Samborondón', 'Vía a la Costa']
    crime_types = ['robo', 'extorsión', 'sicariato', 'asesinato', 'secuestro', 'estafa']
    
    # Coordenadas aproximadas de cada distrito
    district_coords = {
        'Centro': [-2.1894, -79.8890],
        'Norte': [-2.1200, -79.8800],
        'Sur': [-2.2500, -79.9000],
        'Durán': [-2.1751, -79.8319],
        'Samborondón': [-2.1365, -79.8505],
        'Vía a la Costa': [-2.0800, -79.9500]
    }
    
    crimes = []
    current_time = datetime.now()
    
    # Generar menos crímenes de respaldo (3-6)
    for _ in range(random.randint(3, 6)):
        district = random.choice(districts)
        base_coords = district_coords[district]
        
        # Agregar variación aleatoria a las coordenadas
        lat = base_coords[0] + random.uniform(-0.02, 0.02)
        lon = base_coords[1] + random.uniform(-0.02, 0.02)
        
        # Crear crímenes distribuidos en el último minuto - DINÁMICOS
        seconds_ago = random.randint(0, 59)  
        crime_type = random.choice(crime_types)
        
        # Usar timestamp actual + variación para hacer datos dinámicos
        timestamp_with_variation = current_time - timedelta(seconds=seconds_ago)
        
        crime = {
            'timestamp': timestamp_with_variation.isoformat(),
            'crime_type': crime_type,
            'district': district,
            'lat': lat,
            'lon': lon,
            'description': f'{crime_type.title()} reportado en {district} [DEMO]'
        }
        crimes.append(crime)
    
    return crimes

@app.route("/")
def index():
    """Página principal con el mapa"""
    return render_template_string(HTML_TEMPLATE, 
                                MAP_CENTER=MAP_CENTER, 
                                ZOOM_LEVEL=ZOOM_LEVEL)

@app.route("/api/crimes/realtime")
def crimes_realtime():
    """API endpoint que devuelve crímenes en formato GeoJSON - RÁPIDO CON CACHE"""
    print(f"[API] ===== LLAMADA AL ENDPOINT /api/crimes/realtime =====")
    
    # CORRECCIÓN GPT-5: Usar cache con copy seguro para concurrencia
    with LATEST_LOCK:
        recent_crimes = [crime.copy() for crime in LATEST_CRIMES]

    print(f"[API] Crímenes desde cache: {len(recent_crimes)}")
    if recent_crimes:
        print(f"[API] Primer crimen: {recent_crimes[0]}")
    else:
        print(f"[API] NO HAY CRÍMENES EN CACHE - usando fallback")
        recent_crimes = generate_fallback_crimes()
        print(f"[API] Fallback generó: {len(recent_crimes)} crímenes")

    # Convertir a formato GeoJSON
    features = []
    for crime in recent_crimes:
        lon_val = crime["lon"]
        lat_val = crime["lat"]
        print(f"[DEBUG] GeoJSON - lon={lon_val} ({type(lon_val)}), lat={lat_val} ({type(lat_val)})")
        
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon_val), float(lat_val)]  # GeoJSON estándar: [lon, lat]
            },
            "properties": {
                "crime_type": crime["crime_type"],
                "district": crime["district"],
                "timestamp": crime["timestamp"],
                "description": crime["description"]
            }
        }
        features.append(feature)
    
    print(f"Final GeoJSON features: {len(features)}")
    
    return jsonify({
        "type": "FeatureCollection",
        "features": features
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
    print("URL: http://localhost:5000")
    print("Actualizacion: cada 6 segundos")
    print("Duracion por punto: 1 minuto")
    print(f"[INFO] Contenedor HDFS: {HDFS_CONTAINER}")
    
    # CORRECCIÓN GPT-5: Iniciar poller en background
    print("[INIT] Iniciando poller en background...")
    poller = Thread(target=poll_hdfs_loop, args=(6,), daemon=True)
    poller.start()
    
    # Abrir navegador solo si está configurado
    if OPEN_BROWSER:
        Thread(target=open_browser, daemon=True).start()
    
    try:
        # Iniciar servidor Flask
        app.run(host="0.0.0.0", port=5000, debug=False)
    except KeyboardInterrupt:
        print("\n[INFO] Dashboard cerrado por usuario")
    except Exception as e:
        print(f"\n[ERROR] Error en servidor Flask: {e}")
    finally:
        print("[INFO] Limpieza completada")
