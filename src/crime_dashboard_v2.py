#!/usr/bin/env python3
"""
Real-Time Crime Dashboard V2 - Following Project Requirements
Sistema de Análisis en Tiempo Real de Denuncias de Delitos en Guayaquil y Samborondón

Requirements:
- Interactive map of Guayaquil & Samborondón
- Real-time crime points from HDFS (processed by Spark every 6 seconds)
- Crime points visible for 1 minute duration
- Dynamic visualization with automatic updates
- 25 crimes every 3 seconds: Simulator → Kafka → Spark → HDFS → Dashboard
"""

import os
import json
import requests
import tempfile
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, render_template_string, jsonify
import pandas as pd
import pyarrow.parquet as pq
import io

# Flask app
app = Flask(__name__)

# Global variables for real-time crime tracking
active_crimes = []  # Crimes currently visible on map
crimes_lock = threading.Lock()

# Configuration
WEBHDFS_URL = "http://localhost:9870"
HDFS_BASE_PATH = "/crime-data/processed"
UPDATE_INTERVAL = 6  # seconds (Spark processing interval)
CRIME_DURATION = 60  # seconds (1 minute visibility per crime)
MAX_CRIMES_PER_UPDATE = 25  # crimes per update cycle

def log(message):
    """Simple logging with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_recent_hdfs_files():
    """Get most recent Parquet files from HDFS for today"""
    # Use UTC date since Spark writes in UTC
    from datetime import timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # Get all districts dynamically from HDFS instead of hardcoding
    try:
        list_url = f"{WEBHDFS_URL}/webhdfs/v1{HDFS_BASE_PATH}/date_partition={today}?op=LISTSTATUS"
        response = requests.get(list_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            directories = data.get("FileStatuses", {}).get("FileStatus", [])
            districts = [d["pathSuffix"].replace("district=", "") for d in directories if d["pathSuffix"].startswith("district=")]
            log(f"Found districts in HDFS: {districts}")
        else:
            # Fallback to known districts
            districts = ["Centro", "Norte", "Sur"]
            log(f"Using fallback districts: {districts}")
    except Exception as e:
        log(f"Error getting districts from HDFS: {e}")
        districts = ["Centro", "Norte", "Sur"]
    
    recent_files = []
    
    for district in districts:
        try:
            hdfs_path = f"{HDFS_BASE_PATH}/date_partition={today}/district={district}"
            list_url = f"{WEBHDFS_URL}/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
            
            response = requests.get(list_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                files = data.get("FileStatuses", {}).get("FileStatus", [])
                
                # Get Parquet files sorted by modification time
                parquet_files = [f for f in files if f["pathSuffix"].endswith(".parquet")]
                parquet_files.sort(key=lambda x: x["modificationTime"], reverse=True)
                
                # Get the 1 most recent file per district to ensure all districts represented
                for file_info in parquet_files[:1]:
                    file_path = f"{hdfs_path}/{file_info['pathSuffix']}"
                    recent_files.append({
                        "path": file_path,
                        "district": district,
                        "modified": file_info["modificationTime"]
                    })
                    
        except Exception as e:
            log(f"Error listing files for {district}: {e}")
            continue
    
    # Return all files (1 per district) to ensure all districts are represented
    recent_files.sort(key=lambda x: x["modified"], reverse=True)
    return recent_files  # Return all district files

def download_parquet_from_hdfs(file_path):
    """Download and parse Parquet file from HDFS with redirect handling"""
    try:
        download_url = f"{WEBHDFS_URL}/webhdfs/v1{file_path}?op=OPEN"
        
        # Handle WebHDFS redirect to DataNode
        response = requests.get(download_url, timeout=30, allow_redirects=False)
        
        if response.status_code == 307 and 'Location' in response.headers:
            redirect_url = response.headers['Location']
            # Fix DataNode hostname for localhost access
            if 'datanode1:9864' in redirect_url:
                redirect_url = redirect_url.replace('datanode1:9864', 'localhost:9864')
            response = requests.get(redirect_url, timeout=30)
        
        if response.status_code == 200:
            # Parse Parquet data
            parquet_data = io.BytesIO(response.content)
            table = pq.read_table(parquet_data)
            df = table.to_pandas()
            
            crimes = []
            for _, row in df.iterrows():
                try:
                    crime_data = json.loads(row['crime_data'])
                    
                    # Extract crime information
                    lat = float(crime_data["location"]["coordinates"]["lat"])
                    lon = float(crime_data["location"]["coordinates"]["lon"])
                    district = crime_data["location"].get("district", "unknown")
                    
                    crime = {
                        "id": crime_data.get("crime_id", f"crime_{int(time.time())}"),
                        "timestamp": crime_data.get("timestamp", datetime.now().isoformat()),
                        "lat": lat,
                        "lon": lon,
                        "type": crime_data.get("crime_type", "unknown"),
                        "district": district,
                        "added_at": time.time()  # When added to our system
                    }
                    
                    # Debug logging for coordinate validation
                    if district in ["Vía a la Costa", "Centro", "Durán"]:
                        log(f"DEBUG: {district} crime at ({lat}, {lon})")
                    
                    crimes.append(crime)
                    
                except Exception as e:
                    log(f"Error parsing crime record: {e}")
                    continue
            
            log(f"Parsed {len(crimes)} crimes from {file_path}")
            return crimes
            
        else:
            log(f"Failed to download {file_path}: HTTP {response.status_code}")
            return []
            
    except Exception as e:
        log(f"Error downloading {file_path}: {e}")
        return []

def update_crimes_cache():
    """Background thread to update crimes cache from HDFS"""
    log("Starting real-time crime updater...")
    
    while True:
        try:
            current_time = time.time()
            
            # Remove expired crimes (older than 1 minute)
            with crimes_lock:
                global active_crimes
                initial_count = len(active_crimes)
                active_crimes = [
                    crime for crime in active_crimes 
                    if current_time - crime["added_at"] < CRIME_DURATION
                ]
                expired_count = initial_count - len(active_crimes)
                if expired_count > 0:
                    log(f"Removed {expired_count} expired crimes")
            
            # Get new crimes from HDFS
            log("Fetching new crimes from HDFS...")
            recent_files = get_recent_hdfs_files()
            
            new_crimes = []
            for file_info in recent_files:
                crimes_from_file = download_parquet_from_hdfs(file_info["path"])
                new_crimes.extend(crimes_from_file)
                
                # Limit to prevent overwhelming the map
                if len(new_crimes) >= MAX_CRIMES_PER_UPDATE:
                    break
            
            # Add new crimes to active list
            if new_crimes:
                with crimes_lock:
                    # Avoid duplicates by checking crime IDs
                    existing_ids = {crime["id"] for crime in active_crimes}
                    unique_new_crimes = [
                        crime for crime in new_crimes 
                        if crime["id"] not in existing_ids
                    ]
                    
                    active_crimes.extend(unique_new_crimes[:MAX_CRIMES_PER_UPDATE])
                    
                log(f"Added {len(unique_new_crimes)} new crimes. Total active: {len(active_crimes)}")
            
        except Exception as e:
            log(f"Error in crime updater: {e}")
        
        # Wait for next update cycle
        time.sleep(UPDATE_INTERVAL)

# HTML Template for the dashboard
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Crímenes en Tiempo Real - Guayaquil & Samborondón</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <style>
        body { margin: 0; font-family: Arial, sans-serif; }
        #map { height: 100vh; width: 100%; }
        .info-panel {
            position: absolute;
            top: 10px;
            right: 10px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.3);
            z-index: 1000;
            min-width: 250px;
        }
        .crime-counter {
            font-size: 24px;
            font-weight: bold;
            color: #d32f2f;
            margin-bottom: 10px;
        }
        .update-info {
            font-size: 12px;
            color: #666;
        }
        .legend {
            margin-top: 15px;
            border-top: 1px solid #eee;
            padding-top: 10px;
        }
        .legend-item {
            display: flex;
            align-items: center;
            margin: 5px 0;
            font-size: 12px;
        }
        .legend-color {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
    </style>
</head>
<body>
    <div id="map"></div>
    
    <div class="info-panel">
        <h3>CRÍMENES EN TIEMPO REAL</h3>
        <div style="font-size: 14px; margin-bottom: 10px;">Guayaquil & Samborondón</div>
        
        <div class="crime-counter">
            Crímenes activos: <span id="crime-count">0</span>
        </div>
        
        <div class="update-info">
            Actualización: cada 6 segundos<br>
            Duración: 1 minuto por punto
        </div>
        
        <div class="legend">
            <div style="font-weight: bold; margin-bottom: 8px;">Tipos de Crimen</div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #ff4444;"></div>
                Robo
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #ff8800;"></div>
                Extorsión
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #8800ff;"></div>
                Sicariato
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #ff0000;"></div>
                Asesinato
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #0088ff;"></div>
                Secuestro
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #00ff88;"></div>
                Estafa
            </div>
        </div>
    </div>

    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <script>
        // Initialize map centered on Guayaquil
        const map = L.map('map').setView([-2.1709, -79.9224], 11);
        
        // Add OpenStreetMap tiles
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(map);
        
        // Crime markers storage
        let crimeMarkers = new Map();
        
        // Crime type colors
        const crimeColors = {
            'robo': '#ff4444',
            'extorsion': '#ff8800', 
            'sicariato': '#8800ff',
            'asesinato': '#ff0000',
            'secuestro': '#0088ff',
            'estafa': '#00ff88'
        };
        
        function getColorForCrime(crimeType) {
            return crimeColors[crimeType.toLowerCase()] || '#666666';
        }
        
        function createCrimeMarker(crime) {
            const color = getColorForCrime(crime.type);
            
            const marker = L.circleMarker([crime.lat, crime.lon], {
                radius: 8,
                fillColor: color,
                color: '#fff',
                weight: 2,
                opacity: 1,
                fillOpacity: 0.8
            });
            
            // Popup with crime details
            const popupContent = `
                <strong>${crime.type.toUpperCase()}</strong><br>
                Distrito: ${crime.district}<br>
                ID: ${crime.id}<br>
                Hora: ${new Date(crime.timestamp).toLocaleTimeString()}
            `;
            marker.bindPopup(popupContent);
            
            return marker;
        }
        
        function updateCrimes() {
            fetch('/api/crimes/realtime')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('API Error:', data.error);
                        return;
                    }
                    
                    const currentCrimes = new Set();
                    
                    // Add/update crime markers
                    data.crimes.forEach(crime => {
                        currentCrimes.add(crime.id);
                        
                        if (!crimeMarkers.has(crime.id)) {
                            const marker = createCrimeMarker(crime);
                            marker.addTo(map);
                            crimeMarkers.set(crime.id, marker);
                        }
                    });
                    
                    // Remove expired crime markers
                    for (const [crimeId, marker] of crimeMarkers.entries()) {
                        if (!currentCrimes.has(crimeId)) {
                            map.removeLayer(marker);
                            crimeMarkers.delete(crimeId);
                        }
                    }
                    
                    // Update counter
                    document.getElementById('crime-count').textContent = data.crimes.length;
                })
                .catch(error => {
                    console.error('Error fetching crimes:', error);
                });
        }
        
        // Start real-time updates
        updateCrimes();
        setInterval(updateCrimes, 6000); // Update every 6 seconds
        
        console.log('Real-time crime dashboard initialized');
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/crimes/realtime')
def get_realtime_crimes():
    """API endpoint to get current active crimes"""
    try:
        with crimes_lock:
            current_crimes = active_crimes.copy()
        
        # Format crimes for frontend
        crimes_data = []
        for crime in current_crimes:
            crimes_data.append({
                "id": crime["id"],
                "lat": crime["lat"],
                "lon": crime["lon"],
                "type": crime["type"],
                "district": crime["district"],
                "timestamp": crime["timestamp"]
            })
        
        return jsonify({
            "crimes": crimes_data,
            "count": len(crimes_data),
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        log(f"API Error: {e}")
        return jsonify({
            "error": str(e),
            "crimes": [],
            "count": 0
        })

if __name__ == '__main__':
    log("=== REAL-TIME CRIME DASHBOARD V2 ===")
    log("Sistema de Análisis en Tiempo Real de Denuncias de Delitos")
    log("Guayaquil & Samborondón")
    log("")
    log("Configuration:")
    log(f"- Update interval: {UPDATE_INTERVAL} seconds")
    log(f"- Crime duration: {CRIME_DURATION} seconds")
    log(f"- Max crimes per update: {MAX_CRIMES_PER_UPDATE}")
    log(f"- HDFS URL: {WEBHDFS_URL}")
    log("")
    
    # Start background crime updater
    updater_thread = threading.Thread(target=update_crimes_cache, daemon=True)
    updater_thread.start()
    log("Background crime updater started")
    
    # Start Flask server
    log("Starting dashboard server...")
    log("Dashboard URL: http://localhost:5000")
    
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
