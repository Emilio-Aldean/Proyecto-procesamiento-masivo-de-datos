#!/usr/bin/env python3
"""
Crime Density Analysis Dashboard - Advanced Metrics
Sistema de Análisis de Densidad Criminal para Guayaquil y Samborondón

This dashboard reads crime density metrics from HDFS and displays comprehensive analysis.
Reads from: hdfs://namenode:9000/crime-data/density-analysis
Port: 5002
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

# Global variables for density data tracking
density_data = []  # Current density analysis data
density_lock = threading.Lock()

# Configuration
WEBHDFS_URL = "http://localhost:9870"
HDFS_DENSITY_PATH = "/crime-data/density-analysis"
UPDATE_INTERVAL = 15  # seconds (check for new density data every 15 seconds)

def log(message):
    """Simple logging with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_recent_density_files():
    """Get most recent density analysis files from HDFS for today"""
    from datetime import timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    try:
        list_url = f"{WEBHDFS_URL}/webhdfs/v1{HDFS_DENSITY_PATH}/date_partition={today}?op=LISTSTATUS"
        response = requests.get(list_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            files = data.get("FileStatuses", {}).get("FileStatus", [])
            
            recent_files = []
            for file_info in files:
                if file_info["pathSuffix"].endswith(".parquet"):
                    file_path = f"{HDFS_DENSITY_PATH}/date_partition={today}/{file_info['pathSuffix']}"
                    recent_files.append({
                        "path": file_path,
                        "modified": file_info["modificationTime"]
                    })
            
            recent_files.sort(key=lambda x: x["modified"], reverse=True)
            return recent_files[:10]  # Return 10 most recent density files for comprehensive analysis
            
    except Exception as e:
        log(f"Error listing density files: {e}")
        return []

def download_density_parquet_from_hdfs(file_path):
    """Download and parse density Parquet file from HDFS"""
    try:
        download_url = f"{WEBHDFS_URL}/webhdfs/v1{file_path}?op=OPEN"
        
        response = requests.get(download_url, timeout=30, allow_redirects=False)
        
        if response.status_code == 307 and 'Location' in response.headers:
            redirect_url = response.headers['Location']
            if 'datanode1:9864' in redirect_url:
                redirect_url = redirect_url.replace('datanode1:9864', 'localhost:9864')
            
            data_response = requests.get(redirect_url, timeout=30)
            if data_response.status_code == 200:
                parquet_data = data_response.content
            else:
                log(f"Failed to download from redirect URL: HTTP {data_response.status_code}")
                return []
        elif response.status_code == 200:
            parquet_data = response.content
        else:
            log(f"Failed to download {file_path}: HTTP {response.status_code}")
            return []
        
        if parquet_data:
            # Use BytesIO to avoid Windows temp file permission issues
            import io
            parquet_buffer = io.BytesIO(parquet_data)
            df = pd.read_parquet(parquet_buffer)
            
            # Debug: Log data structure
            log(f"DEBUG: Density data columns: {list(df.columns)}")
            log(f"DEBUG: Density data shape: {df.shape}")
            if not df.empty:
                log(f"DEBUG: First row sample: {df.iloc[0].to_dict()}")
            
            density_records = []
            
            for _, row in df.iterrows():
                try:
                    # Parse crime_types_list if it exists and convert to regular list
                    crime_types_list = row.get('crime_types_list', [])
                    if isinstance(crime_types_list, str):
                        try:
                            crime_types_list = json.loads(crime_types_list)
                        except:
                            crime_types_list = [crime_types_list]
                    else:
                        # Convert numpy array to regular Python list and remove duplicates
                        crime_types_list = list(crime_types_list) if hasattr(crime_types_list, 'tolist') else list(crime_types_list)
                    
                    # Remove duplicates while preserving order and get unique crime types
                    unique_crime_types = list(dict.fromkeys(crime_types_list))  # Preserves order, removes duplicates
                    
                    record = {
                        "district": row.get('district', 'Unknown'),
                        "total_crimes": int(row.get('total_crimes', 0)),
                        "crime_type_variety": len(unique_crime_types),  # Use actual unique count
                        "crime_density_score": float(row.get('crime_density_score', 0)),
                        "crime_types_list": unique_crime_types,  # Use unique crime types only
                        "last_updated": row.get('last_updated', datetime.now().isoformat()),
                        "date_partition": row.get('date_partition', datetime.now().strftime("%Y-%m-%d"))
                    }
                    density_records.append(record)
                except Exception as e:
                    log(f"Error parsing density record: {e}")
                    continue
            
            log(f"Parsed {len(density_records)} density records from {file_path}")
            return density_records
        else:
            log(f"No data received from {file_path}")
            return []
            
    except Exception as e:
        log(f"Error downloading {file_path}: {e}")
        return []

def update_density_cache():
    """Background thread to update density cache from HDFS"""
    log("Starting crime density analyzer...")
    
    while True:
        try:
            # Fetch new density data from HDFS
            log("Fetching density analysis from HDFS...")
            recent_files = get_recent_density_files()
            
            if recent_files:
                log(f"Found {len(recent_files)} recent density files")
                
                all_new_data = []
                for file_info in recent_files:
                    new_data = download_density_parquet_from_hdfs(file_info["path"])
                    all_new_data.extend(new_data)
                
                # Update density data (replace with latest)
                with density_lock:
                    global density_data
                    density_data = all_new_data
                    
                log(f"Updated density data: {len(density_data)} districts analyzed")
                
                # Debug: Log current density data summary
                if density_data:
                    total_crimes = sum(d["total_crimes"] for d in density_data)
                    log(f"DEBUG: Total crimes across all districts: {total_crimes}")
                    for d in density_data:
                        log(f"DEBUG: {d['district']}: {d['total_crimes']} crimes, density {d['crime_density_score']}")
                else:
                    log("DEBUG: No density data available")
            else:
                log("No recent density files found")
            
        except Exception as e:
            log(f"Error in density updater: {e}")
        
        # Wait for next update cycle
        time.sleep(UPDATE_INTERVAL)

@app.route('/')
def index():
    """Main density dashboard page"""
    return render_template_string(DENSITY_DASHBOARD_HTML)

@app.route('/api/density/analysis')
def get_density_analysis():
    """API endpoint to get current density analysis"""
    try:
        with density_lock:
            current_data = density_data.copy()
        
        # Sort by density score (highest first)
        current_data.sort(key=lambda x: x["crime_density_score"], reverse=True)
        
        # Calculate summary statistics
        total_crimes = sum(d["total_crimes"] for d in current_data)
        avg_density = sum(d["crime_density_score"] for d in current_data) / len(current_data) if current_data else 0
        max_density = max((d["crime_density_score"] for d in current_data), default=0)
        
        # Find hotspot (highest density district)
        hotspot = current_data[0] if current_data else None
        
        return jsonify({
            "districts": current_data,
            "summary": {
                "total_crimes": total_crimes,
                "avg_density": round(avg_density, 2),
                "max_density": round(max_density, 2),
                "districts_analyzed": len(current_data),
                "hotspot": hotspot["district"] if hotspot else "N/A"
            },
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        log(f"API Error: {e}")
        return jsonify({
            "error": str(e),
            "districts": [],
            "summary": {
                "total_crimes": 0,
                "avg_density": 0,
                "max_density": 0,
                "districts_analyzed": 0,
                "hotspot": "N/A"
            }
        })

# HTML Template for Density Dashboard
DENSITY_DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Análisis de Densidad Criminal - Guayaquil</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            min-height: 100vh;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
            max-width: 1200px;
            margin-left: auto;
            margin-right: auto;
        }
        
        .stat-card {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            text-align: center;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .stat-number {
            font-size: 2.5em;
            font-weight: bold;
            margin-bottom: 10px;
        }
        
        .stat-label {
            font-size: 1em;
            opacity: 0.8;
        }
        
        .charts-container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            max-width: 1400px;
            margin: 0 auto 30px auto;
        }
        
        .chart-card {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .chart-title {
            font-size: 1.3em;
            font-weight: bold;
            margin-bottom: 20px;
            text-align: center;
        }
        
        .districts-table {
            max-width: 1200px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .table-title {
            font-size: 1.5em;
            font-weight: bold;
            margin-bottom: 20px;
            text-align: center;
        }
        
        .district-row {
            display: grid;
            grid-template-columns: 2fr 1fr 1fr 1fr 3fr;
            gap: 15px;
            padding: 15px;
            margin-bottom: 10px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 10px;
            align-items: center;
        }
        
        .district-header {
            background: rgba(255, 255, 255, 0.2);
            font-weight: bold;
        }
        
        .district-name {
            font-weight: bold;
            font-size: 1.1em;
        }
        
        .density-bar {
            height: 20px;
            background: linear-gradient(90deg, #ff6b6b 0%, #feca57 50%, #48dbfb 100%);
            border-radius: 10px;
            position: relative;
            overflow: hidden;
        }
        
        .density-fill {
            height: 100%;
            background: rgba(255, 255, 255, 0.3);
            border-radius: 10px;
            transition: width 0.5s ease;
        }
        
        .crime-types {
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
        }
        
        .crime-type-tag {
            background: rgba(255, 255, 255, 0.2);
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 0.8em;
        }
        
        .loading {
            text-align: center;
            padding: 50px;
            font-size: 1.2em;
        }
        
        .update-info {
            text-align: center;
            margin-top: 30px;
            font-size: 0.9em;
            opacity: 0.7;
        }
        
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }
        
        .pulse {
            animation: pulse 2s infinite;
        }
        
        @media (max-width: 768px) {
            .charts-container {
                grid-template-columns: 1fr;
            }
            
            .district-row {
                grid-template-columns: 1fr;
                gap: 10px;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 ANÁLISIS DE DENSIDAD CRIMINAL</h1>
        <p>Distribución y Concentración de Delitos - Guayaquil & Samborondón</p>
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-number" id="total-crimes">0</div>
            <div class="stat-label">Total Crímenes</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="avg-density">0</div>
            <div class="stat-label">Densidad Promedio</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="max-density">0</div>
            <div class="stat-label">Densidad Máxima</div>
        </div>
        <div class="stat-card">
            <div class="stat-number" id="hotspot">-</div>
            <div class="stat-label">Zona Crítica</div>
        </div>
    </div>
    
    <div class="charts-container">
        <div class="chart-card">
            <div class="chart-title">Densidad por Distrito</div>
            <canvas id="densityChart"></canvas>
        </div>
        <div class="chart-card">
            <div class="chart-title">Variedad de Crímenes</div>
            <canvas id="varietyChart"></canvas>
        </div>
    </div>
    
    <div class="districts-table">
        <div class="table-title">Análisis Detallado por Distrito</div>
        <div id="districts-list" class="loading">
            <div class="pulse">🔄 Analizando densidad criminal...</div>
        </div>
    </div>
    
    <div class="update-info">
        Actualización automática cada 15 segundos • Análisis basado en datos en tiempo real
    </div>
    
    <script>
        let densityChart, varietyChart;
        
        function initCharts() {
            // Density Chart
            const densityCtx = document.getElementById('densityChart').getContext('2d');
            densityChart = new Chart(densityCtx, {
                type: 'bar',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Puntuación de Densidad',
                        data: [],
                        backgroundColor: 'rgba(255, 107, 107, 0.8)',
                        borderColor: 'rgba(255, 107, 107, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: {
                            labels: { color: 'white' }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: { color: 'white' },
                            grid: { color: 'rgba(255, 255, 255, 0.1)' }
                        },
                        x: {
                            ticks: { color: 'white' },
                            grid: { color: 'rgba(255, 255, 255, 0.1)' }
                        }
                    }
                }
            });
            
            // Variety Chart
            const varietyCtx = document.getElementById('varietyChart').getContext('2d');
            varietyChart = new Chart(varietyCtx, {
                type: 'doughnut',
                data: {
                    labels: [],
                    datasets: [{
                        data: [],
                        backgroundColor: [
                            '#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#feca57', '#ff9ff3'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: {
                            labels: { color: 'white' }
                        }
                    }
                }
            });
        }
        
        function updateDensityAnalysis() {
            fetch('/api/density/analysis')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('API Error:', data.error);
                        return;
                    }
                    
                    // Update summary stats
                    document.getElementById('total-crimes').textContent = data.summary.total_crimes;
                    document.getElementById('avg-density').textContent = data.summary.avg_density;
                    document.getElementById('max-density').textContent = data.summary.max_density;
                    document.getElementById('hotspot').textContent = data.summary.hotspot;
                    
                    // Update charts
                    updateCharts(data.districts);
                    
                    // Update districts table
                    updateDistrictsTable(data.districts);
                })
                .catch(error => {
                    console.error('Error fetching density data:', error);
                    document.getElementById('districts-list').innerHTML = `
                        <div class="loading">
                            ❌ Error conectando al sistema de análisis<br>
                            <small>Reintentando...</small>
                        </div>
                    `;
                });
        }
        
        function updateCharts(districts) {
            // Update density chart
            densityChart.data.labels = districts.map(d => d.district);
            densityChart.data.datasets[0].data = districts.map(d => d.crime_density_score);
            densityChart.update();
            
            // Update variety chart
            varietyChart.data.labels = districts.map(d => d.district);
            varietyChart.data.datasets[0].data = districts.map(d => d.crime_type_variety);
            varietyChart.update();
        }
        
        function updateDistrictsTable(districts) {
            const districtsList = document.getElementById('districts-list');
            
            if (districts.length === 0) {
                districtsList.innerHTML = `
                    <div class="loading">
                        📊 No hay datos de densidad disponibles<br>
                        <small>Esperando análisis...</small>
                    </div>
                `;
                return;
            }
            
            // Create header
            let tableHTML = `
                <div class="district-row district-header">
                    <div>Distrito</div>
                    <div>Crímenes</div>
                    <div>Variedad</div>
                    <div>Densidad</div>
                    <div>Tipos de Crimen</div>
                </div>
            `;
            
            // Find max density for bar scaling
            const maxDensity = Math.max(...districts.map(d => d.crime_density_score));
            
            // Add district rows
            districts.forEach(district => {
                const densityPercent = maxDensity > 0 ? (district.crime_density_score / maxDensity) * 100 : 0;
                
                const crimeTypesHTML = district.crime_types_list.map(type => 
                    `<span class="crime-type-tag">${type}</span>`
                ).join('');
                
                tableHTML += `
                    <div class="district-row">
                        <div class="district-name">${district.district}</div>
                        <div>${district.total_crimes}</div>
                        <div>${district.crime_type_variety}/6</div>
                        <div>
                            <div class="density-bar">
                                <div class="density-fill" style="width: ${densityPercent}%"></div>
                            </div>
                            <small>${district.crime_density_score}</small>
                        </div>
                        <div class="crime-types">${crimeTypesHTML}</div>
                    </div>
                `;
            });
            
            districtsList.innerHTML = tableHTML;
        }
        
        // Initialize
        initCharts();
        updateDensityAnalysis();
        setInterval(updateDensityAnalysis, 15000); // Update every 15 seconds
        
        console.log('Crime density analysis dashboard initialized');
    </script>
</body>
</html>
'''

if __name__ == '__main__':
    log("=== CRIME DENSITY ANALYSIS DASHBOARD ===")
    log("Sistema de Análisis de Densidad Criminal")
    log("Guayaquil & Samborondón")
    log("")
    log("Configuration:")
    log(f"- Update interval: {UPDATE_INTERVAL} seconds")
    log(f"- HDFS URL: {WEBHDFS_URL}")
    log(f"- Density path: {HDFS_DENSITY_PATH}")
    log("")
    
    # Start background density updater
    updater_thread = threading.Thread(target=update_density_cache, daemon=True)
    updater_thread.start()
    log("Background density analyzer started")
    
    # Start Flask server
    log("Starting density dashboard server...")
    log("Density Dashboard URL: http://localhost:5002")
    
    app.run(host='0.0.0.0', port=5002, debug=False, threaded=True)
