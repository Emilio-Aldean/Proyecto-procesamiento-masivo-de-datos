#!/usr/bin/env python3
"""
Real-Time Security Alerts Dashboard - Advanced Metrics
Sistema de Alertas de Seguridad en Tiempo Real para Guayaquil y Samborondón

This dashboard reads critical crime alerts from HDFS and displays them in real-time.
Reads from: hdfs://namenode:9000/crime-data/real-time-alerts
Port: 5001
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

# Global variables for alerts tracking
active_alerts = []  # Current active alerts
alerts_lock = threading.Lock()

# Configuration
WEBHDFS_URL = "http://localhost:9870"
HDFS_ALERTS_PATH = "/crime-data/real-time-alerts"
UPDATE_INTERVAL = 10  # seconds (check for new alerts every 10 seconds)
ALERT_DURATION = 300  # seconds (5 minutes visibility per alert)

def log(message):
    """Simple logging with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_recent_alert_files():
    """Get most recent alert files from HDFS for today"""
    from datetime import timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    try:
        list_url = f"{WEBHDFS_URL}/webhdfs/v1{HDFS_ALERTS_PATH}/date_partition={today}?op=LISTSTATUS"
        response = requests.get(list_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            directories = data.get("FileStatuses", {}).get("FileStatus", [])
            
            recent_files = []
            for dir_info in directories:
                if dir_info["type"] == "DIRECTORY":
                    district = dir_info["pathSuffix"]
                    district_path = f"{HDFS_ALERTS_PATH}/date_partition={today}/{district}"
                    
                    try:
                        files_url = f"{WEBHDFS_URL}/webhdfs/v1{district_path}?op=LISTSTATUS"
                        files_response = requests.get(files_url, timeout=10)
                        if files_response.status_code == 200:
                            files_data = files_response.json()
                            files = files_data.get("FileStatuses", {}).get("FileStatus", [])
                            
                            for file_info in files:
                                if file_info["pathSuffix"].endswith(".parquet"):
                                    file_path = f"{district_path}/{file_info['pathSuffix']}"
                                    recent_files.append({
                                        "path": file_path,
                                        "district": district,
                                        "modified": file_info["modificationTime"]
                                    })
                    except Exception as e:
                        log(f"Error listing files for {district}: {e}")
                        continue
            
            recent_files.sort(key=lambda x: x["modified"], reverse=True)
            return recent_files[:10]  # Return 10 most recent alert files
            
    except Exception as e:
        log(f"Error listing alert directories: {e}")
        return []

def download_alert_parquet_from_hdfs(file_path):
    """Download and parse alert Parquet file from HDFS"""
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
            with tempfile.NamedTemporaryFile() as temp_file:
                temp_file.write(parquet_data)
                temp_file.flush()
                
                df = pd.read_parquet(temp_file.name)
                alerts = []
                
                for _, row in df.iterrows():
                    try:
                        alert = {
                            "id": f"ALERT-{row.get('district', 'UNK')}-{int(time.time() * 1000)}",
                            "district": row.get('district', 'Unknown'),
                            "crime_type": row.get('crime_type', 'Unknown'),
                            "crime_count": int(row.get('crime_count', 0)),
                            "alert_level": row.get('alert_level', 'MEDIUM'),
                            "alert_message": row.get('alert_message', 'Security alert detected'),
                            "alert_time": row.get('alert_time', datetime.now().isoformat()),
                            "is_critical": bool(row.get('is_critical', False)),
                            "added_at": time.time()
                        }
                        alerts.append(alert)
                    except Exception as e:
                        log(f"Error parsing alert record: {e}")
                        continue
                
                log(f"Parsed {len(alerts)} alerts from {file_path}")
                return alerts
        else:
            log(f"No data received from {file_path}")
            return []
            
    except Exception as e:
        log(f"Error downloading {file_path}: {e}")
        return []

def update_alerts_cache():
    """Background thread to update alerts cache from HDFS"""
    log("Starting real-time alerts updater...")
    
    while True:
        try:
            current_time = time.time()
            
            # Remove expired alerts (older than 5 minutes)
            with alerts_lock:
                global active_alerts
                initial_count = len(active_alerts)
                active_alerts = [
                    alert for alert in active_alerts 
                    if current_time - alert["added_at"] < ALERT_DURATION
                ]
                expired_count = initial_count - len(active_alerts)
                if expired_count > 0:
                    log(f"Removed {expired_count} expired alerts")
            
            # Fetch new alerts from HDFS
            log("Fetching new alerts from HDFS...")
            recent_files = get_recent_alert_files()
            
            if recent_files:
                log(f"Found {len(recent_files)} recent alert files")
                
                all_new_alerts = []
                for file_info in recent_files:
                    new_alerts = download_alert_parquet_from_hdfs(file_info["path"])
                    all_new_alerts.extend(new_alerts)
                
                # Add unique new alerts
                with alerts_lock:
                    existing_ids = {alert["id"] for alert in active_alerts}
                    unique_new_alerts = [
                        alert for alert in all_new_alerts 
                        if alert["id"] not in existing_ids
                    ]
                    
                    active_alerts.extend(unique_new_alerts)
                    
                log(f"Added {len(unique_new_alerts)} new alerts. Total active: {len(active_alerts)}")
            else:
                log("No recent alert files found")
            
        except Exception as e:
            log(f"Error in alerts updater: {e}")
        
        # Wait for next update cycle
        time.sleep(UPDATE_INTERVAL)

@app.route('/')
def index():
    """Main alerts dashboard page"""
    return render_template_string(ALERTS_DASHBOARD_HTML)

@app.route('/api/alerts/realtime')
def get_realtime_alerts():
    """API endpoint to get current active alerts"""
    try:
        with alerts_lock:
            current_alerts = active_alerts.copy()
        
        # Sort alerts by severity and time
        current_alerts.sort(key=lambda x: (
            0 if x["is_critical"] else 1,  # Critical alerts first
            -x["added_at"]  # Then by newest first
        ))
        
        # Format alerts for frontend
        alerts_data = []
        for alert in current_alerts:
            alerts_data.append({
                "id": alert["id"],
                "district": alert["district"],
                "crime_type": alert["crime_type"],
                "crime_count": alert["crime_count"],
                "alert_level": alert["alert_level"],
                "alert_message": alert["alert_message"],
                "alert_time": alert["alert_time"],
                "is_critical": alert["is_critical"],
                "time_ago": int(time.time() - alert["added_at"])
            })
        
        return jsonify({
            "alerts": alerts_data,
            "count": len(alerts_data),
            "critical_count": len([a for a in alerts_data if a["is_critical"]]),
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        log(f"API Error: {e}")
        return jsonify({
            "error": str(e),
            "alerts": [],
            "count": 0,
            "critical_count": 0
        })

# HTML Template for Alerts Dashboard
ALERTS_DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sistema de Alertas de Seguridad - Guayaquil</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
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
        
        .stats-panel {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 30px;
            flex-wrap: wrap;
        }
        
        .stat-card {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            text-align: center;
            min-width: 200px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .stat-number {
            font-size: 3em;
            font-weight: bold;
            margin-bottom: 10px;
        }
        
        .stat-label {
            font-size: 1.1em;
            opacity: 0.8;
        }
        
        .critical { color: #ff4757; }
        .total { color: #3742fa; }
        
        .alerts-container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .alert-card {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            margin-bottom: 15px;
            border-left: 5px solid;
            transition: transform 0.2s ease;
        }
        
        .alert-card:hover {
            transform: translateX(5px);
        }
        
        .alert-critical {
            border-left-color: #ff4757;
            background: rgba(255, 71, 87, 0.1);
        }
        
        .alert-normal {
            border-left-color: #ffa502;
            background: rgba(255, 165, 2, 0.1);
        }
        
        .alert-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        
        .alert-title {
            font-size: 1.3em;
            font-weight: bold;
        }
        
        .alert-badge {
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }
        
        .badge-critical {
            background: #ff4757;
            color: white;
        }
        
        .badge-normal {
            background: #ffa502;
            color: white;
        }
        
        .alert-message {
            font-size: 1.1em;
            margin-bottom: 10px;
            line-height: 1.4;
        }
        
        .alert-details {
            display: flex;
            gap: 20px;
            font-size: 0.9em;
            opacity: 0.8;
        }
        
        .no-alerts {
            text-align: center;
            padding: 50px;
            font-size: 1.2em;
            opacity: 0.7;
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
    </style>
</head>
<body>
    <div class="header">
        <h1>🚨 SISTEMA DE ALERTAS DE SEGURIDAD</h1>
        <p>Monitoreo en Tiempo Real - Guayaquil & Samborondón</p>
    </div>
    
    <div class="stats-panel">
        <div class="stat-card">
            <div class="stat-number critical" id="critical-count">0</div>
            <div class="stat-label">Alertas Críticas</div>
        </div>
        <div class="stat-card">
            <div class="stat-number total" id="total-count">0</div>
            <div class="stat-label">Total Alertas</div>
        </div>
    </div>
    
    <div class="alerts-container">
        <div id="alerts-list" class="loading">
            <div class="pulse">🔄 Cargando alertas de seguridad...</div>
        </div>
    </div>
    
    <div class="update-info">
        Actualización automática cada 10 segundos • Alertas visibles por 5 minutos
    </div>
    
    <script>
        function updateAlerts() {
            fetch('/api/alerts/realtime')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        console.error('API Error:', data.error);
                        return;
                    }
                    
                    // Update counters
                    document.getElementById('critical-count').textContent = data.critical_count;
                    document.getElementById('total-count').textContent = data.count;
                    
                    const alertsList = document.getElementById('alerts-list');
                    
                    if (data.alerts.length === 0) {
                        alertsList.innerHTML = `
                            <div class="no-alerts">
                                ✅ No hay alertas de seguridad activas<br>
                                <small>Sistema monitoreando normalmente</small>
                            </div>
                        `;
                        return;
                    }
                    
                    // Build alerts HTML
                    let alertsHTML = '';
                    data.alerts.forEach(alert => {
                        const alertClass = alert.is_critical ? 'alert-critical' : 'alert-normal';
                        const badgeClass = alert.is_critical ? 'badge-critical' : 'badge-normal';
                        const badgeText = alert.is_critical ? 'CRÍTICA' : 'ALERTA';
                        
                        const timeAgo = formatTimeAgo(alert.time_ago);
                        const alertTime = new Date(alert.alert_time).toLocaleTimeString();
                        
                        alertsHTML += `
                            <div class="alert-card ${alertClass}">
                                <div class="alert-header">
                                    <div class="alert-title">${alert.district.toUpperCase()}</div>
                                    <div class="alert-badge ${badgeClass}">${badgeText}</div>
                                </div>
                                <div class="alert-message">${alert.alert_message}</div>
                                <div class="alert-details">
                                    <span>🕒 ${timeAgo}</span>
                                    <span>📍 ${alert.district}</span>
                                    <span>⚠️ ${alert.crime_type.toUpperCase()}</span>
                                    <span>📊 ${alert.crime_count} casos</span>
                                </div>
                            </div>
                        `;
                    });
                    
                    alertsList.innerHTML = alertsHTML;
                })
                .catch(error => {
                    console.error('Error fetching alerts:', error);
                    document.getElementById('alerts-list').innerHTML = `
                        <div class="no-alerts">
                            ❌ Error conectando al sistema de alertas<br>
                            <small>Reintentando...</small>
                        </div>
                    `;
                });
        }
        
        function formatTimeAgo(seconds) {
            if (seconds < 60) return `${seconds}s`;
            if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
            return `${Math.floor(seconds / 3600)}h`;
        }
        
        // Start real-time updates
        updateAlerts();
        setInterval(updateAlerts, 10000); // Update every 10 seconds
        
        console.log('Real-time security alerts dashboard initialized');
    </script>
</body>
</html>
'''

if __name__ == '__main__':
    log("=== REAL-TIME SECURITY ALERTS DASHBOARD ===")
    log("Sistema de Alertas de Seguridad en Tiempo Real")
    log("Guayaquil & Samborondón")
    log("")
    log("Configuration:")
    log(f"- Update interval: {UPDATE_INTERVAL} seconds")
    log(f"- Alert duration: {ALERT_DURATION} seconds")
    log(f"- HDFS URL: {WEBHDFS_URL}")
    log(f"- Alerts path: {HDFS_ALERTS_PATH}")
    log("")
    
    # Start background alerts updater
    updater_thread = threading.Thread(target=update_alerts_cache, daemon=True)
    updater_thread.start()
    log("Background alerts updater started")
    
    # Start Flask server
    log("Starting alerts dashboard server...")
    log("Alerts Dashboard URL: http://localhost:5001")
    
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)
