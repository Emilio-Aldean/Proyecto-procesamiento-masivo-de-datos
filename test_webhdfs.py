#!/usr/bin/env python3
"""
Simple WebHDFS test to verify HDFS connection and data reading
"""
import requests
import json
from datetime import datetime

def test_webhdfs_connection():
    """Test basic WebHDFS connection"""
    print("=== TESTING WEBHDFS CONNECTION ===")
    
    try:
        # Test basic HDFS connection
        response = requests.get('http://namenode:9870/webhdfs/v1/?op=LISTSTATUS', timeout=10)
        print(f"Basic HDFS connection: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ WebHDFS connection successful")
        else:
            print(f"❌ WebHDFS connection failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False
    
    return True

def test_crime_data_access():
    """Test access to crime data partitions"""
    print("\n=== TESTING CRIME DATA ACCESS ===")
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    print(f"Looking for date partition: {current_date}")
    
    # Test main crime data directory
    try:
        response = requests.get('http://namenode:9870/webhdfs/v1/crime-data/processed?op=LISTSTATUS', timeout=10)
        if response.status_code == 200:
            data = response.json()
            partitions = data.get('FileStatuses', {}).get('FileStatus', [])
            print(f"Found {len(partitions)} date partitions")
            
            # Look for today's partition
            today_partition = None
            for partition in partitions:
                if partition['pathSuffix'] == f'date_partition={current_date}':
                    today_partition = partition
                    break
            
            if today_partition:
                print(f"✅ Found today's partition: {today_partition['pathSuffix']}")
                return test_district_data(current_date)
            else:
                print(f"❌ No partition found for {current_date}")
                print("Available partitions:")
                for p in partitions:
                    print(f"  - {p['pathSuffix']}")
                return False
        else:
            print(f"❌ Error accessing crime data: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_district_data(date_partition):
    """Test access to district data"""
    print(f"\n=== TESTING DISTRICT DATA FOR {date_partition} ===")
    
    districts = ['Centro', 'Norte', 'Sur', 'Durán', 'Samborondón', 'Vía a la Costa']
    
    for district in districts:
        try:
            hdfs_path = f"/crime-data/processed/date_partition={date_partition}/district={district}"
            webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
            
            response = requests.get(webhdfs_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                files = data.get('FileStatuses', {}).get('FileStatus', [])
                parquet_files = [f for f in files if f['pathSuffix'].endswith('.parquet')]
                
                print(f"✅ {district}: {len(parquet_files)} Parquet files")
                
                if parquet_files:
                    # Show latest file info
                    latest = max(parquet_files, key=lambda x: x['modificationTime'])
                    mod_time = datetime.fromtimestamp(latest['modificationTime'] / 1000)
                    print(f"   Latest: {latest['pathSuffix'][:50]}... ({mod_time})")
                    
            else:
                print(f"❌ {district}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"❌ {district}: Error {e}")
    
    return True

def test_parquet_download():
    """Test downloading and reading a Parquet file"""
    print("\n=== TESTING PARQUET FILE DOWNLOAD ===")
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    hdfs_path = f"/crime-data/processed/date_partition={current_date}/district=Centro"
    
    try:
        # Get file list
        webhdfs_url = f"http://namenode:9870/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
        response = requests.get(webhdfs_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            files = data.get('FileStatuses', {}).get('FileStatus', [])
            parquet_files = [f for f in files if f['pathSuffix'].endswith('.parquet')]
            
            if parquet_files:
                # Try to download the first file
                test_file = parquet_files[0]['pathSuffix']
                file_path = f"{hdfs_path}/{test_file}"
                
                print(f"Attempting to download: {test_file}")
                
                download_url = f"http://namenode:9870/webhdfs/v1{file_path}?op=OPEN"
                download_response = requests.get(download_url, timeout=30)
                
                if download_response.status_code == 200:
                    print(f"✅ Successfully downloaded {len(download_response.content)} bytes")
                    
                    # Try to parse with pyarrow
                    try:
                        import pyarrow.parquet as pq
                        import io
                        
                        parquet_data = io.BytesIO(download_response.content)
                        table = pq.read_table(parquet_data)
                        df = table.to_pandas()
                        
                        print(f"✅ Parsed Parquet: {len(df)} records")
                        print(f"Columns: {list(df.columns)}")
                        
                        if len(df) > 0:
                            print("Sample record:")
                            print(df.iloc[0].to_dict())
                            
                        return True
                        
                    except Exception as e:
                        print(f"❌ Error parsing Parquet: {e}")
                        return False
                        
                else:
                    print(f"❌ Download failed: HTTP {download_response.status_code}")
                    return False
            else:
                print("❌ No Parquet files found")
                return False
        else:
            print(f"❌ Error listing files: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🔍 WEBHDFS INTEGRATION TEST")
    print("=" * 50)
    
    # Run tests
    if test_webhdfs_connection():
        if test_crime_data_access():
            test_parquet_download()
    
    print("\n" + "=" * 50)
    print("Test completed!")
