#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crime Data Simulator for Guayaquil and Samborondón - Docker Version

Este script genera datos simulados de denuncias de delitos en Guayaquil y Samborondón
adaptado para ejecutarse en contenedores Docker con Kafka.
"""

import json
import time
import random
import hashlib
import datetime
import os
import sys
import logging
from typing import Dict, List, Tuple, Any
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/crime_simulator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Inicialización de Faker para datos en español
fake = Faker(['es_ES'])

# Configuraciones para la simulación
CONFIG = {
    "batch_size": 25,  # Denuncias por lote
    "batch_interval": 3,  # Segundos entre lotes
    "crime_types": [
        "robo", "extorsión", "sicariato", "asesinato", "secuestro", "estafa"
    ],
    # Probabilidades para cada tipo de delito (6 tipos según especificación)
    "crime_probabilities": [0.35, 0.25, 0.15, 0.10, 0.10, 0.05]
}

# Zonas geográficas de Guayaquil y Samborondón - COORDENADAS ANTERIORES (FUNCIONABAN MEJOR)
DISTRICTS = {
    "Centro": {
        # Centro histórico de Guayaquil - reducido ligeramente hacia la derecha para evitar agua
        "lat_range": (-2.175, -2.140),
        "lon_range": (-79.930, -79.890),
        "weight": 25
    },
    "Norte": {
        # Zona norte de Guayaquil - expandido más a la izquierda para alcanzar Monte Sinaí
        "lat_range": (-2.140, -2.080),
        "lon_range": (-80.000, -79.870),
        "weight": 20
    },
    "Sur": {
        # Zona sur de Guayaquil - reducido ligeramente a la derecha para evitar agua
        "lat_range": (-2.250, -2.175),
        "lon_range": (-79.950, -79.890),
        "weight": 20
    },
    "Samborondón": {
        # Cantón Samborondón - ajustado un poco a la derecha (corrección del exceso izquierdo)
        "lat_range": (-1.970, -1.950),
        "lon_range": (-79.740, -79.720),
        "weight": 15
    },
    "Durán": {
        # Cantón Durán - expandido ligeramente en todas las direcciones
        "lat_range": (-2.185, -2.155),
        "lon_range": (-79.845, -79.805),
        "weight": 15
    },
    "Vía a la Costa": {
        # Zona oeste hacia la costa - expandido un poco más a la izquierda
        "lat_range": (-2.210, -2.170),
        "lon_range": (-80.070, -79.950),
        "weight": 10
    }
}

def create_kafka_producer():
    """Crear productor de Kafka con configuración para Docker"""
    kafka_servers = "localhost:9092"
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka_servers],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        logger.info(f"Productor Kafka creado exitosamente: {kafka_servers}")
        return producer
    except Exception as e:
        logger.error(f"Error creando productor Kafka: {str(e)}")
        return None

def generate_anonymous_name() -> str:
    """Genera un nombre anonimizado"""
    return fake.name()

def generate_hashed_id() -> str:
    """Genera un hash a partir de una cédula simulada de Ecuador"""
    cedula = fake.random_number(digits=10, fix_len=True)
    return hashlib.sha256(str(cedula).encode()).hexdigest()[:16]

def get_crime_type() -> str:
    """Selecciona un tipo de delito basado en las probabilidades configuradas"""
    return random.choices(
        CONFIG["crime_types"], 
        weights=CONFIG["crime_probabilities"]
    )[0]

def get_location() -> Dict[str, Any]:
    """Genera coordenadas geográficas realistas dentro de Guayaquil y Samborondón"""
    # Seleccionar distrito basado en peso
    districts = list(DISTRICTS.keys())
    weights = [DISTRICTS[d]["weight"] for d in districts]
    selected_district = random.choices(districts, weights=weights)[0]
    
    district_info = DISTRICTS[selected_district]
    
    # Generar coordenadas dentro del rango del distrito
    lat = random.uniform(*district_info["lat_range"])
    lon = random.uniform(*district_info["lon_range"])
    
    # Generar dirección ficticia
    address = f"{fake.street_name()} {fake.building_number()}, {selected_district}"
    
    return {
        "address": address,
        "district": selected_district,
        "coordinates": {
            "lat": round(lat, 6),
            "lon": round(lon, 6)
        }
    }

def generate_timestamp() -> str:
    """Genera un timestamp en formato ISO 8601"""
    # Distribución horaria más realista (más crímenes en la noche)
    hour_weights = [2, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6, 5, 6, 7, 8, 9, 10, 8, 6, 4]
    hour = random.choices(range(24), weights=hour_weights)[0]
    
    now = datetime.datetime.now()
    crime_time = now.replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999)
    )
    
    return crime_time.isoformat()

def generate_crime_details(crime_type: str) -> str:
    """Genera detalles adicionales basados en el tipo de delito"""
    details_templates = {
        "robo": [
            "Sustracción de pertenencias personales mediante intimidación",
            "Hurto de objetos de valor en la vía pública",
            "Robo de celular y billetera en transporte público"
        ],
        "extorsión": [
            "Amenazas para obtener dinero mediante chantaje",
            "Cobro de cupos a comerciantes locales",
            "Intimidación para extorsión telefónica"
        ],
        "sicariato": [
            "Atentado con arma de fuego por encargo",
            "Asesinato selectivo en vía pública",
            "Ejecución planificada por terceros"
        ],
        "asesinato": [
            "Homicidio con arma blanca en riña",
            "Muerte violenta durante asalto",
            "Asesinato por venganza personal"
        ],
        "secuestro": [
            "Privación ilegal de libertad por rescate",
            "Retención forzada de persona",
            "Secuestro express para extorsión"
        ],
        "estafa": [
            "Fraude mediante engaño telefónico",
            "Estafa con tarjetas de crédito clonadas",
            "Engaño para obtener dinero fraudulentamente"
        ]
    }
    
    if crime_type in details_templates:
        return random.choice(details_templates[crime_type])
    else:
        return f"Incidente relacionado con {crime_type.lower()}"

def generate_crime_record() -> Dict[str, Any]:
    """Genera un registro completo de denuncia de delito"""
    crime_type = get_crime_type()
    
    return {
        "crime_id": f"GYE-{fake.random_number(digits=8, fix_len=True)}",
        "timestamp": generate_timestamp(),
        "location": get_location(),
        "crime_type": crime_type,
        "description": generate_crime_details(crime_type),
        "reporter_info": {
            "name": generate_anonymous_name(),
            "cedula_hash": generate_hashed_id(),  # Campo agregado
            "phone": fake.phone_number()
        }
    }

def generate_batch(batch_size: int = CONFIG["batch_size"]) -> List[Dict[str, Any]]:
    """Genera un lote de denuncias del tamaño especificado"""
    return [generate_crime_record() for _ in range(batch_size)]

def simulate_crime_data_docker():
    """Simula la generación continua de datos de denuncias criminales para Docker"""
    logger.info("=== INICIANDO SIMULADOR DE DATOS DE CRIMINALIDAD - DOCKER ===")
    
    # Crear productor Kafka
    producer = create_kafka_producer()
    if not producer:
        logger.error("No se pudo crear el productor Kafka. Terminando...")
        sys.exit(1)
    
    topic_name = "raw-crimes"
    batch_count = 0
    total_records = 0
    
    logger.info(f"Configuración:")
    logger.info(f"  - Tamaño de lote: {CONFIG['batch_size']} denuncias")
    logger.info(f"  - Intervalo: {CONFIG['batch_interval']} segundos")
    logger.info(f"  - Tópico Kafka: {topic_name}")
    
    try:
        while True:
            batch_count += 1
            logger.info(f"Generando lote #{batch_count}...")
            
            # Generar lote de datos
            batch = generate_batch()
            
            # Enviar cada registro a Kafka
            for record in batch:
                try:
                    future = producer.send(
                        topic_name,
                        key=record["crime_id"],
                        value=record
                    )
                    # Esperar confirmación
                    future.get(timeout=10)
                    total_records += 1
                    
                except KafkaError as e:
                    logger.error(f"Error enviando mensaje a Kafka: {str(e)}")
                except Exception as e:
                    logger.error(f"Error inesperado: {str(e)}")
            
            # Flush para asegurar envío
            producer.flush()
            
            logger.info(f"Lote #{batch_count} enviado exitosamente")
            logger.info(f"Total de registros enviados: {total_records}")
            
            # Esperar antes del siguiente lote
            time.sleep(CONFIG["batch_interval"])
            
    except KeyboardInterrupt:
        logger.info("Deteniendo simulador...")
    except Exception as e:
        logger.error(f"Error en el simulador: {str(e)}")
    finally:
        if producer:
            producer.close()
        logger.info(f"Simulador detenido. Total de registros enviados: {total_records}")

def print_sample_record():
    """Imprime un registro de muestra para verificación"""
    sample = generate_crime_record()
    logger.info("=== REGISTRO DE MUESTRA ===")
    logger.info(json.dumps(sample, indent=2, ensure_ascii=False))
    return sample

if __name__ == "__main__":
    # Verificar si estamos en modo de muestra
    if len(sys.argv) > 1 and sys.argv[1] == "--sample":
        print_sample_record()
    else:
        # Ejecutar simulador continuo
        simulate_crime_data_docker()
