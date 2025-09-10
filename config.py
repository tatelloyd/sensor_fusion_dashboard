#!/usr/bin/env python3
"""
Configuration file for Raspberry Pi Sensor Dashboard
"""

import os
from datetime import timedelta

class Config:
    # Flask configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'sensor_dashboard_secret_key_change_in_production'
    
    # Database configuration
    DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'sensor_data.db')
    
    # Sensor configuration
    SENSOR_CONFIG = {
        'DHT22': {
            'sensors': {
                'dht22_1': {'pin': 17, 'name': 'Living Room'},
                'dht22_2': {'pin': 27, 'name': 'Bedroom'},
                'dht22_3': {'pin': 22, 'name': 'Kitchen'}
            },
            'read_interval': 2.0,  # seconds
            'retry_attempts': 3
        },
        'MQ135': {
            'adc_channel': 0,
            'vref': 3.3,
            'read_interval': 1.0,
            'calibration_factor': 1.0,
            'name': 'Air Quality'
        },
        'DFR0026': {
            'adc_channel': 1,
            'vref': 3.3,
            'read_interval': 0.5,
            'smoothing_window': 10,
            'name': 'Sound Level'
        }
    }
    
    # MCP3008 ADC configuration
    MCP3008_CONFIG = {
        'spi_bus': 0,
        'spi_device': 0,
        'max_speed_hz': 1000000,
        'spi_mode': 0
    }
    
    # Data fusion configuration
    FUSION_CONFIG = {
        'update_interval': 5.0,  # seconds
        'history_window': 100,   # number of readings to keep in memory
        'anomaly_threshold': {
            'temperature_std': 5.0,
            'humidity_std': 15.0,
            'sudden_change_threshold': 10.0
        },
        'comfort_ranges': {
            'temperature': {'min': 18, 'max': 26, 'optimal': 22},
            'humidity': {'min': 30, 'max': 70, 'optimal': 50},
            'air_quality': {'good': 30, 'moderate': 60, 'poor': 80},
            'sound_level': {'quiet': 20, 'moderate': 50, 'loud': 70}
        }
    }
    
    # Web interface configuration
    WEB_CONFIG = {
        'host': '0.0.0.0',
        'port': 5000,
        'debug': False,
        'chart_history_points': 50,
        'auto_refresh_interval': 2000  # milliseconds
    }
    
    # Logging configuration
    LOGGING_CONFIG = {
        'level': 'INFO',
        'file': os.path.join(os.path.dirname(__file__), 'logs', 'sensor_dashboard.log'),
        'max_file_size': 10 * 1024 * 1024,  # 10MB
        'backup_count': 3,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    }
    
    # Database retention policy
    DATA_RETENTION = {
        'raw_data_days': 30,
        'aggregated_data_days': 365,
        'cleanup_interval_hours': 24
    }

class DevelopmentConfig(Config):
    DEBUG = True
    WEB_CONFIG = Config.WEB_CONFIG.copy()
    WEB_CONFIG['debug'] = True

class ProductionConfig(Config):
    DEBUG = False
    # Add production-specific settings here
    pass

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
