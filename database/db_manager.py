#!/usr/bin/env python3
"""
Database Manager for Raspberry Pi Sensor Dashboard
database/db_manager.py
"""

import sqlite3
import json
import logging
import threading
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Tuple
import os


class DatabaseManager:
    """
    Centralized database manager for sensor data storage and retrieval
    Handles both raw sensor data and fusion analytics
    """
    
    def __init__(self, config):
        """
        Initialize database manager
        
        Args:
            config: Configuration object containing database settings
        """
        self.config = config
        self.db_path = config.DATABASE_PATH
        self.logger = logging.getLogger(__name__)
        self.lock = threading.Lock()
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Initialize database
        self.initialize_database()
        
        # Start cleanup thread if configured
        if hasattr(config, 'DATA_RETENTION'):
            self._start_cleanup_thread()
    
    def initialize_database(self):
        """Create database tables if they don't exist"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create sensor_readings table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_readings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        sensor_type TEXT NOT NULL,
                        sensor_id TEXT NOT NULL,
                        temperature REAL,
                        humidity REAL,
                        adc_value INTEGER,
                        voltage REAL,
                        processed_value REAL,
                        extra_data TEXT,
                        status TEXT DEFAULT 'valid',
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create fusion_data table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fusion_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        comfort_index REAL,
                        environment_status TEXT,
                        anomaly_detected BOOLEAN DEFAULT 0,
                        avg_temperature REAL,
                        avg_humidity REAL,
                        correlation_temp_humidity REAL,
                        correlation_data TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create sensor_status table for tracking sensor health
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_status (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sensor_id TEXT NOT NULL,
                        sensor_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        last_reading_time DATETIME,
                        error_count INTEGER DEFAULT 0,
                        last_error TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(sensor_id, sensor_type)
                    )
                ''')
                
                # Create aggregated_data table for daily/hourly summaries
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS aggregated_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date_hour DATETIME NOT NULL,
                        sensor_type TEXT NOT NULL,
                        sensor_id TEXT NOT NULL,
                        avg_value REAL,
                        min_value REAL,
                        max_value REAL,
                        count_readings INTEGER,
                        data_type TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(date_hour, sensor_type, sensor_id, data_type)
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_sensor_readings_timestamp 
                    ON sensor_readings(timestamp)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor 
                    ON sensor_readings(sensor_type, sensor_id)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_fusion_data_timestamp 
                    ON fusion_data(timestamp)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_aggregated_data_date_hour 
                    ON aggregated_data(date_hour)
                ''')
                
                conn.commit()
                self.logger.info("Database initialized successfully")
                
        except Exception as e:
            self.logger.error(f"Database initialization error: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            with self.lock:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.row_factory = sqlite3.Row  # Enable column access by name
                yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def store_sensor_reading(self, sensor_type: str, sensor_id: str, 
                           temperature: Optional[float] = None,
                           humidity: Optional[float] = None,
                           adc_value: Optional[int] = None,
                           voltage: Optional[float] = None,
                           processed_value: Optional[float] = None,
                           extra_data: Optional[Dict] = None,
                           timestamp: Optional[datetime] = None) -> bool:
        """
        Store a sensor reading in the database
        
        Args:
            sensor_type: Type of sensor (DHT22, MQ135, DFR0026)
            sensor_id: Unique identifier for the sensor
            temperature: Temperature reading (for DHT22)
            humidity: Humidity reading (for DHT22)
            adc_value: Raw ADC value (for analog sensors)
            voltage: Voltage reading (for analog sensors)
            processed_value: Processed sensor value
            extra_data: Additional data as dictionary
            timestamp: Reading timestamp (default: now)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if timestamp is None:
                timestamp = datetime.now()
            
            extra_data_json = json.dumps(extra_data) if extra_data else None
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO sensor_readings 
                    (timestamp, sensor_type, sensor_id, temperature, humidity, 
                     adc_value, voltage, processed_value, extra_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (timestamp, sensor_type, sensor_id, temperature, humidity,
                      adc_value, voltage, processed_value, extra_data_json))
                
                conn.commit()
                
                # Update sensor status
                self._update_sensor_status(cursor, sensor_id, sensor_type, 'online', timestamp)
                conn.commit()
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error storing sensor reading: {e}")
            return False
    
    def store_fusion_data(self, comfort_index: float, environment_status: str,
                         anomaly_detected: bool, avg_temperature: float,
                         avg_humidity: float, correlation_temp_humidity: float,
                         additional_data: Optional[Dict] = None,
                         timestamp: Optional[datetime] = None) -> bool:
        """
        Store fusion analysis data
        
        Args:
            comfort_index: Calculated comfort index (0-100)
            environment_status: Overall environment status
            anomaly_detected: Whether anomaly was detected
            avg_temperature: Average temperature from all sensors
            avg_humidity: Average humidity from all sensors
            correlation_temp_humidity: Temperature-humidity correlation
            additional_data: Additional fusion data as dictionary
            timestamp: Analysis timestamp (default: now)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if timestamp is None:
                timestamp = datetime.now()
            
            correlation_data_json = json.dumps(additional_data) if additional_data else None
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO fusion_data 
                    (timestamp, comfort_index, environment_status, anomaly_detected,
                     avg_temperature, avg_humidity, correlation_temp_humidity, correlation_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (timestamp, comfort_index, environment_status, anomaly_detected,
                      avg_temperature, avg_humidity, correlation_temp_humidity, correlation_data_json))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error storing fusion data: {e}")
            return False
    
    def get_recent_readings(self, sensor_type: Optional[str] = None,
                          sensor_id: Optional[str] = None,
                          hours: int = 24, limit: int = 1000) -> List[Dict]:
        """
        Get recent sensor readings
        
        Args:
            sensor_type: Filter by sensor type
            sensor_id: Filter by sensor ID
            hours: Number of hours to look back
            limit: Maximum number of readings to return
            
        Returns:
            List of sensor readings as dictionaries
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = '''
                    SELECT * FROM sensor_readings 
                    WHERE timestamp > datetime('now', '-{} hours')
                '''.format(hours)
                
                params = []
                
                if sensor_type:
                    query += ' AND sensor_type = ?'
                    params.append(sensor_type)
                
                if sensor_id:
                    query += ' AND sensor_id = ?'
                    params.append(sensor_id)
                
                query += ' ORDER BY timestamp DESC LIMIT ?'
                params.append(limit)
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                readings = []
                for row in rows:
                    reading = dict(row)
                    if reading['extra_data']:
                        try:
                            reading['extra_data'] = json.loads(reading['extra_data'])
                        except:
                            pass
                    readings.append(reading)
                
                return readings
                
        except Exception as e:
            self.logger.error(f"Error getting recent readings: {e}")
            return []
    
    def get_fusion_data(self, hours: int = 24, limit: int = 1000) -> List[Dict]:
        """
        Get recent fusion analysis data
        
        Args:
            hours: Number of hours to look back
            limit: Maximum number of records to return
            
        Returns:
            List of fusion data records as dictionaries
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM fusion_data 
                    WHERE timestamp > datetime('now', '-{} hours')
                    ORDER BY timestamp DESC 
                    LIMIT ?
                '''.format(hours), (limit,))
                
                rows = cursor.fetchall()
                
                fusion_data = []
                for row in rows:
                    data = dict(row)
                    if data['correlation_data']:
                        try:
                            data['correlation_data'] = json.loads(data['correlation_data'])
                        except:
                            pass
                    fusion_data.append(data)
                
                return fusion_data
                
        except Exception as e:
            self.logger.error(f"Error getting fusion data: {e}")
            return []
    
    def get_sensor_statistics(self, sensor_type: str, sensor_id: str,
                            hours: int = 24) -> Dict[str, Any]:
        """
        Get statistical summary for a sensor
        
        Args:
            sensor_type: Type of sensor
            sensor_id: Sensor identifier
            hours: Number of hours to analyze
            
        Returns:
            Dictionary containing statistical summary
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                stats = {}
                
                # Temperature statistics (for DHT22)
                if sensor_type == 'DHT22':
                    cursor.execute('''
                        SELECT 
                            AVG(temperature) as avg_temp,
                            MIN(temperature) as min_temp,
                            MAX(temperature) as max_temp,
                            AVG(humidity) as avg_humidity,
                            MIN(humidity) as min_humidity,
                            MAX(humidity) as max_humidity,
                            COUNT(*) as reading_count
                        FROM sensor_readings 
                        WHERE sensor_type = ? AND sensor_id = ?
                        AND timestamp > datetime('now', '-{} hours')
                        AND temperature IS NOT NULL
                    '''.format(hours), (sensor_type, sensor_id))
                    
                    row = cursor.fetchone()
                    if row:
                        stats.update({
                            'temperature': {
                                'avg': round(row['avg_temp'], 1) if row['avg_temp'] else None,
                                'min': round(row['min_temp'], 1) if row['min_temp'] else None,
                                'max': round(row['max_temp'], 1) if row['max_temp'] else None
                            },
                            'humidity': {
                                'avg': round(row['avg_humidity'], 1) if row['avg_humidity'] else None,
                                'min': round(row['min_humidity'], 1) if row['min_humidity'] else None,
                                'max': round(row['max_humidity'], 1) if row['max_humidity'] else None
                            },
                            'reading_count': row['reading_count']
                        })
                
                # Analog sensor statistics (MQ135, DFR0026)
                else:
                    cursor.execute('''
                        SELECT 
                            AVG(processed_value) as avg_value,
                            MIN(processed_value) as min_value,
                            MAX(processed_value) as max_value,
                            AVG(voltage) as avg_voltage,
                            COUNT(*) as reading_count
                        FROM sensor_readings 
                        WHERE sensor_type = ? AND sensor_id = ?
                        AND timestamp > datetime('now', '-{} hours')
                        AND processed_value IS NOT NULL
                    '''.format(hours), (sensor_type, sensor_id))
                    
                    row = cursor.fetchone()
                    if row:
                        stats.update({
                            'processed_value': {
                                'avg': round(row['avg_value'], 1) if row['avg_value'] else None,
                                'min': round(row['min_value'], 1) if row['min_value'] else None,
                                'max': round(row['max_value'], 1) if row['max_value'] else None
                            },
                            'voltage': {
                                'avg': round(row['avg_voltage'], 3) if row['avg_voltage'] else None
                            },
                            'reading_count': row['reading_count']
                        })
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error getting sensor statistics: {e}")
            return {}
    
    def get_sensor_status(self) -> Dict[str, Any]:
        """
        Get current status of all sensors
        
        Returns:
            Dictionary containing sensor status information
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT sensor_id, sensor_type, status, last_reading_time,
                           error_count, last_error, updated_at
                    FROM sensor_status
                    ORDER BY sensor_type, sensor_id
                ''')
                
                rows = cursor.fetchall()
                
                status = {}
                for row in rows:
                    status[row['sensor_id']] = {
                        'sensor_type': row['sensor_type'],
                        'status': row['status'],
                        'last_reading_time': row['last_reading_time'],
                        'error_count': row['error_count'],
                        'last_error': row['last_error'],
                        'updated_at': row['updated_at']
                    }
                
                return status
                
        except Exception as e:
            self.logger.error(f"Error getting sensor status: {e}")
            return {}
    
    def _update_sensor_status(self, cursor, sensor_id: str, sensor_type: str,
                            status: str, timestamp: datetime,
                            error_message: Optional[str] = None):
        """Update sensor status in database"""
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO sensor_status 
                (sensor_id, sensor_type, status, last_reading_time, 
                 error_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, 
                        CASE WHEN ? = 'error' THEN COALESCE((SELECT error_count FROM sensor_status WHERE sensor_id = ? AND sensor_type = ?), 0) + 1 
                             ELSE 0 END,
                        ?, ?)
            ''', (sensor_id, sensor_type, status, timestamp,
                  status, sensor_id, sensor_type, error_message, datetime.now()))
            
        except Exception as e:
            self.logger.error(f"Error updating sensor status: {e}")
    
    def record_sensor_error(self, sensor_id: str, sensor_type: str,
                          error_message: str) -> bool:
        """
        Record a sensor error
        
        Args:
            sensor_id: Sensor identifier
            sensor_type: Type of sensor
            error_message: Error description
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                self._update_sensor_status(cursor, sensor_id, sensor_type, 
                                         'error', datetime.now(), error_message)
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error recording sensor error: {e}")
            return False
    
    def cleanup_old_data(self):
        """Remove old data based on retention policy"""
        if not hasattr(self.config, 'DATA_RETENTION'):
            return
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Clean up old raw sensor data
                raw_data_cutoff = datetime.now() - timedelta(days=self.config.DATA_RETENTION['raw_data_days'])
                cursor.execute('''
                    DELETE FROM sensor_readings 
                    WHERE timestamp < ?
                ''', (raw_data_cutoff,))
                
                # Clean up old fusion data
                fusion_data_cutoff = datetime.now() - timedelta(days=self.config.DATA_RETENTION['aggregated_data_days'])
                cursor.execute('''
                    DELETE FROM fusion_data 
                    WHERE timestamp < ?
                ''', (fusion_data_cutoff,))
                
                conn.commit()
                
                self.logger.info(f"Cleaned up data older than {self.config.DATA_RETENTION['raw_data_days']} days")
                
        except Exception as e:
            self.logger.error(f"Error during data cleanup: {e}")
    
    def _start_cleanup_thread(self):
        """Start background thread for periodic data cleanup"""
        def cleanup_worker():
            import time
            while True:
                try:
                    self.cleanup_old_data()
                    time.sleep(self.config.DATA_RETENTION['cleanup_interval_hours'] * 3600)
                except Exception as e:
                    self.logger.error(f"Cleanup thread error: {e}")
                    time.sleep(3600)  # Wait 1 hour on error
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        self.logger.info("Data cleanup thread started")
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get database information and statistics
        
        Returns:
            Dictionary containing database information
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                info = {
                    'database_path': self.db_path,
                    'tables': {}
                }
                
                # Get table information
                tables = ['sensor_readings', 'fusion_data', 'sensor_status', 'aggregated_data']
                
                for table in tables:
                    cursor.execute(f'SELECT COUNT(*) as count FROM {table}')
                    count = cursor.fetchone()['count']
                    
                    cursor.execute(f'SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest FROM {table}')
                    time_range = cursor.fetchone()
                    
                    info['tables'][table] = {
                        'record_count': count,
                        'oldest_record': time_range['oldest'],
                        'newest_record': time_range['newest']
                    }
                
                return info
                
        except Exception as e:
            self.logger.error(f"Error getting database info: {e}")
            return {'error': str(e)}
    
    def export_data(self, output_file: str, start_date: Optional[datetime] = None,
                   end_date: Optional[datetime] = None) -> bool:
        """
        Export sensor data to JSON file
        
        Args:
            output_file: Output file path
            start_date: Start date for export (optional)
            end_date: End date for export (optional)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = 'SELECT * FROM sensor_readings WHERE 1=1'
                params = []
                
                if start_date:
                    query += ' AND timestamp >= ?'
                    params.append(start_date)
                
                if end_date:
                    query += ' AND timestamp <= ?'
                    params.append(end_date)
                
                query += ' ORDER BY timestamp'
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                data = []
                for row in rows:
                    record = dict(row)
                    if record['extra_data']:
                        try:
                            record['extra_data'] = json.loads(record['extra_data'])
                        except:
                            pass
                    data.append(record)
                
                with open(output_file, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
                
                self.logger.info(f"Data exported to {output_file}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error exporting data: {e}")
            return False
    
    def close(self):
        """Close database connections and cleanup"""
        self.logger.info("Database manager shutting down")
        # SQLite connections are closed automatically by context manager
        # No persistent connections to close
