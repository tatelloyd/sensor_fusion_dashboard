#!/usr/bin/env python3
"""
DHT22 Sensor Reader Module
sensors/dht22_reader.py
"""

import time
import board
import adafruit_dht
import logging
from datetime import datetime

class DHT22Reader:
    def __init__(self, config):
        """
        Initialize DHT22 sensor reader
        
        Args:
            config: DHT22 configuration dictionary
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.sensors = {}
        self.retry_attempts = config.get('retry_attempts', 3)
        
        # Initialize sensors based on configuration
        self._initialize_sensors()
    
    def _initialize_sensors(self):
        """Initialize DHT22 sensor instances"""
        try:
            for sensor_id, sensor_config in self.config['sensors'].items():
                pin = sensor_config['pin']
                
                # Convert pin number to board pin
                board_pin = getattr(board, f'D{pin}')
                
                # Create DHT22 instance
                self.sensors[sensor_id] = {
                    'device': adafruit_dht.DHT22(board_pin),
                    'name': sensor_config.get('name', sensor_id),
                    'pin': pin,
                    'last_reading': None,
                    'error_count': 0
                }
                
                self.logger.info(f"DHT22 {sensor_id} initialized on GPIO{pin}")
                
        except Exception as e:
            self.logger.error(f"Error initializing DHT22 sensors: {e}")
            raise
    
    def read_sensor(self, sensor_id):
        """
        Read a specific DHT22 sensor
        
        Args:
            sensor_id: Sensor identifier
            
        Returns:
            dict: Reading result with success flag and data
        """
        if sensor_id not in self.sensors:
            return {
                'success': False,
                'error': f'Sensor {sensor_id} not found',
                'sensor_id': sensor_id
            }
        
        sensor = self.sensors[sensor_id]
        device = sensor['device']
        
        for attempt in range(self.retry_attempts):
            try:
                temperature = device.temperature
                humidity = device.humidity
                
                if temperature is not None and humidity is not None:
                    # Reset error count on successful read
                    sensor['error_count'] = 0
                    
                    reading = {
                        'success': True,
                        'sensor_id': sensor_id,
                        'sensor_name': sensor['name'],
                        'temperature': round(temperature, 1),
                        'humidity': round(humidity, 1),
                        'timestamp': datetime.now().isoformat(),
                        'pin': sensor['pin'],
                        'attempt': attempt + 1
                    }
                    
                    sensor['last_reading'] = reading
                    return reading
                
                else:
                    # One of the values is None, try again
                    if attempt < self.retry_attempts - 1:
                        time.sleep(0.5)  # Wait before retry
                        continue
                    
            except RuntimeError as e:
                error_msg = str(e)
                self.logger.warning(f"DHT22 {sensor_id} attempt {attempt + 1}: {error_msg}")
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(0.5)  # Wait before retry
                    continue
                
                # All attempts failed
                sensor['error_count'] += 1
                return {
                    'success': False,
                    'error': error_msg,
                    'sensor_id': sensor_id,
                    'sensor_name': sensor['name'],
                    'error_count': sensor['error_count'],
                    'timestamp': datetime.now().isoformat()
                }
                
            except Exception as e:
                self.logger.error(f"Unexpected error reading DHT22 {sensor_id}: {e}")
                sensor['error_count'] += 1
                return {
                    'success': False,
                    'error': f'Unexpected error: {str(e)}',
                    'sensor_id': sensor_id,
                    'sensor_name': sensor['name'],
                    'error_count': sensor['error_count'],
                    'timestamp': datetime.now().isoformat()
                }
        
        # If we get here, all attempts failed
        sensor['error_count'] += 1
        return {
            'success': False,
            'error': 'All retry attempts failed',
            'sensor_id': sensor_id,
            'sensor_name': sensor['name'],
            'error_count': sensor['error_count'],
            'timestamp': datetime.now().isoformat()
        }
    
    def read_all(self):
        """
        Read all configured DHT22 sensors
        
        Returns:
            dict: Dictionary of sensor readings
        """
        readings = {}
        
        for sensor_id in self.sensors.keys():
            readings[sensor_id] = self.read_sensor(sensor_id)
            
            # Small delay between sensor reads to prevent interference
            time.sleep(0.1)
        
        return readings
    
    def get_sensor_info(self):
        """
        Get information about all configured sensors
        
        Returns:
            dict: Sensor information
        """
        info = {}
        
        for sensor_id, sensor in self.sensors.items():
            info[sensor_id] = {
                'name': sensor['name'],
                'pin': sensor['pin'],
                'error_count': sensor['error_count'],
                'last_reading_time': sensor['last_reading'].get('timestamp') if sensor['last_reading'] else None,
                'status': 'online' if sensor['error_count'] < 5 else 'error'
            }
        
        return info
    
    def reset_error_counts(self):
        """Reset error counts for all sensors"""
        for sensor in self.sensors.values():
            sensor['error_count'] = 0
        self.logger.info("DHT22 error counts reset")
    
    def get_last_readings(self):
        """
        Get last successful readings from all sensors
        
        Returns:
            dict: Last readings for each sensor
        """
        readings = {}
        
        for sensor_id, sensor in self.sensors.items():
            if sensor['last_reading'] and sensor['last_reading']['success']:
                readings[sensor_id] = sensor['last_reading']
        
        return readings
    
    def calculate_averages(self):
        """
        Calculate average temperature and humidity from all sensors
        
        Returns:
            dict: Average values or None if no valid readings
        """
        last_readings = self.get_last_readings()
        
        if not last_readings:
            return None
        
        temperatures = [reading['temperature'] for reading in last_readings.values()]
        humidities = [reading['humidity'] for reading in last_readings.values()]
        
        if not temperatures or not humidities:
            return None
        
        return {
            'avg_temperature': round(sum(temperatures) / len(temperatures), 1),
            'avg_humidity': round(sum(humidities) / len(humidities), 1),
            'sensor_count': len(last_readings),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_health_status(self):
        """
        Get overall health status of DHT22 sensors
        
        Returns:
            dict: Health status information
        """
        total_sensors = len(self.sensors)
        online_sensors = sum(1 for sensor in self.sensors.values() if sensor['error_count'] < 5)
        error_sensors = total_sensors - online_sensors
        
        return {
            'total_sensors': total_sensors,
            'online_sensors': online_sensors,
            'error_sensors': error_sensors,
            'health_percentage': round((online_sensors / total_sensors) * 100, 1) if total_sensors > 0 else 0,
            'status': 'healthy' if error_sensors == 0 else 'degraded' if online_sensors > 0 else 'critical'
        }
    
    def close(self):
        """Clean up resources"""
        try:
            # DHT22 sensors don't require explicit cleanup, but we can clear references
            for sensor in self.sensors.values():
                sensor['device'] = None
            
            self.logger.info("DHT22 reader closed")
            
        except Exception as e:
            self.logger.error(f"Error closing DHT22 reader: {e}")
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        self.close()
