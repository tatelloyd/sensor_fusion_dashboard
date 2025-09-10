#!/usr/bin/env python3
"""
Modular Sensor Manager for Raspberry Pi Dashboard
sensors/sensor_manager.py
"""

import time
import threading
from datetime import datetime
from collections import deque
import logging

# Import sensor modules
from .dht22_reader import DHT22Reader
from .mq135_reader import MQ135Reader
from .dfr0026_reader import DFR0026Reader

class SensorManager:
    def __init__(self, config, socketio=None, db_manager=None):
        """
        Initialize the sensor manager with configuration
        
        Args:
            config: Configuration object
            socketio: SocketIO instance for real-time updates
            db_manager: Database manager instance
        """
        self.config = config
        self.socketio = socketio
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Initialize sensors
        self.sensors = {}
        self.sensor_data = {}
        self.historical_data = deque(maxlen=config.FUSION_CONFIG['history_window'])
        
        # Threading control
        self.running = False
        self.threads = []
        
        # Initialize individual sensors
        self._initialize_sensors()
        
        # Initialize data storage
        self._initialize_data_structure()
    
    def _initialize_sensors(self):
        """Initialize all sensor instances"""
        try:
            # Initialize DHT22 sensors
            self.sensors['dht22'] = DHT22Reader(
                self.config.SENSOR_CONFIG['DHT22']
            )
            
            # Initialize MQ-135 sensor
            self.sensors['mq135'] = MQ135Reader(
                self.config.SENSOR_CONFIG['MQ135'],
                self.config.MCP3008_CONFIG
            )
            
            # Initialize DFR0026 sensor
            self.sensors['dfr0026'] = DFR0026Reader(
                self.config.SENSOR_CONFIG['DFR0026'],
                self.config.MCP3008_CONFIG
            )
            
            self.logger.info("All sensors initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing sensors: {e}")
            raise
    
    def _initialize_data_structure(self):
        """Initialize the data structure for storing sensor readings"""
        self.sensor_data = {
            'dht22_1': {'temp': None, 'humidity': None, 'timestamp': None, 'status': 'offline'},
            'dht22_2': {'temp': None, 'humidity': None, 'timestamp': None, 'status': 'offline'},
            'dht22_3': {'temp': None, 'humidity': None, 'timestamp': None, 'status': 'offline'},
            'mq135': {'adc': None, 'voltage': None, 'air_quality': None, 'timestamp': None, 'status': 'offline'},
            'dfr0026': {'adc': None, 'voltage': None, 'sound_level': None, 'db_estimate': None, 'timestamp': None, 'status': 'offline'}
        }
    
    def start(self):
        """Start all sensor reading threads"""
        if self.running:
            self.logger.warning("Sensor manager is already running")
            return
        
        self.running = True
        self.logger.info("Starting sensor manager...")
        
        # Start DHT22 reading thread
        dht22_thread = threading.Thread(target=self._dht22_loop, daemon=True)
        dht22_thread.start()
        self.threads.append(dht22_thread)
        
        # Start MQ-135 reading thread
        mq135_thread = threading.Thread(target=self._mq135_loop, daemon=True)
        mq135_thread.start()
        self.threads.append(mq135_thread)
        
        # Start DFR0026 reading thread
        dfr0026_thread = threading.Thread(target=self._dfr0026_loop, daemon=True)
        dfr0026_thread.start()
        self.threads.append(dfr0026_thread)
        
        # Start data aggregation thread
        aggregation_thread = threading.Thread(target=self._data_aggregation_loop, daemon=True)
        aggregation_thread.start()
        self.threads.append(aggregation_thread)
        
        self.logger.info("All sensor threads started")
    
    def stop(self):
        """Stop all sensor reading threads"""
        self.logger.info("Stopping sensor manager...")
        self.running = False
        
        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=5)
        
        # Close sensor connections
        for sensor in self.sensors.values():
            if hasattr(sensor, 'close'):
                sensor.close()
        
        self.logger.info("Sensor manager stopped")
    
    def _dht22_loop(self):
        """DHT22 sensors reading loop"""
        interval = self.config.SENSOR_CONFIG['DHT22']['read_interval']
        
        while self.running:
            try:
                readings = self.sensors['dht22'].read_all()
                
                for sensor_id, data in readings.items():
                    if data['success']:
                        self.sensor_data[sensor_id].update({
                            'temp': data['temperature'],
                            'humidity': data['humidity'],
                            'timestamp': datetime.now().isoformat(),
                            'status': 'online'
                        })
                        
                        # Store in database
                        if self.db_manager:
                            self.db_manager.store_sensor_reading(
                                sensor_type='DHT22',
                                sensor_id=sensor_id,
                                temperature=data['temperature'],
                                humidity=data['humidity'],
                                timestamp=datetime.now()
                            )
                    else:
                        self.sensor_data[sensor_id]['status'] = 'error'
                        self.logger.warning(f"DHT22 {sensor_id} error: {data.get('error', 'Unknown error')}")
                
                # Emit real-time update
                if self.socketio:
                    self.socketio.emit('sensor_update', {
                        'type': 'dht22',
                        'data': {k: v for k, v in self.sensor_data.items() if k.startswith('dht22')}
                    })
                
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"DHT22 loop error: {e}")
                time.sleep(interval * 2)  # Wait longer on error
    
    def _mq135_loop(self):
        """MQ-135 sensor reading loop"""
        interval = self.config.SENSOR_CONFIG['MQ135']['read_interval']
        
        while self.running:
            try:
                reading = self.sensors['mq135'].read()
                
                if reading['success']:
                    self.sensor_data['mq135'].update({
                        'adc': reading['adc_value'],
                        'voltage': reading['voltage'],
                        'air_quality': reading['air_quality'],
                        'timestamp': datetime.now().isoformat(),
                        'status': 'online'
                    })
                    
                    # Store in database
                    if self.db_manager:
                        self.db_manager.store_sensor_reading(
                            sensor_type='MQ135',
                            sensor_id='mq135',
                            adc_value=reading['adc_value'],
                            voltage=reading['voltage'],
                            processed_value=reading['air_quality'],
                            timestamp=datetime.now()
                        )
                else:
                    self.sensor_data['mq135']['status'] = 'error'
                    self.logger.warning(f"MQ-135 error: {reading.get('error', 'Unknown error')}")
                
                # Emit real-time update
                if self.socketio:
                    self.socketio.emit('sensor_update', {
                        'type': 'mq135',
                        'data': {'mq135': self.sensor_data['mq135']}
                    })
                
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"MQ-135 loop error: {e}")
                time.sleep(interval * 2)
    
    def _dfr0026_loop(self):
        """DFR0026 sensor reading loop"""
        interval = self.config.SENSOR_CONFIG['DFR0026']['read_interval']
        
        while self.running:
            try:
                reading = self.sensors['dfr0026'].read()
                
                if reading['success']:
                    self.sensor_data['dfr0026'].update({
                        'adc': reading['adc_value'],
                        'voltage': reading['voltage'],
                        'sound_level': reading['sound_level'],
                        'db_estimate': reading['db_estimate'],
                        'timestamp': datetime.now().isoformat(),
                        'status': 'online'
                    })
                    
                    # Store in database
                    if self.db_manager:
                        self.db_manager.store_sensor_reading(
                            sensor_type='DFR0026',
                            sensor_id='dfr0026',
                            adc_value=reading['adc_value'],
                            voltage=reading['voltage'],
                            processed_value=reading['sound_level'],
                            extra_data={'db_estimate': reading['db_estimate']},
                            timestamp=datetime.now()
                        )
                else:
                    self.sensor_data['dfr0026']['status'] = 'error'
                    self.logger.warning(f"DFR0026 error: {reading.get('error', 'Unknown error')}")
                
                # Emit real-time update
                if self.socketio:
                    self.socketio.emit('sensor_update', {
                        'type': 'dfr0026',
                        'data': {'dfr0026': self.sensor_data['dfr0026']}
                    })
                
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"DFR0026 loop error: {e}")
                time.sleep(interval * 2)
    
    def _data_aggregation_loop(self):
        """Data aggregation and historical storage loop"""
        while self.running:
            try:
                # Create a snapshot of current data
                snapshot = {
                    'timestamp': datetime.now().isoformat(),
                    'sensors': dict(self.sensor_data)
                }
                
                # Add to historical data
                self.historical_data.append(snapshot)
                
                # Emit aggregated update
                if self.socketio:
                    self.socketio.emit('data_snapshot', snapshot)
                
                time.sleep(10)  # Aggregate every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Data aggregation loop error: {e}")
                time.sleep(10)
    
    def get_current_data(self):
        """Get current sensor data"""
        return dict(self.sensor_data)
    
    def get_historical_data(self, limit=100):
        """Get historical sensor data"""
        return list(self.historical_data)[-limit:]
    
    def get_sensor_status(self):
        """Get status of all sensors"""
        status = {}
        for sensor_id, data in self.sensor_data.items():
            status[sensor_id] = {
                'status': data.get('status', 'unknown'),
                'last_update': data.get('timestamp'),
                'sensor_type': sensor_id.split('_')[0] if '_' in sensor_id else sensor_id
            }
        return status
    
    def calibrate_sensor(self, sensor_id, calibration_data):
        """Calibrate a specific sensor"""
        try:
            if sensor_id.startswith('dht22'):
                # DHT22 calibration (if needed)
                pass
            elif sensor_id == 'mq135':
                self.sensors['mq135'].calibrate(calibration_data)
            elif sensor_id == 'dfr0026':
                self.sensors['dfr0026'].calibrate(calibration_data)
            
            self.logger.info(f"Sensor {sensor_id} calibrated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error calibrating sensor {sensor_id}: {e}")
            return False
