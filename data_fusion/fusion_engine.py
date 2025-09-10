#!/usr/bin/env python3
"""
Data Fusion Engine for Raspberry Pi Sensor Dashboard
data_fusion/fusion_engine.py
"""

import time
import math
import threading
import statistics
from datetime import datetime, timedelta
from collections import deque
import logging
import json

class FusionEngine:
    def __init__(self, config, db_manager=None, socketio=None):
        """
        Initialize the data fusion engine
        
        Args:
            config: Configuration object
            db_manager: Database manager instance
            socketio: SocketIO instance for real-time updates
        """
        self.config = config
        self.db_manager = db_manager
        self.socketio = socketio
        self.logger = logging.getLogger(__name__)
        
        # Fusion configuration
        self.fusion_config = config.FUSION_CONFIG
        self.comfort_ranges = self.fusion_config['comfort_ranges']
        self.anomaly_threshold = self.fusion_config['anomaly_threshold']
        
        # Data storage
        self.sensor_data = {}
        self.fusion_results = {}
        self.historical_fusion = deque(maxlen=self.fusion_config['history_window'])
        
        # Threading
        self.running = False
        self.fusion_thread = None
        
        # Initialize fusion results structure
        self._initialize_fusion_results()
        
        # Statistical tracking
        self.temp_history = deque(maxlen=50)
        self.humidity_history = deque(maxlen=50)
        self.air_quality_history = deque(maxlen=50)
        self.sound_level_history = deque(maxlen=50)
    
    def _initialize_fusion_results(self):
        """Initialize the fusion results structure"""
        self.fusion_results = {
            'comfort_index': None,
            'environment_status': None,
            'anomaly_detected': False,
            'anomaly_details': [],
            'correlations': {},
            'averages': {},
            'trends': {},
            'air_quality_index': None,
            'noise_level_status': None,
            'recommendations': [],
            'sensor_health': {},
            'timestamp': None
        }
    
    def start(self):
        """Start the fusion engine"""
        if self.running:
            self.logger.warning("Fusion engine is already running")
            return
        
        self.running = True
        self.fusion_thread = threading.Thread(target=self._fusion_loop, daemon=True)
        self.fusion_thread.start()
        
        self.logger.info("Data fusion engine started")
    
    def stop(self):
        """Stop the fusion engine"""
        self.running = False
        if self.fusion_thread:
            self.fusion_thread.join(timeout=5)
        
        self.logger.info("Data fusion engine stopped")
    
    def update_sensor_data(self, sensor_data):
        """
        Update sensor data for fusion processing
        
        Args:
            sensor_data: Dictionary of current sensor readings
        """
        self.sensor_data = sensor_data.copy()
        
        # Update historical data for trend analysis
        self._update_historical_data()
    
    def _update_historical_data(self):
        """Update historical data for trend analysis"""
        try:
            # Collect temperature readings
            temps = []
            for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
                if (sensor_id in self.sensor_data and 
                    self.sensor_data[sensor_id].get('temp') is not None):
                    temps.append(self.sensor_data[sensor_id]['temp'])
            
            if temps:
                avg_temp = statistics.mean(temps)
                self.temp_history.append(avg_temp)
            
            # Collect humidity readings
            humidities = []
            for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
                if (sensor_id in self.sensor_data and 
                    self.sensor_data[sensor_id].get('humidity') is not None):
                    humidities.append(self.sensor_data[sensor_id]['humidity'])
            
            if humidities:
                avg_humidity = statistics.mean(humidities)
                self.humidity_history.append(avg_humidity)
            
            # Air quality
            if ('mq135' in self.sensor_data and 
                self.sensor_data['mq135'].get('air_quality') is not None):
                self.air_quality_history.append(self.sensor_data['mq135']['air_quality'])
            
            # Sound level
            if ('dfr0026' in self.sensor_data and 
                self.sensor_data['dfr0026'].get('sound_level') is not None):
                self.sound_level_history.append(self.sensor_data['dfr0026']['sound_level'])
                
        except Exception as e:
            self.logger.error(f"Error updating historical data: {e}")
    
    def _fusion_loop(self):
        """Main fusion processing loop"""
        while self.running:
            try:
                if self.sensor_data:
                    self.process_fusion()
                
                time.sleep(self.fusion_config['update_interval'])
                
            except Exception as e:
                self.logger.error(f"Fusion loop error: {e}")
                time.sleep(self.fusion_config['update_interval'] * 2)
    
    def process_fusion(self):
        """Main fusion processing function"""
        try:
            # Calculate averages
            averages = self._calculate_averages()
            
            # Calculate comfort index
            comfort_index = self._calculate_comfort_index(averages)
            
            # Determine environment status
            environment_status = self._determine_environment_status(averages)
            
            # Detect anomalies
            anomaly_results = self._detect_anomalies()
            
            # Calculate correlations
            correlations = self._calculate_correlations()
            
            # Analyze trends
            trends = self._analyze_trends()
            
            # Generate recommendations
            recommendations = self._generate_recommendations(averages, anomaly_results['detected'])
            
            # Assess sensor health
            sensor_health = self._assess_sensor_health()
            
            # Update fusion results
            self.fusion_results.update({
                'comfort_index': comfort_index,
                'environment_status': environment_status,
                'anomaly_detected': anomaly_results['detected'],
                'anomaly_details': anomaly_results['details'],
                'correlations': correlations,
                'averages': averages,
                'trends': trends,
                'air_quality_index': self._calculate_air_quality_index(averages),
                'noise_level_status': self._determine_noise_status(averages),
                'recommendations': recommendations,
                'sensor_health': sensor_health,
                'timestamp': datetime.now().isoformat()
            })
            
            # Store in historical fusion data
            self.historical_fusion.append(dict(self.fusion_results))
            
            # Store in database
            if self.db_manager:
                self.db_manager.store_fusion_data(self.fusion_results)
            
            # Emit real-time update
            if self.socketio:
                self.socketio.emit('fusion_update', self.fusion_results)
                
        except Exception as e:
            self.logger.error(f"Fusion processing error: {e}")
    
    def _calculate_averages(self):
        """Calculate average values from sensor data"""
        averages = {}
        
        # Temperature average
        temps = []
        for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
            if (sensor_id in self.sensor_data and 
                self.sensor_data[sensor_id].get('temp') is not None):
                temps.append(self.sensor_data[sensor_id]['temp'])
        
        if temps:
            averages['temperature'] = round(statistics.mean(temps), 1)
            averages['temperature_std'] = round(statistics.stdev(temps) if len(temps) > 1 else 0, 2)
        
        # Humidity average
        humidities = []
        for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
            if (sensor_id in self.sensor_data and 
                self.sensor_data[sensor_id].get('humidity') is not None):
                humidities.append(self.sensor_data[sensor_id]['humidity'])
        
        if humidities:
            averages['humidity'] = round(statistics.mean(humidities), 1)
            averages['humidity_std'] = round(statistics.stdev(humidities) if len(humidities) > 1 else 0, 2)
        
        # Air quality
        if ('mq135' in self.sensor_data and 
            self.sensor_data['mq135'].get('air_quality') is not None):
            averages['air_quality'] = self.sensor_data['mq135']['air_quality']
        
        # Sound level
        if ('dfr0026' in self.sensor_data and 
            self.sensor_data['dfr0026'].get('sound_level') is not None):
            averages['sound_level'] = self.sensor_data['dfr0026']['sound_level']
        
        return averages
    
    def _calculate_comfort_index(self, averages):
        """Calculate comfort index (0-100)"""
        if 'temperature' not in averages or 'humidity' not in averages:
            return None
        
        temp = averages['temperature']
        humidity = averages['humidity']
        
        temp_range = self.comfort_ranges['temperature']
        humidity_range = self.comfort_ranges['humidity']
        
        # Temperature comfort (0-100)
        if temp_range['min'] <= temp <= temp_range['max']:
            temp_comfort = 100 - abs(temp - temp_range['optimal']) * 5
        else:
            temp_comfort = max(0, 100 - abs(temp - temp_range['optimal']) * 8)
        
        # Humidity comfort (0-100)
        if humidity_range['min'] <= humidity <= humidity_range['max']:
            humidity_comfort = 100 - abs(humidity - humidity_range['optimal']) * 2
        else:
            humidity_comfort = max(0, 100 - abs(humidity - humidity_range['optimal']) * 3)
        
        # Air quality impact (if available)
        air_quality_impact = 1.0
        if 'air_quality' in averages:
            air_quality = averages['air_quality']
            ranges = self.comfort_ranges['air_quality']
            
            if air_quality <= ranges['good']:
                air_quality_impact = 1.0
            elif air_quality <= ranges['moderate']:
                air_quality_impact = 0.8
            else:
                air_quality_impact = 0.6
        
        # Sound level impact (if available)
        sound_impact = 1.0
        if 'sound_level' in averages:
            sound_level = averages['sound_level']
            ranges = self.comfort_ranges['sound_level']
            
            if sound_level <= ranges['quiet']:
                sound_impact = 1.0
            elif sound_level <= ranges['moderate']:
                sound_impact = 0.9
            else:
                sound_impact = 0.7
        
        # Combined comfort index
        base_comfort = (temp_comfort + humidity_comfort) / 2
        comfort_index = base_comfort * air_quality_impact * sound_impact
        
        return round(max(0, min(100, comfort_index)), 1)
    
    def _determine_environment_status(self, averages):
        """Determine overall environment status"""
        if 'temperature' not in averages or 'humidity' not in averages:
            return "Unknown"
        
        comfort_index = self._calculate_comfort_index(averages)
        
        if comfort_index is None:
            return "Unknown"
        
        if comfort_index >= 80:
            return "Excellent"
        elif comfort_index >= 60:
            return "Good"
        elif comfort_index >= 40:
            return "Fair"
        else:
            return "Poor"
    
    def _detect_anomalies(self):
        """Detect anomalies in sensor data"""
        anomalies = []
        
        try:
            # Check for sensor variance anomalies
            temps = []
            for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
                if (sensor_id in self.sensor_data and 
                    self.sensor_data[sensor_id].get('temp') is not None):
                    temps.append(self.sensor_data[sensor_id]['temp'])
            
            if len(temps) > 1:
                temp_std = statistics.stdev(temps)
                if temp_std > self.anomaly_threshold['temperature_std']:
                    anomalies.append({
                        'type': 'temperature_variance',
                        'severity': 'high' if temp_std > 10 else 'medium',
                        'description': f'High temperature variance detected ({temp_std:.1f}°C)',
                        'value': temp_std,
                        'threshold': self.anomaly_threshold['temperature_std']
                    })
            
            # Check for humidity variance anomalies
            humidities = []
            for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
                if (sensor_id in self.sensor_data and 
                    self.sensor_data[sensor_id].get('humidity') is not None):
                    humidities.append(self.sensor_data[sensor_id]['humidity'])
            
            if len(humidities) > 1:
                humidity_std = statistics.stdev(humidities)
                if humidity_std > self.anomaly_threshold['humidity_std']:
                    anomalies.append({
                        'type': 'humidity_variance',
                        'severity': 'high' if humidity_std > 25 else 'medium',
                        'description': f'High humidity variance detected ({humidity_std:.1f}%)',
                        'value': humidity_std,
                        'threshold': self.anomaly_threshold['humidity_std']
                    })
            
            # Check for sudden changes in trends
            if len(self.temp_history) >= 10:
                recent_temps = list(self.temp_history)[-10:]
                if len(recent_temps) >= 5:
                    recent_change = abs(recent_temps[-1] - recent_temps[-5])
                    if recent_change > self.anomaly_threshold['sudden_change_threshold']:
                        anomalies.append({
                            'type': 'sudden_temperature_change',
                            'severity': 'high' if recent_change > 15 else 'medium',
                            'description': f'Sudden temperature change detected ({recent_change:.1f}°C)',
                            'value': recent_change,
                            'threshold': self.anomaly_threshold['sudden_change_threshold']
                        })
            
            # Check for extreme values
            if 'temperature' in self.sensor_data:
                for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
                    if (sensor_id in self.sensor_data and 
                        self.sensor_data[sensor_id].get('temp') is not None):
                        temp = self.sensor_data[sensor_id]['temp']
                        if temp < -10 or temp > 50:
                            anomalies.append({
                                'type': 'extreme_temperature',
                                'severity': 'high',
                                'description': f'Extreme temperature detected on {sensor_id}: {temp}°C',
                                'value': temp,
                                'sensor_id': sensor_id
                            })
            
            # Check for air quality anomalies
            if ('mq135' in self.sensor_data and 
                self.sensor_data['mq135'].get('air_quality') is not None):
                air_quality = self.sensor_data['mq135']['air_quality']
                if air_quality > 90:
                    anomalies.append({
                        'type': 'poor_air_quality',
                        'severity': 'high',
                        'description': f'Very poor air quality detected: {air_quality}%',
                        'value': air_quality
                    })
            
        except Exception as e:
            self.logger.error(f"Anomaly detection error: {e}")
        
        return {
            'detected': len(anomalies) > 0,
            'count': len(anomalies),
            'details': anomalies
        }
    
    def _calculate_correlations(self):
        """Calculate correlations between sensor readings"""
        correlations = {}
        
        try:
            # Temperature-Humidity correlation
            if len(self.temp_history) >= 10 and len(self.humidity_history) >= 10:
                temp_data = list(self.temp_history)[-10:]
                humidity_data = list(self.humidity_history)[-10:]
                
                if len(temp_data) == len(humidity_data):
                    correlation = self._calculate_correlation_coefficient(temp_data, humidity_data)
                    correlations['temperature_humidity'] = round(correlation, 3)
            
            # Temperature-Air Quality correlation
            if (len(self.temp_history) >= 10 and len(self.air_quality_history) >= 10):
                temp_data = list(self.temp_history)[-10:]
                air_quality_data = list(self.air_quality_history)[-10:]
                
                min_len = min(len(temp_data), len(air_quality_data))
                if min_len >= 5:
                    correlation = self._calculate_correlation_coefficient(
                        temp_data[-min_len:], air_quality_data[-min_len:]
                    )
                    correlations['temperature_air_quality'] = round(correlation, 3)
            
            # Sound level patterns
            if len(self.sound_level_history) >= 10:
                sound_data = list(self.sound_level_history)[-10:]
                correlations['sound_level_stability'] = round(
                    1 - (statistics.stdev(sound_data) / statistics.mean(sound_data)), 3
                ) if statistics.mean(sound_data) > 0 else 0
                
        except Exception as e:
            self.logger.error(f"Correlation calculation error: {e}")
        
        return correlations
    
    def _calculate_correlation_coefficient(self, x, y):
        """Calculate Pearson correlation coefficient"""
        try:
            n = len(x)
            if n != len(y) or n < 2:
                return 0
            
            sum_x = sum(x)
            sum_y = sum(y)
            sum_x_sq = sum(xi * xi for xi in x)
            sum_y_sq = sum(yi * yi for yi in y)
            sum_xy = sum(x[i] * y[i] for i in range(n))
            
            numerator = n * sum_xy - sum_x * sum_y
            denominator = math.sqrt((n * sum_x_sq - sum_x**2) * (n * sum_y_sq - sum_y**2))
            
            return numerator / denominator if denominator != 0 else 0
            
        except Exception:
            return 0
    
    def _analyze_trends(self):
        """Analyze trends in sensor data"""
        trends = {}
        
        try:
            # Temperature trend
            if len(self.temp_history) >= 20:
                recent_temps = list(self.temp_history)[-20:]
                first_half = recent_temps[:10]
                second_half = recent_temps[10:]
                
                trend_value = statistics.mean(second_half) - statistics.mean(first_half)
                trends['temperature'] = {
                    'direction': 'increasing' if trend_value > 0.5 else 'decreasing' if trend_value < -0.5 else 'stable',
                    'rate': round(trend_value, 2),
                    'confidence': min(1.0, abs(trend_value) / 2.0)
                }
            
            # Humidity trend
            if len(self.humidity_history) >= 20:
                recent_humidity = list(self.humidity_history)[-20:]
                first_half = recent_humidity[:10]
                second_half = recent_humidity[10:]
                
                trend_value = statistics.mean(second_half) - statistics.mean(first_half)
                trends['humidity'] = {
                    'direction': 'increasing' if trend_value > 2 else 'decreasing' if trend_value < -2 else 'stable',
                    'rate': round(trend_value, 2),
                    'confidence': min(1.0, abs(trend_value) / 5.0)
                }
            
            # Air quality trend
            if len(self.air_quality_history) >= 20:
                recent_air = list(self.air_quality_history)[-20:]
                first_half = recent_air[:10]
                second_half = recent_air[10:]
                
                trend_value = statistics.mean(second_half) - statistics.mean(first_half)
                trends['air_quality'] = {
                    'direction': 'worsening' if trend_value > 5 else 'improving' if trend_value < -5 else 'stable',
                    'rate': round(trend_value, 2),
                    'confidence': min(1.0, abs(trend_value) / 10.0)
                }
                
        except Exception as e:
            self.logger.error(f"Trend analysis error: {e}")
        
        return trends
    
    def _calculate_air_quality_index(self, averages):
        """Calculate air quality index"""
        if 'air_quality' not in averages:
            return None
        
        air_quality = averages['air_quality']
        ranges = self.comfort_ranges['air_quality']
        
        if air_quality <= ranges['good']:
            return "Good"
        elif air_quality <= ranges['moderate']:
            return "Moderate"
        else:
            return "Poor"
    
    def _determine_noise_status(self, averages):
        """Determine noise level status"""
        if 'sound_level' not in averages:
            return None
        
        sound_level = averages['sound_level']
        ranges = self.comfort_ranges['sound_level']
        
        if sound_level <= ranges['quiet']:
            return "Quiet"
        elif sound_level <= ranges['moderate']:
            return "Moderate"
        else:
            return "Loud"
    
    def _generate_recommendations(self, averages, anomaly_detected):
        """Generate environmental recommendations"""
        recommendations = []
        
        try:
            # Temperature recommendations
            if 'temperature' in averages:
                temp = averages['temperature']
                optimal = self.comfort_ranges['temperature']['optimal']
                
                if temp < optimal - 3:
                    recommendations.append({
                        'type': 'temperature',
                        'priority': 'medium',
                        'message': f'Temperature is {temp}°C, consider heating to reach optimal {optimal}°C'
                    })
                elif temp > optimal + 3:
                    recommendations.append({
                        'type': 'temperature',
                        'priority': 'medium',
                        'message': f'Temperature is {temp}°C, consider cooling to reach optimal {optimal}°C'
                    })
            
            # Humidity recommendations
            if 'humidity' in averages:
                humidity = averages['humidity']
                optimal = self.comfort_ranges['humidity']['optimal']
                
                if humidity < optimal - 10:
                    recommendations.append({
                        'type': 'humidity',
                        'priority': 'medium',
                        'message': f'Humidity is {humidity}%, consider using a humidifier'
                    })
                elif humidity > optimal + 15:
                    recommendations.append({
                        'type': 'humidity',
                        'priority': 'high',
                        'message': f'Humidity is {humidity}%, consider using a dehumidifier'
                    })
            
            # Air quality recommendations
            if 'air_quality' in averages:
                air_quality = averages['air_quality']
                if air_quality > self.comfort_ranges['air_quality']['moderate']:
                    recommendations.append({
                        'type': 'air_quality',
                        'priority': 'high',
                        'message': 'Air quality is poor, consider improving ventilation'
                    })
            
            # Sound level recommendations
            if 'sound_level' in averages:
                sound_level = averages['sound_level']
                if sound_level > self.comfort_ranges['sound_level']['moderate']:
                    recommendations.append({
                        'type': 'sound_level',
                        'priority': 'medium',
                        'message': 'Sound level is high, consider noise reduction measures'
                    })
            
            # Anomaly recommendations
            if anomaly_detected:
                recommendations.append({
                    'type': 'anomaly',
                    'priority': 'high',
                    'message': 'Anomalies detected in sensor data, check sensor placement and calibration'
                })
                
        except Exception as e:
            self.logger.error(f"Recommendation generation error: {e}")
        
        return recommendations
    
    def _assess_sensor_health(self):
        """Assess health of individual sensors"""
        health = {}
        
        try:
            # DHT22 sensor health
            for sensor_id in ['dht22_1', 'dht22_2', 'dht22_3']:
                if sensor_id in self.sensor_data:
                    sensor_data = self.sensor_data[sensor_id]
                    status = sensor_data.get('status', 'unknown')
                    
                    health[sensor_id] = {
                        'status': status,
                        'health_score': 100 if status == 'online' else 0,
                        'last_reading': sensor_data.get('timestamp')
                    }
            
            # MQ135 sensor health
            if 'mq135' in self.sensor_data:
                sensor_data = self.sensor_data['mq135']
                status = sensor_data.get('status', 'unknown')
                
                health['mq135'] = {
                    'status': status,
                    'health_score': 100 if status == 'online' else 0,
                    'last_reading': sensor_data.get('timestamp')
                }
            
            # DFR0026 sensor health
            if 'dfr0026' in self.sensor_data:
                sensor_data = self.sensor_data['dfr0026']
                status = sensor_data.get('status', 'unknown')
                
                health['dfr0026'] = {
                    'status': status,
                    'health_score': 100 if status == 'online' else 0,
                    'last_reading': sensor_data.get('timestamp')
                }
                
        except Exception as e:
            self.logger.error(f"Sensor health assessment error: {e}")
        
        return health
    
    def get_current_results(self):
        """Get current fusion results"""
        return dict(self.fusion_results)
    
    def get_historical_results(self, limit=50):
        """Get historical fusion results"""
        return list(self.historical_fusion)[-limit:]
    
    def reset_history(self):
        """Reset historical data"""
        self.temp_history.clear()
        self.humidity_history.clear()
        self.air_quality_history.clear()
        self.sound_level_history.clear()
        self.historical_fusion.clear()
        
        self.logger.info("Fusion engine history reset")
    
    def get_statistics(self):
        """Get fusion engine statistics"""
        return {
            'total_fusion_cycles': len(self.historical_fusion),
            'average_comfort_index': statistics.mean([
                r['comfort_index'] for r in self.historical_fusion 
                if r['comfort_index'] is not None
            ]) if self.historical_fusion else None,
            'anomaly_detection_rate': sum([
                1 for r in self.historical_fusion if r['anomaly_detected']
            ]) / len(self.historical_fusion) if self.historical_fusion else 0,
            'data_points': {
                'temperature': len(self.temp_history),
                'humidity': len(self.humidity_history),
                'air_quality': len(self.air_quality_history),
                'sound_level': len(self.sound_level_history)
            }
        }
