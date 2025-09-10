#!/usr/bin/env python3
"""
Analytics Engine for Raspberry Pi Sensor Dashboard
data_fusion/analytics.py
"""

import math
import statistics
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict, deque
import logging
from typing import Dict, List, Optional, Tuple, Any
import sqlite3
import json

class SensorAnalytics:
    def __init__(self, db_manager=None, config=None):
        """
        Initialize the analytics engine
        
        Args:
            db_manager: Database manager instance
            config: Configuration object
        """
        self.db_manager = db_manager
        self.config = config or self._default_config()
        self.logger = logging.getLogger(__name__)
        
        # Cache for recent calculations
        self.calculation_cache = {}
        self.cache_timeout = 300  # 5 minutes
        
        # Statistical thresholds
        self.thresholds = self.config.FUSION_CONFIG['anomaly_threshold']
        self.comfort_ranges = self.config.FUSION_CONFIG['comfort_ranges']
    
    def _default_config(self):
        """Default configuration if none provided"""
        class DefaultConfig:
            FUSION_CONFIG = {
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
        return DefaultConfig()
    
    def _get_cache_key(self, operation: str, **kwargs) -> str:
        """Generate cache key for operations"""
        key_parts = [operation]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        return "_".join(key_parts)
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached result is still valid"""
        if cache_key not in self.calculation_cache:
            return False
        
        cached_time = self.calculation_cache[cache_key].get('timestamp')
        if not cached_time:
            return False
        
        return (datetime.now() - cached_time).total_seconds() < self.cache_timeout
    
    def _cache_result(self, cache_key: str, result: Any) -> None:
        """Cache a calculation result"""
        self.calculation_cache[cache_key] = {
            'result': result,
            'timestamp': datetime.now()
        }
    
    def _get_cached_result(self, cache_key: str) -> Any:
        """Get cached result"""
        return self.calculation_cache[cache_key]['result']
    
    def basic_statistics(self, data: List[float]) -> Dict[str, float]:
        """
        Calculate basic statistics for a dataset
        
        Args:
            data: List of numerical values
            
        Returns:
            Dictionary with statistical measures
        """
        if not data:
            return {
                'count': 0,
                'mean': None,
                'median': None,
                'std': None,
                'min': None,
                'max': None,
                'range': None
            }
        
        try:
            return {
                'count': len(data),
                'mean': round(statistics.mean(data), 2),
                'median': round(statistics.median(data), 2),
                'std': round(statistics.stdev(data) if len(data) > 1 else 0, 2),
                'min': round(min(data), 2),
                'max': round(max(data), 2),
                'range': round(max(data) - min(data), 2)
            }
        except Exception as e:
            self.logger.error(f"Error calculating basic statistics: {e}")
            return {'error': str(e)}
    
    def moving_average(self, data: List[float], window: int = 10) -> List[float]:
        """
        Calculate moving average
        
        Args:
            data: List of numerical values
            window: Window size for moving average
            
        Returns:
            List of moving averages
        """
        if len(data) < window:
            return data
        
        averages = []
        for i in range(len(data) - window + 1):
            avg = statistics.mean(data[i:i + window])
            averages.append(round(avg, 2))
        
        return averages
    
    def exponential_smoothing(self, data: List[float], alpha: float = 0.3) -> List[float]:
        """
        Apply exponential smoothing to data
        
        Args:
            data: List of numerical values
            alpha: Smoothing factor (0 < alpha < 1)
            
        Returns:
            List of smoothed values
        """
        if not data:
            return []
        
        smoothed = [data[0]]
        for i in range(1, len(data)):
            smoothed_value = alpha * data[i] + (1 - alpha) * smoothed[-1]
            smoothed.append(round(smoothed_value, 2))
        
        return smoothed
    
    def detect_outliers(self, data: List[float], method: str = 'iqr') -> Dict[str, Any]:
        """
        Detect outliers in data
        
        Args:
            data: List of numerical values
            method: Method to use ('iqr', 'zscore', 'modified_zscore')
            
        Returns:
            Dictionary with outlier information
        """
        if len(data) < 3:
            return {'outliers': [], 'outlier_indices': [], 'method': method}
        
        outliers = []
        outlier_indices = []
        
        try:
            if method == 'iqr':
                q1 = statistics.quantiles(data, n=4)[0]
                q3 = statistics.quantiles(data, n=4)[2]
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                for i, value in enumerate(data):
                    if value < lower_bound or value > upper_bound:
                        outliers.append(value)
                        outlier_indices.append(i)
            
            elif method == 'zscore':
                mean = statistics.mean(data)
                std = statistics.stdev(data)
                threshold = 2.5
                
                for i, value in enumerate(data):
                    z_score = abs((value - mean) / std) if std > 0 else 0
                    if z_score > threshold:
                        outliers.append(value)
                        outlier_indices.append(i)
            
            elif method == 'modified_zscore':
                median = statistics.median(data)
                mad = statistics.median([abs(x - median) for x in data])
                threshold = 3.5
                
                for i, value in enumerate(data):
                    if mad > 0:
                        modified_z_score = 0.6745 * (value - median) / mad
                        if abs(modified_z_score) > threshold:
                            outliers.append(value)
                            outlier_indices.append(i)
            
            return {
                'outliers': outliers,
                'outlier_indices': outlier_indices,
                'outlier_count': len(outliers),
                'outlier_percentage': round((len(outliers) / len(data)) * 100, 2),
                'method': method
            }
            
        except Exception as e:
            self.logger.error(f"Error detecting outliers: {e}")
            return {'error': str(e)}
    
    def correlation_analysis(self, data1: List[float], data2: List[float]) -> Dict[str, float]:
        """
        Calculate correlation between two datasets
        
        Args:
            data1: First dataset
            data2: Second dataset
            
        Returns:
            Dictionary with correlation metrics
        """
        if len(data1) != len(data2) or len(data1) < 2:
            return {'error': 'Invalid data for correlation analysis'}
        
        try:
            # Pearson correlation coefficient
            pearson_corr = statistics.correlation(data1, data2)
            
            # Spearman rank correlation (using numpy for convenience)
            try:
                rank1 = [sorted(data1).index(x) + 1 for x in data1]
                rank2 = [sorted(data2).index(x) + 1 for x in data2]
                spearman_corr = statistics.correlation(rank1, rank2)
            except:
                spearman_corr = None
            
            # Covariance
            mean1 = statistics.mean(data1)
            mean2 = statistics.mean(data2)
            covariance = sum((x - mean1) * (y - mean2) for x, y in zip(data1, data2)) / (len(data1) - 1)
            
            return {
                'pearson_correlation': round(pearson_corr, 3),
                'spearman_correlation': round(spearman_corr, 3) if spearman_corr else None,
                'covariance': round(covariance, 3),
                'sample_size': len(data1)
            }
            
        except Exception as e:
            self.logger.error(f"Error in correlation analysis: {e}")
            return {'error': str(e)}
    
    def trend_analysis(self, data: List[float], timestamps: List[datetime] = None) -> Dict[str, Any]:
        """
        Analyze trends in time series data
        
        Args:
            data: List of numerical values
            timestamps: Optional list of timestamps
            
        Returns:
            Dictionary with trend analysis
        """
        if len(data) < 3:
            return {'error': 'Insufficient data for trend analysis'}
        
        try:
            # Simple linear regression for trend
            n = len(data)
            x = list(range(n)) if not timestamps else [(t - timestamps[0]).total_seconds() for t in timestamps]
            y = data
            
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(x[i] * y[i] for i in range(n))
            sum_x2 = sum(xi * xi for xi in x)
            
            # Calculate slope and intercept
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
            intercept = (sum_y - slope * sum_x) / n
            
            # R-squared
            y_mean = statistics.mean(y)
            ss_tot = sum((yi - y_mean) ** 2 for yi in y)
            ss_res = sum((y[i] - (slope * x[i] + intercept)) ** 2 for i in range(n))
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            # Trend direction
            if abs(slope) < 0.001:
                trend_direction = 'stable'
            elif slope > 0:
                trend_direction = 'increasing'
            else:
                trend_direction = 'decreasing'
            
            # Rate of change
            rate_of_change = (data[-1] - data[0]) / len(data) if len(data) > 1 else 0
            
            return {
                'slope': round(slope, 4),
                'intercept': round(intercept, 4),
                'r_squared': round(r_squared, 4),
                'trend_direction': trend_direction,
                'rate_of_change': round(rate_of_change, 4),
                'trend_strength': 'strong' if abs(r_squared) > 0.7 else 'moderate' if abs(r_squared) > 0.3 else 'weak'
            }
            
        except Exception as e:
            self.logger.error(f"Error in trend analysis: {e}")
            return {'error': str(e)}
    
    def anomaly_detection(self, data: List[float], sensitivity: float = 2.0) -> Dict[str, Any]:
        """
        Detect anomalies in sensor data
        
        Args:
            data: List of numerical values
            sensitivity: Sensitivity threshold (lower = more sensitive)
            
        Returns:
            Dictionary with anomaly detection results
        """
        if len(data) < 10:
            return {'anomalies': [], 'anomaly_count': 0, 'warning': 'Insufficient data for reliable anomaly detection'}
        
        try:
            # Use multiple methods for anomaly detection
            
            # Method 1: Statistical outliers
            outliers = self.detect_outliers(data, method='modified_zscore')
            
            # Method 2: Sudden changes
            sudden_changes = []
            threshold = self.thresholds['sudden_change_threshold']
            
            for i in range(1, len(data)):
                change = abs(data[i] - data[i-1])
                if change > threshold:
                    sudden_changes.append({
                        'index': i,
                        'value': data[i],
                        'change': round(change, 2),
                        'type': 'sudden_change'
                    })
            
            # Method 3: Pattern-based anomalies (using moving average)
            window = min(10, len(data) // 3)
            moving_avg = self.moving_average(data, window)
            pattern_anomalies = []
            
            for i in range(len(moving_avg)):
                actual_index = i + window - 1
                if actual_index < len(data):
                    deviation = abs(data[actual_index] - moving_avg[i])
                    if deviation > sensitivity * statistics.stdev(data):
                        pattern_anomalies.append({
                            'index': actual_index,
                            'value': data[actual_index],
                            'expected': moving_avg[i],
                            'deviation': round(deviation, 2),
                            'type': 'pattern_deviation'
                        })
            
            # Combine all anomalies
            all_anomalies = []
            
            # Add outliers
            for i, outlier in enumerate(outliers['outliers']):
                all_anomalies.append({
                    'index': outliers['outlier_indices'][i],
                    'value': outlier,
                    'type': 'statistical_outlier',
                    'method': outliers['method']
                })
            
            # Add sudden changes
            all_anomalies.extend(sudden_changes)
            
            # Add pattern anomalies
            all_anomalies.extend(pattern_anomalies)
            
            # Remove duplicates based on index
            unique_anomalies = []
            seen_indices = set()
            for anomaly in all_anomalies:
                if anomaly['index'] not in seen_indices:
                    unique_anomalies.append(anomaly)
                    seen_indices.add(anomaly['index'])
            
            # Sort by index
            unique_anomalies.sort(key=lambda x: x['index'])
            
            return {
                'anomalies': unique_anomalies,
                'anomaly_count': len(unique_anomalies),
                'anomaly_percentage': round((len(unique_anomalies) / len(data)) * 100, 2),
                'methods_used': ['statistical_outlier', 'sudden_change', 'pattern_deviation'],
                'sensitivity': sensitivity
            }
            
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return {'error': str(e)}
    
    def comfort_analysis(self, temperature: List[float], humidity: List[float], 
                        air_quality: List[float] = None, sound_level: List[float] = None) -> Dict[str, Any]:
        """
        Analyze comfort levels based on multiple environmental factors
        
        Args:
            temperature: Temperature readings
            humidity: Humidity readings
            air_quality: Air quality readings (optional)
            sound_level: Sound level readings (optional)
            
        Returns:
            Dictionary with comfort analysis
        """
        if not temperature or not humidity:
            return {'error': 'Temperature and humidity data required'}
        
        try:
            comfort_scores = []
            detailed_analysis = []
            
            for i in range(min(len(temperature), len(humidity))):
                temp = temperature[i]
                hum = humidity[i]
                
                # Temperature comfort score
                temp_range = self.comfort_ranges['temperature']
                if temp_range['min'] <= temp <= temp_range['max']:
                    temp_score = 100 - abs(temp - temp_range['optimal']) * 5
                else:
                    temp_score = max(0, 100 - abs(temp - temp_range['optimal']) * 10)
                
                # Humidity comfort score
                hum_range = self.comfort_ranges['humidity']
                if hum_range['min'] <= hum <= hum_range['max']:
                    hum_score = 100 - abs(hum - hum_range['optimal']) * 2
                else:
                    hum_score = max(0, 100 - abs(hum - hum_range['optimal']) * 3)
                
                # Air quality score
                air_score = 100
                if air_quality and i < len(air_quality):
                    aq = air_quality[i]
                    if aq <= self.comfort_ranges['air_quality']['good']:
                        air_score = 100
                    elif aq <= self.comfort_ranges['air_quality']['moderate']:
                        air_score = 75
                    elif aq <= self.comfort_ranges['air_quality']['poor']:
                        air_score = 50
                    else:
                        air_score = 25
                
                # Sound level score
                sound_score = 100
                if sound_level and i < len(sound_level):
                    sl = sound_level[i]
                    if sl <= self.comfort_ranges['sound_level']['quiet']:
                        sound_score = 100
                    elif sl <= self.comfort_ranges['sound_level']['moderate']:
                        sound_score = 75
                    elif sl <= self.comfort_ranges['sound_level']['loud']:
                        sound_score = 50
                    else:
                        sound_score = 25
                
                # Overall comfort score (weighted average)
                weights = [0.3, 0.3, 0.25, 0.15]  # temp, humidity, air, sound
                scores = [temp_score, hum_score, air_score, sound_score]
                
                overall_score = sum(w * s for w, s in zip(weights, scores))
                comfort_scores.append(round(overall_score, 1))
                
                detailed_analysis.append({
                    'index': i,
                    'temperature_score': round(temp_score, 1),
                    'humidity_score': round(hum_score, 1),
                    'air_quality_score': round(air_score, 1),
                    'sound_level_score': round(sound_score, 1),
                    'overall_score': round(overall_score, 1),
                    'comfort_level': self._get_comfort_level(overall_score)
                })
            
            # Overall statistics
            avg_comfort = statistics.mean(comfort_scores)
            comfort_std = statistics.stdev(comfort_scores) if len(comfort_scores) > 1 else 0
            
            # Comfort distribution
            comfort_distribution = {
                'excellent': len([s for s in comfort_scores if s >= 80]),
                'good': len([s for s in comfort_scores if 60 <= s < 80]),
                'fair': len([s for s in comfort_scores if 40 <= s < 60]),
                'poor': len([s for s in comfort_scores if s < 40])
            }
            
            return {
                'average_comfort_score': round(avg_comfort, 1),
                'comfort_stability': round(100 - comfort_std, 1),
                'comfort_distribution': comfort_distribution,
                'comfort_trend': self.trend_analysis(comfort_scores)['trend_direction'],
                'detailed_analysis': detailed_analysis[-10:],  # Last 10 readings
                'recommendations': self._generate_comfort_recommendations(detailed_analysis[-1] if detailed_analysis else None)
            }
            
        except Exception as e:
            self.logger.error(f"Error in comfort analysis: {e}")
            return {'error': str(e)}
    
    def _get_comfort_level(self, score: float) -> str:
        """Convert comfort score to level"""
        if score >= 80:
            return 'excellent'
        elif score >= 60:
            return 'good'
        elif score >= 40:
            return 'fair'
        else:
            return 'poor'
    
    def _generate_comfort_recommendations(self, latest_analysis: Dict) -> List[str]:
        """Generate recommendations based on comfort analysis"""
        if not latest_analysis:
            return []
        
        recommendations = []
        
        if latest_analysis['temperature_score'] < 70:
            recommendations.append("Consider adjusting temperature for better comfort")
        
        if latest_analysis['humidity_score'] < 70:
            recommendations.append("Humidity levels could be optimized")
        
        if latest_analysis['air_quality_score'] < 70:
            recommendations.append("Consider improving air quality ventilation")
        
        if latest_analysis['sound_level_score'] < 70:
            recommendations.append("Sound levels may be affecting comfort")
        
        if not recommendations:
            recommendations.append("Current conditions are optimal for comfort")
        
        return recommendations
    
    def generate_report(self, hours: int = 24) -> Dict[str, Any]:
        """
        Generate comprehensive analytics report
        
        Args:
            hours: Number of hours to analyze
            
        Returns:
            Dictionary with comprehensive report
        """
        cache_key = self._get_cache_key('report', hours=hours)
        
        if self._is_cache_valid(cache_key):
            return self._get_cached_result(cache_key)
        
        try:
            # Get data from database
            if not self.db_manager:
                return {'error': 'Database manager not available'}
            
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            # Fetch sensor data
            data = self.db_manager.get_sensor_data_range(start_time, end_time)
            
            # Organize data by sensor type
            dht22_data = defaultdict(list)
            mq135_data = []
            dfr0026_data = []
            
            for reading in data:
                sensor_type = reading.get('sensor_type')
                sensor_id = reading.get('sensor_id')
                
                if sensor_type == 'DHT22':
                    if reading.get('temperature') is not None:
                        dht22_data[f'{sensor_id}_temp'].append(reading['temperature'])
                    if reading.get('humidity') is not None:
                        dht22_data[f'{sensor_id}_humidity'].append(reading['humidity'])
                
                elif sensor_type == 'MQ135':
                    if reading.get('processed_value') is not None:
                        mq135_data.append(reading['processed_value'])
                
                elif sensor_type == 'DFR0026':
                    if reading.get('processed_value') is not None:
                        dfr0026_data.append(reading['processed_value'])
            
            # Generate report
            report = {
                'report_period': {
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                    'duration_hours': hours
                },
                'data_summary': {
                    'total_readings': len(data),
                    'dht22_readings': len([r for r in data if r.get('sensor_type') == 'DHT22']),
                    'mq135_readings': len([r for r in data if r.get('sensor_type') == 'MQ135']),
                    'dfr0026_readings': len([r for r in data if r.get('sensor_type') == 'DFR0026'])
                },
                'temperature_analysis': {},
                'humidity_analysis': {},
                'air_quality_analysis': {},
                'sound_level_analysis': {},
                'correlation_analysis': {},
                'anomaly_analysis': {},
                'comfort_analysis': {},
                'trends': {}
            }
            
            # Temperature analysis
            all_temps = []
            for key, values in dht22_data.items():
                if key.endswith('_temp'):
                    all_temps.extend(values)
            
            if all_temps:
                report['temperature_analysis'] = {
                    'statistics': self.basic_statistics(all_temps),
                    'outliers': self.detect_outliers(all_temps),
                    'trend': self.trend_analysis(all_temps)
                }
            
            # Humidity analysis
            all_humidity = []
            for key, values in dht22_data.items():
                if key.endswith('_humidity'):
                    all_humidity.extend(values)
            
            if all_humidity:
                report['humidity_analysis'] = {
                    'statistics': self.basic_statistics(all_humidity),
                    'outliers': self.detect_outliers(all_humidity),
                    'trend': self.trend_analysis(all_humidity)
                }
            
            # Air quality analysis
            if mq135_data:
                report['air_quality_analysis'] = {
                    'statistics': self.basic_statistics(mq135_data),
                    'outliers': self.detect_outliers(mq135_data),
                    'trend': self.trend_analysis(mq135_data)
                }
            
            # Sound level analysis
            if dfr0026_data:
                report['sound_level_analysis'] = {
                    'statistics': self.basic_statistics(dfr0026_data),
                    'outliers': self.detect_outliers(dfr0026_data),
                    'trend': self.trend_analysis(dfr0026_data)
                }
            
            # Correlation analysis
            if all_temps and all_humidity:
                min_len = min(len(all_temps), len(all_humidity))
                report['correlation_analysis'] = {
                    'temp_humidity': self.correlation_analysis(all_temps[:min_len], all_humidity[:min_len])
                }
            
            # Anomaly analysis
            anomalies = {}
            if all_temps:
                anomalies['temperature'] = self.anomaly_detection(all_temps)
            if all_humidity:
                anomalies['humidity'] = self.anomaly_detection(all_humidity)
            if mq135_data:
                anomalies['air_quality'] = self.anomaly_detection(mq135_data)
            if dfr0026_data:
                anomalies['sound_level'] = self.anomaly_detection(dfr0026_data)
            
            report['anomaly_analysis'] = anomalies
            
            # Comfort analysis
            if all_temps and all_humidity:
                report['comfort_analysis'] = self.comfort_analysis(
                    all_temps, all_humidity, mq135_data, dfr0026_data
                )
            
            # Cache the result
            self._cache_result(cache_key, report)
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating report: {e}")
            return {'error': str(e)}
    
    def clear_cache(self):
        """Clear the calculation cache"""
        self.calculation_cache.clear()
        self.logger.info("Analytics cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'cache_size': len(self.calculation_cache),
            'cache_keys': list(self.calculation_cache.keys()),
            'cache_timeout': self.cache_timeout
        }
