#!/usr/bin/env python3
"""
DFR0026 Sound Level Sensor Reader Module
sensors/dfr0026_reader.py
"""

import time
import math
import logging
from datetime import datetime
from collections import deque
import busio
import digitalio
import board
import adafruit_mcp3xxx.mcp3008 as MCP
from adafruit_mcp3xxx.analog_in import AnalogIn

class DFR0026Reader:
    def __init__(self, sensor_config, mcp_config):
        """
        Initialize DFR0026 sound level sensor reader
        
        Args:
            sensor_config: DFR0026 sensor configuration dictionary
            mcp_config: MCP3008 ADC configuration dictionary
        """
        self.sensor_config = sensor_config
        self.mcp_config = mcp_config
        self.logger = logging.getLogger(__name__)
        
        # Sensor parameters
        self.channel = sensor_config['channel']
        self.vcc = sensor_config.get('vcc', 5.0)
        self.sample_rate = sensor_config.get('sample_rate', 100)  # Hz
        self.sample_duration = sensor_config.get('sample_duration', 0.1)  # seconds
        self.calibration_offset = sensor_config.get('calibration_offset', 0.0)
        
        # Sound level thresholds (dB estimates)
        self.thresholds = sensor_config.get('thresholds', {
            'quiet': 40.0,
            'normal': 60.0,
            'loud': 80.0,
            'very_loud': 100.0
        })
        
        # Calibration parameters
        self.db_reference = sensor_config.get('db_reference', 40.0)  # dB at reference voltage
        self.voltage_reference = sensor_config.get('voltage_reference', 1.0)  # V
        
        # Initialize MCP3008 ADC
        self._initialize_adc()
        
        # Data buffers
        self.sample_buffer = deque(maxlen=int(self.sample_rate * self.sample_duration))
        self.last_reading = None
        self.error_count = 0
        
        self.logger.info(f"DFR0026 initialized on channel {self.channel}")
    
    def _initialize_adc(self):
        """Initialize MCP3008 ADC connection"""
        try:
            # Create SPI bus
            spi = busio.SPI(clock=board.SCK, MISO=board.MISO, MOSI=board.MOSI)
            
            # Create CS (chip select)
            cs = digitalio.DigitalInOut(getattr(board, f"D{self.mcp_config['cs_pin']}"))
            
            # Create MCP object
            self.mcp = MCP.MCP3008(spi, cs)
            
            # Create an analog input channel
            self.chan = AnalogIn(self.mcp, getattr(MCP, f"P{self.channel}"))
            
            self.logger.info(f"MCP3008 initialized on CS pin {self.mcp_config['cs_pin']}")
            
        except Exception as e:
            self.logger.error(f"Error initializing MCP3008: {e}")
            raise
    
    def read_raw(self):
        """
        Read raw ADC value from DFR0026 sensor
        
        Returns:
            dict: Raw reading result
        """
        try:
            # Read ADC value (0-65535 for 16-bit)
            adc_value = self.chan.value
            
            # Convert to voltage (0-3.3V reference)
            voltage = self.chan.voltage
            
            return {
                'success': True,
                'adc_value': adc_value,
                'voltage': voltage,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error reading DFR0026 raw data: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _collect_samples(self):
        """
        Collect multiple samples for sound level calculation
        
        Returns:
            list: List of voltage samples
        """
        samples = []
        sample_interval = 1.0 / self.sample_rate
        
        try:
            for _ in range(int(self.sample_rate * self.sample_duration)):
                raw_data = self.read_raw()
                
                if raw_data['success']:
                    samples.append(raw_data['voltage'])
                
                time.sleep(sample_interval)
            
            return samples
            
        except Exception as e:
            self.logger.error(f"Error collecting samples: {e}")
            return []
    
    def _calculate_rms(self, samples):
        """
        Calculate RMS (Root Mean Square) value of samples
        
        Args:
            samples: List of voltage samples
            
        Returns:
            float: RMS value
        """
        if not samples:
            return 0.0
        
        try:
            # Calculate mean
            mean = sum(samples) / len(samples)
            
            # Calculate RMS
            squared_diffs = [(sample - mean) ** 2 for sample in samples]
            rms = math.sqrt(sum(squared_diffs) / len(squared_diffs))
            
            return rms
            
        except Exception as e:
            self.logger.error(f"Error calculating RMS: {e}")
            return 0.0
    
    def _calculate_sound_level(self, rms_voltage):
        """
        Calculate sound level from RMS voltage
        
        Args:
            rms_voltage: RMS voltage value
            
        Returns:
            dict: Sound level information
        """
        if rms_voltage <= 0:
            return {
                'level': 'silent',
                'db_estimate': 0.0,
                'description': 'No sound detected'
            }
        
        try:
            # Convert RMS voltage to relative sound level (0-100 scale)
            # This is a simplified calculation - real sound level meters use complex algorithms
            max_voltage = 3.3  # Max ADC voltage
            sound_level = (rms_voltage / max_voltage) * 100
            
            # Estimate dB level using logarithmic relationship
            # This is a rough approximation and should be calibrated
            if rms_voltage > 0:
                db_estimate = self.db_reference + (20 * math.log10(rms_voltage / self.voltage_reference))
                db_estimate += self.calibration_offset
            else:
                db_estimate = 0.0
            
            # Ensure reasonable bounds
            db_estimate = max(0.0, min(120.0, db_estimate))
            
            # Determine sound level category
            if db_estimate <= self.thresholds['quiet']:
                level = 'quiet'
                description = 'Quiet environment'
            elif db_estimate <= self.thresholds['normal']:
                level = 'normal'
                description = 'Normal sound level'
            elif db_estimate <= self.thresholds['loud']:
                level = 'loud'
                description = 'Loud environment'
            else:
                level = 'very_loud'
                description = 'Very loud environment'
            
            return {
                'level': level,
                'db_estimate': round(db_estimate, 1),
                'description': description
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating sound level: {e}")
            return {
                'level': 'error',
                'db_estimate': 0.0,
                'description': 'Calculation error'
            }
    
    def read(self):
        """
        Read DFR0026 sensor and return processed data
        
        Returns:
            dict: Complete sensor reading with sound level analysis
        """
        try:
            # Collect samples
            samples = self._collect_samples()
            
            if not samples:
                self.error_count += 1
                return {
                    'success': False,
                    'error': 'No samples collected',
                    'sensor_id': 'dfr0026',
                    'error_count': self.error_count,
                    'timestamp': datetime.now().isoformat()
                }
            
            # Calculate statistics
            avg_voltage = sum(samples) / len(samples)
            min_voltage = min(samples)
            max_voltage = max(samples)
            rms_voltage = self._calculate_rms(samples)
            
            # Calculate sound level
            sound_analysis = self._calculate_sound_level(rms_voltage)
            
            # Get latest raw ADC value for reference
            latest_raw = self.read_raw()
            adc_value = latest_raw['adc_value'] if latest_raw['success'] else 0
            
            # Reset error count on successful read
            self.error_count = 0
            
            reading = {
                'success': True,
                'sensor_id': 'dfr0026',
                'adc_value': adc_value,
                'voltage': round(avg_voltage, 3),
                'voltage_stats': {
                    'min': round(min_voltage, 3),
                    'max': round(max_voltage, 3),
                    'avg': round(avg_voltage, 3),
                    'rms': round(rms_voltage, 3)
                },
                'sound_level': sound_analysis['level'],
                'db_estimate': sound_analysis['db_estimate'],
                'description': sound_analysis['description'],
                'samples_count': len(samples),
                'timestamp': datetime.now().isoformat()
            }
            
            self.last_reading = reading
            return reading
            
        except Exception as e:
            self.logger.error(f"Error reading DFR0026: {e}")
            self.error_count += 1
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'sensor_id': 'dfr0026',
                'error_count': self.error_count,
                'timestamp': datetime.now().isoformat()
            }
    
    def calibrate(self, calibration_data):
        """
        Calibrate the sensor with known sound levels
        
        Args:
            calibration_data: Dictionary with calibration parameters
            
        Returns:
            dict: Calibration result
        """
        try:
            # Update calibration parameters
            if 'db_reference' in calibration_data:
                self.db_reference = calibration_data['db_reference']
            
            if 'voltage_reference' in calibration_data:
                self.voltage_reference = calibration_data['voltage_reference']
            
            if 'calibration_offset' in calibration_data:
                self.calibration_offset = calibration_data['calibration_offset']
            
            self.logger.info(f"DFR0026 calibration updated: dB_ref={self.db_reference}, V_ref={self.voltage_reference}, offset={self.calibration_offset}")
            
            return {
                'success': True,
                'db_reference': self.db_reference,
                'voltage_reference': self.voltage_reference,
                'calibration_offset': self.calibration_offset,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error calibrating DFR0026: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_status(self):
        """
        Get sensor status information
        
        Returns:
            dict: Sensor status
        """
        return {
            'sensor_id': 'dfr0026',
            'channel': self.channel,
            'sample_rate': self.sample_rate,
            'sample_duration': self.sample_duration,
            'calibration_offset': self.calibration_offset,
            'error_count': self.error_count,
            'last_reading_time': self.last_reading.get('timestamp') if self.last_reading else None,
            'status': 'online' if self.error_count < 5 else 'error'
        }
    
    def get_peak_detection(self, threshold_multiplier=2.0):
        """
        Detect sound peaks above background level
        
        Args:
            threshold_multiplier: Multiplier for peak detection threshold
            
        Returns:
            dict: Peak detection information
        """
        try:
            samples = self._collect_samples()
            
            if not samples:
                return {
                    'success': False,
                    'error': 'No samples for peak detection'
                }
            
            avg_level = sum(samples) / len(samples)
            threshold = avg_level * threshold_multiplier
            
            peaks = [sample for sample in samples if sample > threshold]
            peak_count = len(peaks)
            max_peak = max(peaks) if peaks else 0
            
            return {
                'success': True,
                'peak_count': peak_count,
                'max_peak': round(max_peak, 3),
                'threshold': round(threshold, 3),
                'avg_level': round(avg_level, 3),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error in peak detection: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def close(self):
        """Clean up resources"""
        try:
            # MCP3008 cleanup
            if hasattr(self, 'mcp'):
                self.mcp = None
            
            self.logger.info("DFR0026 reader closed")
            
        except Exception as e:
            self.logger.error(f"Error closing DFR0026 reader: {e}")
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        self.close()
