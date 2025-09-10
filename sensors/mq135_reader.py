#!/usr/bin/env python3
"""
MQ-135 Air Quality Sensor Reader Module
sensors/mq135_reader.py
"""

import time
import logging
from datetime import datetime
import busio
import digitalio
import board
import adafruit_mcp3xxx.mcp3008 as MCP
from adafruit_mcp3xxx.analog_in import AnalogIn

class MQ135Reader:
    def __init__(self, sensor_config, mcp_config):
        """
        Initialize MQ-135 sensor reader
        
        Args:
            sensor_config: MQ-135 sensor configuration dictionary
            mcp_config: MCP3008 ADC configuration dictionary
        """
        self.sensor_config = sensor_config
        self.mcp_config = mcp_config
        self.logger = logging.getLogger(__name__)
        
        # Sensor parameters
        self.channel = sensor_config['channel']
        self.vcc = sensor_config.get('vcc', 5.0)
        self.load_resistance = sensor_config.get('load_resistance', 10000)  # 10kΩ
        self.calibration_factor = sensor_config.get('calibration_factor', 1.0)
        
        # Air quality thresholds (ppm equivalent estimates)
        self.thresholds = sensor_config.get('thresholds', {
            'excellent': 0.0,
            'good': 50.0,
            'moderate': 100.0,
            'poor': 200.0,
            'very_poor': 300.0
        })
        
        # Initialize MCP3008 ADC
        self._initialize_adc()
        
        # Calibration data
        self.r0 = sensor_config.get('r0', None)  # Clean air resistance
        self.last_reading = None
        self.error_count = 0
        
        self.logger.info(f"MQ-135 initialized on channel {self.channel}")
    
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
        Read raw ADC value from MQ-135 sensor
        
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
            self.logger.error(f"Error reading MQ-135 raw data: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _calculate_resistance(self, voltage):
        """
        Calculate sensor resistance from voltage reading
        
        Args:
            voltage: Voltage reading from ADC
            
        Returns:
            float: Sensor resistance in ohms
        """
        if voltage <= 0:
            return float('inf')
        
        # Rs = (Vcc - Vout) * RL / Vout
        # Where Vcc = supply voltage, Vout = measured voltage, RL = load resistance
        try:
            resistance = ((self.vcc - voltage) * self.load_resistance) / voltage
            return resistance
        except (ZeroDivisionError, ValueError):
            return float('inf')
    
    def _calculate_ratio(self, resistance):
        """
        Calculate Rs/R0 ratio for air quality estimation
        
        Args:
            resistance: Current sensor resistance
            
        Returns:
            float: Rs/R0 ratio
        """
        if self.r0 is None or self.r0 <= 0:
            return None
        
        return resistance / self.r0
    
    def _estimate_air_quality(self, ratio):
        """
        Estimate air quality based on Rs/R0 ratio
        
        Args:
            ratio: Rs/R0 ratio
            
        Returns:
            dict: Air quality estimation
        """
        if ratio is None:
            return {
                'level': 'unknown',
                'ppm_estimate': None,
                'description': 'Sensor not calibrated'
            }
        
        # Rough estimation based on MQ-135 characteristics
        # These are approximate values and should be calibrated for specific conditions
        if ratio < 0.5:
            ppm_estimate = ratio * 100  # Very rough estimate
        elif ratio < 1.0:
            ppm_estimate = 50 + (ratio - 0.5) * 100
        elif ratio < 2.0:
            ppm_estimate = 100 + (ratio - 1.0) * 200
        else:
            ppm_estimate = 300 + (ratio - 2.0) * 100
        
        # Apply calibration factor
        ppm_estimate *= self.calibration_factor
        
        # Determine air quality level
        if ppm_estimate <= self.thresholds['good']:
            level = 'excellent' if ppm_estimate <= self.thresholds['excellent'] else 'good'
            description = 'Air quality is excellent' if level == 'excellent' else 'Air quality is good'
        elif ppm_estimate <= self.thresholds['moderate']:
            level = 'moderate'
            description = 'Air quality is moderate'
        elif ppm_estimate <= self.thresholds['poor']:
            level = 'poor'
            description = 'Air quality is poor'
        else:
            level = 'very_poor'
            description = 'Air quality is very poor'
        
        return {
            'level': level,
            'ppm_estimate': round(ppm_estimate, 1),
            'description': description
        }
    
    def read(self):
        """
        Read MQ-135 sensor and return processed data
        
        Returns:
            dict: Complete sensor reading with air quality estimation
        """
        try:
            # Get raw reading
            raw_data = self.read_raw()
            
            if not raw_data['success']:
                self.error_count += 1
                return {
                    'success': False,
                    'error': raw_data['error'],
                    'sensor_id': 'mq135',
                    'error_count': self.error_count,
                    'timestamp': datetime.now().isoformat()
                }
            
            # Calculate resistance
            resistance = self._calculate_resistance(raw_data['voltage'])
            
            # Calculate ratio (if calibrated)
            ratio = self._calculate_ratio(resistance)
            
            # Estimate air quality
            air_quality = self._estimate_air_quality(ratio)
            
            # Reset error count on successful read
            self.error_count = 0
            
            reading = {
                'success': True,
                'sensor_id': 'mq135',
                'adc_value': raw_data['adc_value'],
                'voltage': round(raw_data['voltage'], 3),
                'resistance': round(resistance, 2) if resistance != float('inf') else None,
                'ratio': round(ratio, 3) if ratio is not None else None,
                'air_quality': air_quality,
                'calibrated': self.r0 is not None,
                'timestamp': datetime.now().isoformat()
            }
            
            self.last_reading = reading
            return reading
            
        except Exception as e:
            self.logger.error(f"Error reading MQ-135: {e}")
            self.error_count += 1
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'sensor_id': 'mq135',
                'error_count': self.error_count,
                'timestamp': datetime.now().isoformat()
            }
    
    def calibrate(self, clean_air_samples=50):
        """
        Calibrate the sensor in clean air conditions
        
        Args:
            clean_air_samples: Number of samples to take for calibration
            
        Returns:
            dict: Calibration result
        """
        try:
            self.logger.info(f"Starting MQ-135 calibration with {clean_air_samples} samples")
            
            resistances = []
            successful_reads = 0
            
            for i in range(clean_air_samples):
                raw_data = self.read_raw()
                
                if raw_data['success']:
                    resistance = self._calculate_resistance(raw_data['voltage'])
                    if resistance != float('inf'):
                        resistances.append(resistance)
                        successful_reads += 1
                
                time.sleep(0.1)  # Small delay between readings
            
            if successful_reads < clean_air_samples * 0.8:  # Need at least 80% successful reads
                return {
                    'success': False,
                    'error': f'Insufficient successful readings: {successful_reads}/{clean_air_samples}',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Calculate average resistance in clean air (R0)
            self.r0 = sum(resistances) / len(resistances)
            
            self.logger.info(f"MQ-135 calibration completed. R0 = {self.r0:.2f} ohms")
            
            return {
                'success': True,
                'r0': round(self.r0, 2),
                'samples_used': len(resistances),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error calibrating MQ-135: {e}")
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
            'sensor_id': 'mq135',
            'channel': self.channel,
            'calibrated': self.r0 is not None,
            'r0': self.r0,
            'error_count': self.error_count,
            'last_reading_time': self.last_reading.get('timestamp') if self.last_reading else None,
            'status': 'online' if self.error_count < 5 else 'error'
        }
    
    def reset_calibration(self):
        """Reset calibration data"""
        self.r0 = None
        self.logger.info("MQ-135 calibration reset")
    
    def close(self):
        """Clean up resources"""
        try:
            # MCP3008 cleanup
            if hasattr(self, 'mcp'):
                self.mcp = None
            
            self.logger.info("MQ-135 reader closed")
            
        except Exception as e:
            self.logger.error(f"Error closing MQ-135 reader: {e}")
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        self.close()
