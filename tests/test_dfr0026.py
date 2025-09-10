#!/usr/bin/env python3
"""
DFR0026 Sound Sensor Test Script for Raspberry Pi
Tests only the DFR0026 sound sensor via MCP3008 ADC on CH1
Uses spidev library (pre-installed on Raspberry Pi OS)
"""

import spidev
import time

# Initialize SPI
spi = spidev.SpiDev()
spi.open(0, 0)  # Bus 0, Device 0 (CE0)
spi.max_speed_hz = 1000000  # 1MHz
spi.mode = 0

def read_adc(channel):
    """
    Read from MCP3008 ADC channel (0-7)
    Returns 10-bit value (0-1023)
    """
    if channel < 0 or channel > 7:
        return -1
    
    # MCP3008 command: start bit + single-ended + channel selection
    cmd = 0x18 | channel  # 0x18 = 0b00011000 (start bit + single-ended)
    
    # Send 3 bytes: [start], [cmd], [don't care]
    response = spi.xfer2([1, cmd << 4, 0])
    
    # Extract 10-bit result from response
    result = ((response[1] & 0x03) << 8) | response[2]
    return result

def voltage_from_adc(adc_value, vref=3.3):
    """Convert ADC value to voltage"""
    return (adc_value / 1023.0) * vref

def percentage_from_adc(adc_value):
    """Convert ADC value to percentage (0-100%)"""
    return (adc_value / 1023.0) * 100

def create_sound_bar(percentage, width=40):
    """Create a visual sound level bar"""
    filled = int((percentage / 100) * width)
    bar = '█' * filled + '░' * (width - filled)
    return f"[{bar}]"

def estimate_db(voltage):
    """Rough decibel estimate (not scientifically accurate)"""
    if voltage > 0.1:
        return 20 + (voltage / 3.3) * 60  # Rough 20-80 dB range
    else:
        return 20

def main():
    print("=== DFR0026 Sound Sensor Test ===")
    print("Reading from MCP3008 Channel 1")
    print("Make some noise to test the sensor!")
    print("Press Ctrl+C to stop")
    print()
    
    # Variables for tracking
    max_reading = 0
    min_reading = 100
    readings = []
    
    try:
        while True:
            # Read from channel 1 (where DFR0026 A0 is connected)
            adc_value = read_adc(1)
            voltage = voltage_from_adc(adc_value)
            percentage = percentage_from_adc(adc_value)
            db_estimate = estimate_db(voltage)
            
            # Update min/max
            if percentage > max_reading:
                max_reading = percentage
            if percentage < min_reading:
                min_reading = percentage
            
            # Keep last 10 readings for average
            readings.append(percentage)
            if len(readings) > 10:
                readings.pop(0)
            
            avg_reading = sum(readings) / len(readings)
            
            # Clear screen and display
            print("\033[2J\033[H")  # Clear screen
            
            print("=== DFR0026 Sound Sensor Test ===")
            print(f"Time: {time.strftime('%H:%M:%S')}")
            print("-" * 60)
            
            # Current reading
            print(f"Current Sound Level:")
            print(f"  Raw ADC Value: {adc_value:4d}")
            print(f"  Voltage: {voltage:.3f}V")
            print(f"  Percentage: {percentage:.1f}%")
            print(f"  Est. dB: {db_estimate:.1f} dB")
            print()
            
            # Visual bar
            print(f"Sound Level: {create_sound_bar(percentage)}")
            print(f"            0%{' ' * 34}100%")
            print()
            
            # Statistics
            print(f"Statistics:")
            print(f"  Current: {percentage:.1f}%")
            print(f"  Average: {avg_reading:.1f}%")
            print(f"  Maximum: {max_reading:.1f}%")
            print(f"  Minimum: {min_reading:.1f}%")
            print()
            
            # Sound level indicator
            if percentage < 10:
                status = "Very Quiet"
            elif percentage < 30:
                status = "Quiet"
            elif percentage < 50:
                status = "Moderate"
            elif percentage < 70:
                status = "Loud"
            elif percentage < 90:
                status = "Very Loud"
            else:
                status = "EXTREMELY LOUD"
            
            print(f"Status: {status}")
            print()
            print("Make some noise to test the sensor!")
            print("Press Ctrl+C to exit")
            
            time.sleep(0.1)  # Fast updates for sound
            
    except KeyboardInterrupt:
        print("\n" + "="*60)
        print("Final Statistics:")
        print(f"  Maximum sound level: {max_reading:.1f}%")
        print(f"  Minimum sound level: {min_reading:.1f}%")
        if readings:
            print(f"  Average sound level: {sum(readings)/len(readings):.1f}%")
        print("Test stopped by user")
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure:")
        print("- SPI is enabled (sudo raspi-config)")
        print("- MCP3008 is connected properly")
        print("- DFR0026 is connected to CH1 of MCP3008")
    finally:
        spi.close()
        print("SPI connection closed")

if __name__ == "__main__":
    main()
