#!/usr/bin/env python3
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

def main():
    print("MQ-135 Air Quality Sensor Test")
    print("Reading from MCP3008 Channel 0")
    print("Press Ctrl+C to stop")
    print("-" * 40)
    
    try:
        while True:
            # Read from channel 0 (where MQ-135 A0 is connected)
            adc_value = read_adc(0)
            voltage = voltage_from_adc(adc_value)
            
            print(f"ADC Value: {adc_value:4d} | Voltage: {voltage:.3f}V | Raw: {adc_value/1023*100:.1f}%")
            
            time.sleep(0.5)  # Read every 500ms
            
    except KeyboardInterrupt:
        print("\nTest stopped by user")
    finally:
        spi.close()
        print("SPI connection closed")

if __name__ == "__main__":
    main()
