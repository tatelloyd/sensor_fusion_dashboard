from flask import Flask, jsonify
import spidev
import time
import random
import RPi.GPIO as GPIO

app = Flask(__name__)

# Setup GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# DHT22 pins
DHT22_PINS = [17, 27, 22]

def read_dht22_data(pin):
    """
    Read DHT22 sensor data using bit-banging approach
    Returns (humidity, temperature) or (None, None) on error
    """
    try:
        # Setup pin as output and send start signal
        GPIO.setup(pin, GPIO.OUT)
        GPIO.output(pin, GPIO.LOW)
        time.sleep(0.02)  # 20ms low signal
        GPIO.output(pin, GPIO.HIGH)
        time.sleep(0.00004)  # 40us high signal
        
        # Switch to input and read response
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        
        # Wait for DHT22 to pull low (start of transmission)
        timeout = time.time() + 0.1
        while GPIO.input(pin) == GPIO.HIGH:
            if time.time() > timeout:
                return None, None
        
        # Wait for DHT22 to pull high (ready to send data)
        timeout = time.time() + 0.1
        while GPIO.input(pin) == GPIO.LOW:
            if time.time() > timeout:
                return None, None
        
        # Read 40 bits of data
        bits = []
        for i in range(40):
            # Wait for low signal
            timeout = time.time() + 0.1
            while GPIO.input(pin) == GPIO.HIGH:
                if time.time() > timeout:
                    return None, None
            
            # Wait for high signal and measure duration
            timeout = time.time() + 0.1
            while GPIO.input(pin) == GPIO.LOW:
                if time.time() > timeout:
                    return None, None
            
            start_time = time.time()
            timeout = time.time() + 0.1
            while GPIO.input(pin) == GPIO.HIGH:
                if time.time() > timeout:
                    return None, None
            
            # If high signal was long, it's a 1, otherwise 0
            if (time.time() - start_time) > 0.00005:  # 50us threshold
                bits.append(1)
            else:
                bits.append(0)
        
        # Convert bits to bytes
        bytes_data = []
        for i in range(0, 40, 8):
            byte = 0
            for j in range(8):
                byte = (byte << 1) | bits[i + j]
            bytes_data.append(byte)
        
        # Verify checksum
        checksum = (bytes_data[0] + bytes_data[1] + bytes_data[2] + bytes_data[3]) & 0xFF
        if checksum != bytes_data[4]:
            return None, None
        
        # Calculate humidity and temperature
        humidity = ((bytes_data[0] << 8) | bytes_data[1]) / 10.0
        temperature = (((bytes_data[2] & 0x7F) << 8) | bytes_data[3]) / 10.0
        if bytes_data[2] & 0x80:
            temperature = -temperature
        
        return humidity, temperature
        
    except Exception as e:
        print(f"DHT22 read error on pin {pin}: {e}")
        return None, None

def read_adc(channel):
    if channel < 0 or channel > 7:
        return -1
    try:
        spi = spidev.SpiDev()
        spi.open(0,0)
        spi.max_speed_hz = 1000000
        spi.mode = 0

        cmd = 0x18|channel
        response = spi.xfer2([1, cmd << 4, 0])
        result=((response[1] & 0x03) << 8) | response[2]
        spi.close()
        return result
    except Exception as e:
        print(f"ADC read error: {e}")
        return -1

def voltage_from_adc(adc_value, vref=3.3):
   return (adc_value/1023.0)*vref

def estimate_db(voltage):
    if voltage > 0.1:
        return 20 + (voltage/3.3)*60
    else:
        return 20

def get_humidity():
    """Read humidity from DHT22 sensors and return average"""
    humidity_readings = []
    
    for pin in DHT22_PINS:
        try:
            humidity, temperature = read_dht22_data(pin)
            if humidity is not None:
                humidity_readings.append(humidity)
                print(f"DHT22 pin {pin}: Humidity = {humidity:.1f}%, Temperature = {temperature:.1f}°C")
            else:
                print(f"DHT22 pin {pin}: No reading")
        except Exception as e:
            print(f"DHT22 pin {pin}: Error - {e}")
        
        time.sleep(0.1)  # Small delay between readings
    
    if humidity_readings:
        average_humidity = sum(humidity_readings) / len(humidity_readings)
        print(f"Average humidity: {average_humidity:.1f}%")
        return average_humidity
    else:
        print("No valid humidity readings, using simulated data")
        return random.uniform(45, 55)

def get_air_quality():
    try:
        adc_value = read_adc(0)
        if adc_value > 0:
            voltage = voltage_from_adc(adc_value)
            return voltage
        else:
            return random.uniform(40, 50)
    except Exception as e:
        print(f"Air quality sensor error: {e}")
        return random.uniform(40, 50)

def get_noise():
    try:
        adc_value = read_adc(1)
        if adc_value > 0:
            voltage = voltage_from_adc(adc_value)
            db_estimate = estimate_db(voltage)
            return db_estimate
        else:
            return random.uniform(60, 70)
    except Exception as e:
        print(f"Noise sensor error: {e}")
        return random.uniform(60, 70)

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/get_sensor_data')
def get_sensor_data():
    return jsonify({
        'sensor1': round(get_humidity(), 2),
        'sensor2': round(get_air_quality(), 2),
        'sensor3': round(get_noise(), 2)
    })

if __name__ == '__main__':
    print("Starting Flask app with hardware DHT22 implementation...")
    print("Testing DHT22 sensors...")
    
    # Test each sensor once at startup
    for pin in DHT22_PINS:
        humidity, temperature = read_dht22_data(pin)
        if humidity is not None:
            print(f"DHT22 pin {pin}: Working - {humidity:.1f}% humidity")
        else:
            print(f"DHT22 pin {pin}: Not responding")
    
    print("Starting Flask server...")
    app.run(host='0.0.0.0', port=5000, debug=True)
