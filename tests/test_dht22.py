import time
import board
import adafruit_dht

#initialize the dht device with GPIO pin
dht_sensors = {
    "DHT22_1": adafruit_dht.DHT22(board.D17), #GPIO17
    "DHT22_2": adafruit_dht.DHT22(board.D27), #GPIO27
    "DHT22_3": adafruit_dht.DHT22(board.D22), #GPIO22
}

while True:
    print(f"\n--- Reading at {time.strftime('%H:%M:%S')} ---")

    for sensor_name, dht_device in dht_sensors.items():
        try:
            temperature = dht_device.temperature
            humidity = dht_device.humidity
            print(f"{sensor_name}: Temp:{temperature:.1f}°C, Humidity: {humidity:.1f}%")
        except RunTimeError as error:
            print(f"{sensor_name}: {error.args[0]}")

    time.sleep(3.0)

