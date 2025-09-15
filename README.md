**Overview**


https://github.com/user-attachments/assets/ffc555f5-8176-477d-b462-379de1422a8d


This project takes in data from 5 different environment sensors and displays it to a webpage dashboard. 

**Bill of Materials**
- One breadboard
  
- 3 DHT22 sensors: https://www.digikey.com/en/products/detail/dfrobot/SEN0137/6588461?gclsrc=aw.ds&gad_source=1&gad_campaignid=20243136172&gbraid=0AAAAADrbLlivqw7rBRx3TrtIwlWxAmRBi&gclid=Cj0KCQjw8p7GBhCjARIsAEhghZ1LKgAP-nDGbN0IZ5i5GKEmS_bY3QUpFfaidb7iAF-ExCw1mFCyCfcaAgR2EALw_wcB
  
- 1 MQ135 sensor: https://www.digikey.com/en/products/detail/soldered-electronics/333111/21720373?gclsrc=aw.ds&gad_source=1&gad_campaignid=20243136172&gbraid=0AAAAADrbLlivqw7rBRx3TrtIwlWxAmRBi&gclid=Cj0KCQjw8p7GBhCjARIsAEhghZ3g0o39tyhEurUg59UlLHklZApP5gEEg00mZ0r6IrjWb-0FPIJ-WzoaAjrgEALw_wcB
  
- 1 DFR0026 sensor: https://www.digikey.com/en/products/detail/dfrobot/DFR0026/6588548?gclsrc=aw.ds&gad_source=1&gad_campaignid=20243136172&gbraid=0AAAAADrbLlivqw7rBRx3TrtIwlWxAmRBi&gclid=Cj0KCQjw8p7GBhCjARIsAEhghZ34u9Bmj-ohLwXOtut38IFQ4CXgrPPmTZTrz7jrMZ8LXzjy71-ctJ0aAi0SEALw_wcB
  
- 1 MCP3008 ADC: https://www.digikey.com/en/products/detail/microchip-technology/MCP3008-I-P/319422?gclsrc=aw.ds&gad_source=1&gad_campaignid=20228387720&gbraid=0AAAAADrbLliRw4e8OTlKe_XwZehV84MJQ&gclid=Cj0KCQjw8p7GBhCjARIsAEhghZ1GtBGFCpJdyKevvc7IMqAvP0vPQ5zRcHXPbN_7xFxUoTho36FxHbkaAqHBEALw_wcB
  
- 1 Raspberry Pi 4/ Raspberry Pi 4 Charger: https://vilros.com/products/raspberry-pi-4-model-b-1?variant=40809478914142&currency=USD&utm_medium=product_sync&utm_source=google&utm_content=sag_organic&utm_campaign=sag_organic&tw_source=google&tw_adid=&tw_campaign=19684058556&gad_source=1&gad_campaignid=19684058613&gbraid=0AAAAAD1QJAjxPRRK6tDoM9WFWvyH-8_GX&gclid=Cj0KCQjw8p7GBhCjARIsAEhghZ0-nIjpnkxZRFHumjP_g8NmVTYj2Gl2sETMMZJa8D94ale7ngWfI-4aAmFYEALw_wcB
  
- 1 MicroSD card: https://shop.sandisk.com/products/memory-cards/microsd-cards/sandisk-ultra-uhs-i-microsd?sku=SDSQUAC-256G-GN6MA&ef_id=Cj0KCQjw5onGBhDeARIsAFK6QJZq9oXigKwOKMaYQoy68BFhKIx-FvX3doK0AE07Pt_gKbEKBOpIPTEaAn0YEALw_wcB:G:s&s_kwcid=AL!15012!3!!!!x!!!21840826498!&utm_medium=pdsh2&utm_source=gads&utm_campaign=Google-B2C-Conversion-Pmax-NA-US-EN-Memory_Card-All-All-Brand&utm_content=&utm_term=SDSQUAC-256G-GN6MA&cp2=&gad_source=4&gad_campaignid=21836907008&gbraid=0AAAAA-HVYqnR4xOjgBaxD24l-IEJuHxfs&gclid=Cj0KCQjw5onGBhDeARIsAFK6QJZq9oXigKwOKMaYQoy68BFhKIx-FvX3doK0AE07Pt_gKbEKBOpIPTEaAn0YEALw_wcB
  
- Jumper Cables as Needed
  
- 3 x Pull up resistors (~5k-10k Ohms)
  
- 2 x 20k resistors
  
- 2 x 10k resistors
  

**Getting Started**
The 3 DHT22 sensors go into pins 17,22,and 27. Each of these sensors will need a pull down resistor between the data and power line. Both the 
MQ135 and DFR0026 sensors will need to connect to the pi via an ADC. In the code, the MQ135 is attached via channel 0 and the DFRR0026 is attached
via channel 1. The ADC will connect to the raspberry pi in the following format:

VDD -> 3.3V
VREF -> 3.3V
CLK -> SCLK (GPIO11)
DOUT -> MISO (GPIO9)
DIN -> MOSI (GPIO10)
CS -> CE0 (GPIO8)

Additionally, the the MQ135 and DFR0026 sensors have a 5V maximum output, while the MC3008 has a maximum voltage intake of 3.3V, so voltage
divider will be needed to adujst the voltage appropriately. Here I have a 20k/10k resistor combination to scale the 5V max to an appropriate 3.3V max,
but obviously any combination of R1, R2 in which R1/(R1 +R2) ~0.67 will work. Since the adc takes a maximum of 3.3V this also means that the MC3008 ADC
must be connected to a 3.3V rail, while the sensors must be connected to a 5V rail. However, they can obviously share a GND rail.
There are tests for each sensor in the test directory for a more modular setup.

**Software Architecture**
The layout is fairly self explanatory, with the fusion analytics in the data_fuusion directory, the html file in templates, and the database manager in the
database directory.

Run the app.py script to start the webpage, and access it via the local address via port 5000.
  
