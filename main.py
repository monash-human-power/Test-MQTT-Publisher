# Mock mqtt publisher for T2 Pit Subsystem
# Version: 1
# Author: some MHP software Scoot

import paho.mqtt.client as mqtt
import time
import random

# on successful connection, print it out
def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties):
    print(f"Connected with code {reason_code}")

# currently not used
def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    print(msg.topic + " " + str(msg.payload))

def on_publish(client: mqtt.Client, userdata, mid, reason_code, properties):
    # function that removed the message id when publishing
    try:
        userdata.remove(mid)
    except KeyError:
        print("Failed")

# helper function to make a client
def make_client(id: str, broker: str, port: int):
    mqttc = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, # type: ignore
        id
    )
    mqttc.on_publish = on_publish
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.connect(broker, port)
    mqttc.tls_set()
    return mqttc

def get_hhmmss() -> str:
    # returns a string that has the time time formatted properly
    return time.strftime("%H:%M:%S", time.localtime())

def send_message(mqttc: mqtt.Client, unacked_publish, topic, msg):
    # method to send the message to the mqtt broker
    randSpeed = round(random.random() * 60, 2)
    string = '''
{{
    "type": "telemetry",
    "timestamp": "2026-08-25T{0}.500Z",
    "sessionId": "mock-session-001",
    "data": {{
        "speed": {{ "value": {1}, "unit": "km/h" }},
        "cadence": {{ "value": 91, "unit": "rpm" }},
        "power": {{ "value": {2}, "unit": "W" }},
        "batteryVoltage": {{ "value": 48.2, "unit": "V" }},
        "gps": {{ "latitude": -37.9105, "longitude": 145.1362, "altitude": 35.2, "speed": 42.7 }}
    }}
}}\n'''.format(get_hhmmss(),
               # randomly change the speed and power
               randSpeed,
               round(randSpeed * 10 * (1+(random.random() - 0.5)/10)), 2)

    # publish and wait for acknowledgement
    msg = mqttc.publish(topic, string, qos=1)
    unacked_publish.add(msg.mid)
    time.sleep(0.5)
    msg.wait_for_publish()

def main():
    # get the input from the user
    broker = (lambda addr: addr if len(addr) > 0 else "localhost")(
        input("Enter Broker Adress: ").strip())
    try: 
        port = int(input("Enter Port: ").strip())
    except:
        port = 1883
        print("Failed defaulting to 1883")
    topic = (lambda topic : topic if len(topic) > 0 else "test/mqtt")(
        input("Enter Topic: ").strip())
    print(f"Topic set to: {topic}")
    id = f'python-mqtt-test1'

    # create and connect to broker
    mqttc = make_client(id, broker, port)

    unacked_publish = set()
    mqttc.user_data_set(unacked_publish)

    mqttc.loop_start()

    print("Starting to send data")

    while True: # keep sending data until user stops with keyboard interrupt
        try: 
            send_message(mqttc, unacked_publish, topic, str(port))
        except KeyboardInterrupt:
            break

    mqttc.loop_stop() # stop and disconnect from server

if __name__ == "__main__":
    main()