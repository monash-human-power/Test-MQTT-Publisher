# Mock mqtt publisher for T2 Pit Subsystem
# Version: 1
# Author: some MHP software Scoot

import paho.mqtt.client as mqtt
import time
import random
from datetime import datetime
import pandas as pd
import os

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
        pass

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

def csv_time_to_iso(csv_timestamp: str) -> str:
    dt = datetime.strptime(csv_timestamp, "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%Y-%m-%dT%H:%M:%S.500Z")

def send_message(mqttc: mqtt.Client, unacked_publish, topic, msg, rand=False, data=None, idx=0):
    # method to send the message to the mqtt broker
    randSpeed = round(random.random() * 60, 2)
    if rand or data is None:
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
        "gps": {{ "latitude": -38.12539467, "longitude": 145.31497998, "altitude": 35.2, "speed": 42.7 }}
    }}
}}\n'''.format(get_hhmmss(),
               # randomly change the speed and power
               randSpeed,
               round(randSpeed * 10 * (1+(random.random() - 0.5)/10)), 2)
    else:
        string = '''
{{
    "type": "telemetry",
    "timestamp": "{0}",
    "sessionId": "{1}",
    "data": {{
        "speed": {{ "value": {2}, "unit": "km/h" }},
        "cadence": {{ "value": {3}, "unit": "rpm" }},
        "power": {{ "value": {4}, "unit": "W" }},
        "batteryVoltage": {{ "value": 48.2, "unit": "V" }},
        "gps": {{ "latitude": {5}, "longitude": {6}, "altitude": {7}, "speed": {8} }}
    }}
}}\n'''.format(csv_time_to_iso(data["time"][idx]),
               topic.split('/')[1] if topic.count('/') >= 2 else "mock-session-001",
               (lambda s: 0 if s < 1 else s)(data["SPEED_mps"][idx] * 3.6),
               data["CADENCE"][idx],
               data["POWER"][idx],
               data["LATITUDE"][idx],
               data["LONGITUDE"][idx],
               data["ALTITUDE"][idx],
               (lambda s: 0 if s < 1 else s)(data["SPEED_mps_gps"][idx] * 3.6),
               )

    # publish and wait for acknowledgement
    msg = mqttc.publish(topic, string, qos=1)
    unacked_publish.add(msg.mid)
    time.sleep(0.1)
    msg.wait_for_publish()
    return idx + 1

def random_disconnect(mqttc: mqtt.Client, id: str, broker: str, port: int) -> mqtt.Client:
    rng = random.random()
    if rng > 1:
        mqttc.disconnect()
        print("disconnected")
        time.sleep(2 * random.random() + 1)
        c = make_client(id, broker, port)
        c.user_data_set(set())
        c.loop_start()
        print("reconnected")
        return c

    return mqttc


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
    print(f"Topic set to: '{topic}'")
    id = f'python-mqtt-test1'
    useRandom = (lambda r : True if r.strip().lower() == "y" else False)(
            input("Use Random Values (y/n)? ").strip())

    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'CaseyGPSData.csv')

    data = pd.read_csv(csv_path)
    idx = 880

    # create and connect to broker
    mqttc = make_client(id, broker, port)

    unacked_publish = set()
    mqttc.user_data_set(unacked_publish)

    mqttc.loop_start()

    print("Starting to send data")

    while True: # keep sending data until user stops with keyboard interrupt
        try: 
            idx = send_message(mqttc, 
                               unacked_publish, 
                               topic, 
                               str(port), 
                               rand=useRandom, 
                               data=data, 
                               idx=idx)
            mqttc = random_disconnect(mqttc, id, broker, port)
        except KeyboardInterrupt:
            break

    mqttc.loop_stop() # stop and disconnect from server

if __name__ == "__main__":
    main()