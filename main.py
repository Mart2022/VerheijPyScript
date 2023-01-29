import paho.mqtt.client as mqtt
import pypyodbc
import threading
import logging
import datetime
import json
import time
from logging.handlers import TimedRotatingFileHandler

# Lees de database- en MQTT-configuratie uit JSON-bestanden
with open("database_config.txt") as f:
    database_config = json.load(f)["database"]
with open("mqtt_config.txt") as f:
    mqtt_config = json.load(f)

# Stel de logging-configuratie in en maak een RotatingFileHandler aan met een maximum van 100KB en maximaal 1 rotatie
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = TimedRotatingFileHandler('MysqlLogfile.log', when='MIDNIGHT', interval=1, backupCount=5)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(handler)

# Maak verbinding met de MQTT-broker
client = mqtt.Client()
try:
    client.connect(mqtt_config["mqtt"]["host"], mqtt_config["mqtt"]["port"])
except Exception as e:
    logging.error("Kon geen verbinding maken met de MQTT-broker: %s", e)

# Maak een dictionary om de timers bij te houden
timers = {}

#maakt een functie een verbinding met de SQL Server-database
def create_db_connection():

    max_retries = 11
    retries = 1
    while retries < max_retries:
        print(f"User={database_config['user']};")
        try:
            cnxn = pypyodbc.connect(
                f"Driver= {database_config['driver']};"
                f"Server={database_config['server']};"
                f"Database={database_config['database']};"
                f"Trusted_Connection={database_config['trusted_connection']};"
                f"Uid={database_config['username']};"
                f"Pwd={database_config['password']};"
            )
            return cnxn
        except Exception as e:
            logging.error(f"Kon na {retries} pogingen geen verbinding maken met de SQL Server-database: {e}")
            retries += 1  # Increment the retry counter
            time.sleep(5)  # Wait 5 seconds before trying again

    if retries == max_retries:
        logging.error("Maximaal aantal verbindingpogingen overschreden.")
        exit()


#Test de verbinding met de database
DatabaseTest = create_db_connection()
DatabaseTest.close()

#maakt een functie die wordt aangeroepen als er een bericht binnenkomt
def on_message(client, userdata, message):
    # Haal de data van het bericht op uit topic
    cnxn = create_db_connection()
    if (cnxn is None):
        return

    payload = message.payload.decode("utf-8")
    logging.info(f"Received `{message.payload.decode()}` from `{message.topic}` topic")
    timestamp = datetime.datetime.now()
    date = datetime.date.today()
    time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-2]
    # Voeg de data toe aan de SQL Server-database
    device = getTopic(message.topic.encode())
    Create_Tables(device)

    insert_data(cnxn, device.decode('utf-8'), payload, timestamp, date, time)


# Maak een functie die wordt aangeroepen als er een timer verstrijkt

def on_timeout(device):
    # Haal de data op uit de timer en stop de timer
    timer = timers[device]
    timer.cancel()
    del timers[device]

    # Voeg de data toe aan de SQL Server-database
    cnxn = create_db_connection()
    if (cnxn is None):
        return

    payload = "timeout"
    logging.info(f"Received timeout message from `{device}`")
    timestamp = datetime.datetime.now()
    date = datetime.date.today()
    time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-2]

    insert_data(cnxn, device, payload, timestamp, date, time)
#Maak een functie die wordt aangeroepen als er een insert wordt gemaakt.
def insert_data(cnxn, device, payload, timestamp, date, time):
    #Voegt data toe aan een SQL Server-database
    try:
        cursor = cnxn.cursor()
        query = "INSERT INTO " + device + "(TS,date,time,device,message) VALUES (?, ?, ?,?,?)"
        cursor.execute(query, (timestamp, date, time, device, payload))
        cnxn.commit()
        logging.info(f"Successfully inserted `{payload}` into `{device}` table")
    except Exception as e:
        logging.error("Failed to insert data into database: %s", e)

# Maak een functie die wordt aangeroepen als er een verbinding wordt gemaakt
def on_connect(client, userdata, flags, rc):
    # Abonneer op alle topics
    client.subscribe(mqtt_config["mqtt"]["topic"])

# Maak een functie die wordt aangeroepen als er een verbinding verbroken wordt
def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning("Verbinding verbroken met foutcode %s", rc)

# Maak een functie die de device-naam uit het topic haalt
def getTopic(topic):
    return topic.split("/")[1]

# Maak een functie die tabellen maakt voor elk device
def Create_Tables(device):
    cnxn = create_db_connection()
    cursor = cnxn.cursor()
    table_name = device.decode('utf-8')
    query = "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '" + table_name + "') CREATE TABLE " + table_name + " (TS DATETIME, date DATE, time TIME, device VARCHAR(255), message VARCHAR(MAX));"
    cursor.execute(query)
    cnxn.commit()
    cursor.close()

# Zet de event handlers
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Start de MQTT-client
client.loop_forever()