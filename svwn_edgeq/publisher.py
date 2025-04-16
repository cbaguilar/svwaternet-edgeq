# svwn_edgeq/publisher.py

# main tool for publishing queued messages.

from paho.mqtt.client import MQTT_ERR_SUCCESS
import sqlite3
from datetime import datetime
from typing import List, Tuple

MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24
WEEK = DAY * 7
MONTH = DAY * 31

class EdgeQueuePublisher:
    '''
    Main class, handles publishing
    '''

    '''
    Create instance of publisher.
    Needs:
    '''
    def __init__(self, host: str, port: int = 1883, username: str = None,
                 password: str = None, client_id: str = "",
                 keepalive: int = 60, db_path: str = "queue.db", retention: int = DAY):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client_id = client_id
        self.keepalive = keepalive
        self.db_path = db_path

        self.mq_client = mqtt.Client(client_id=self.client_id)
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish

        
        init_db(db_path)
    


    def push_data(data: str):
        """
        Queue new data for publication and try to send unpublished messages
        """
        self.store_message(str)
        self.publish_unpublished_messages()



    def init_db(db_path: str):
       """
       Initialize sqlite database
       """
       conn = sqlite3.connect(db_path)
       cur = conn.cursor()
       cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                published INTEGER DEFAULT 0
            );
                   """)
       conn.commit()
       conn.close()

    def store_message(payload: str):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        timestamp = datetime.utcnow().isoformat()
        cur.execute("""
        INSERT INTO messages (topic, payload, timestamp, published)
        VALUES (?, ?, ?, 0)
        """, (topic, payload, timestamp))
        conn.commit()
        conn.close()

    def get_unpublished_readings():
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, sensor_name, value, timestamp FROM readings WHERE published=0 ORDER BY id ASC")
        rows = cur.fetchall()
        conn.close()
        return rows

    def mark_as_published(row_id):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("UPDATE readings SET published=1 WHERE id=?", (row_id,))
        conn.commit()
        conn.close()

    def publish_unpublished_messages(self):
        """
        Fetch unpublished messages from SQLite, publish them with QoS=1,
        and mark them as published if successful.
        """
        unpublished = get_unpublished_messages(db_path=self.db_path)
        for msg_id, topic, payload, timestamp in unpublished:
            result = self.client.publish(topic, payload=payload, qos=1)
            result.wait_for_publish()
            if result.rc == MQTT_ERR_SUCCESS:
                mark_as_published(msg_id, db_path=db_path)
                print(f"Published message ID={msg_id} to topic={topic}")
            else:
                print(f"Failed to publish message ID={msg_id}, rc={result.rc}")
                # We won't mark as published. We'll retry next time.

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT broker successfully")
        else:
            print("Failed to connect")

