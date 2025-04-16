import paho.mqtt.client as mqtt
import sqlite3
from datetime import datetime, timedelta
from typing import List, Tuple
import time
import threading

MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24
WEEK = DAY * 7
MONTH = DAY * 31

class EdgeQueuePublisher:
    '''
    Main class, handles publishing queued messages.
    '''

    def __init__(self,
                 host: str,
                 port: int = 1883,
                 username: str = None,
                 password: str = None,
                 client_id: str = "",
                 keepalive: int = 60,
                 db_path: str = "queue.db",
                 retention: int = None,
                 reconnect_min_delay: int = 1,
                 reconnect_max_delay: int = 120):
        """
        :param host: MQTT broker host
        :param port: MQTT broker port
        :param username: Username for authentication
        :param password: Password for authentication
        :param client_id: MQTT client ID
        :param keepalive: Keepalive in seconds
        :param db_path: Path to local SQLite DB
        :param retention: Retention window in seconds; if not None, old messages are removed
        :param reconnect_min_delay: Minimum delay for automatic reconnect
        :param reconnect_max_delay: Maximum delay for automatic reconnect
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client_id = client_id
        self.keepalive = keepalive
        self.db_path = db_path
        self.retention = retention

        # Initialize MQTT client
        self.mq_client = mqtt.Client(client_id=self.client_id)
        if self.username and self.password:
            self.mq_client.username_pw_set(self.username, self.password)

        # Set up callbacks
        self.mq_client.on_connect = self.on_connect
        self.mq_client.on_disconnect = self.on_disconnect
        self.mq_client.on_publish = self.on_publish

        # Configure reconnect behavior
        # This tells paho-mqtt to automatically retry connecting with exponential backoff
        self.mq_client.reconnect_delay_set(min_delay=reconnect_min_delay,
                                           max_delay=reconnect_max_delay)

        try:
            self.start_mqtt_client()
        except Exception as e:
            print("Could not start MQTT client, queuing locally.", e)

    
        # Initialize the local DB to store messages
        self.init_db(self.db_path)

        # Start a thread to periodically publish any remaining queued messages
        self.publisher_thread = threading.Thread(
            target=self._publisher_loop, daemon=True
        )
        self.publisher_thread.start()

    def start_mqtt_client(self):
        # Connect to broker (async)
        self.mq_client.connect(self.host, self.port, keepalive=self.keepalive)

        # Start the network loop in a background thread
        self.mq_client.loop_start()


    def push_data(self, topic: str, payload: str):
        """
        Queue new data for publication and trigger a publish attempt.
        """
        self.store_message(topic, payload)
        # We'll just queue it and let our publisher loop handle sending it
        if self.retention is not None:
            self.clean_old_messages()

    def _publisher_loop(self, interval=5):
        """
        Background loop that tries to publish unpublished messages every few seconds.
        """
        while True:
            
            if self.mq_client.is_connected():
                self.publish_unpublished_messages()
            else:
                try:
                    self.start_mqtt_client()
                except Exception as e:
                    print("Could not connect to broker, still queueing.", e)
            time.sleep(interval)

    def init_db(self, db_path: str):
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

    def store_message(self, topic: str, payload: str):
        """
        Insert a new message into the queue (SQLite).
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        timestamp = datetime.utcnow().isoformat()
        cur.execute("""
            INSERT INTO messages (topic, payload, timestamp, published)
            VALUES (?, ?, ?, 0)
        """, (topic, payload, timestamp))
        conn.commit()
        conn.close()

    def get_all_messages(self):
        """
        Get all messages in the SQLITE queue
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM messages ORDER BY id ASC
            """)
        rows = cur.fetchall()
        conn.close()
        return rows

    def get_unpublished_messages(self):
        """
        Retrieve all unpublished messages (published=0) from SQLite.
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT id, topic, payload, timestamp
            FROM messages
            WHERE published=0
            ORDER BY id ASC
        """)
        rows = cur.fetchall()
        conn.close()
        return rows

    def mark_as_published(self, row_id: int):
        """
        Mark a message as published (published=1).
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("UPDATE messages SET published=1 WHERE id=?", (row_id,))
        conn.commit()
        conn.close()

    def clean_old_messages(self):
        """
        Clean up messages older than the retention window
        """
        lookback_window = (datetime.utcnow() - timedelta(seconds=self.retention)).isoformat()
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("DELETE FROM messages WHERE timestamp < ?", (lookback_window,))
        conn.commit()
        conn.close()

    def publish_unpublished_messages(self):
        """
        Fetch unpublished messages from SQLite and attempt to publish them with QoS=1.
        If successful, mark them as published.
        """
        unpublished = self.get_unpublished_messages()
        if not unpublished:
            return

        for msg_id, topic, payload, timestamp in unpublished:
            # If not connected, skip for now (loop will retry later).
            if not self.mq_client.is_connected():
                break

            result = self.mq_client.publish(topic, payload=payload, qos=1, retain=True)
            # Synchronously wait for the publish to be sent
            result.wait_for_publish()
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.mark_as_published(msg_id)
                print(f"Published message ID={msg_id} to topic={topic}")
            else:
                print(f"Failed to publish message ID={msg_id}, rc={result.rc}")
                # We'll try again in the next loop iteration
                break

    def on_connect(self, client, userdata, flags, rc):
        """
        Callback when client connects to the MQTT broker.
        """
        if rc == 0:
            print("Connected to MQTT broker successfully.")
        else:
            print(f"Failed to connect. Return code={rc}")

    def on_disconnect(self, client, userdata, rc):
        """
        Callback when the client disconnects from the MQTT broker.
        """
        if rc != 0:
            print(f"Unexpected disconnection (rc={rc}). Paho will try to reconnect...")

    def on_publish(self, client, userdata, mid):
        """
        Callback when a message publish is completed.
        """
        # We already mark as published in publish_unpublished_messages()
        # so we don’t need to do anything here.
        pass

