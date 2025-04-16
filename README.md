# svwaternet-edgeq
Edge queueing library for svwaternet system.



Super basic wrapper around Paho MQTT messaging. 

```
from publisher import EdgeQueuePublisher
eqp = EdgeQueuePublisher(host="localhost", client_id = "test", db_path = "queue.db", retention=120)
for i in range(100):
    eqp.push_data("test_topic", "hello!!"+str(i))
```


This will attempt to broadcast data to the MQTT broker at `host`. If a broadcast fails, the messages
are queued in a local sqlite file `queue.db`. A background thread periodically attempts to reconnect
and send unsent data from the sqllite file. Old messages (sent and unsent) are retained in the file up to
`retention` seconds after they have been created.
