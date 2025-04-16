from publisher import EdgeQueuePublisher


print("connectiong...")
eqp = EdgeQueuePublisher(host="localhost", client_id = "test", db_path = "queue.db", retention=120)
print("connected")


print("pushing data")
for i in range(100):
    eqp.push_data("test_topic", "hello!!"+str(i))

print("pushhed!")


allrows = eqp.get_all_messages()
#print([row for row in allrows])


while True:
    pass
