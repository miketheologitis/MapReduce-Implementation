from kazoo.client import KazooClient

# Create a new client instance
zk = KazooClient(hosts='localhost:2181')

# Start the client
zk.start()

# ... Perform ZooKeeper operations ...

# Stop the client
zk.stop()