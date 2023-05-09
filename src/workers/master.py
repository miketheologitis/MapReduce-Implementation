from flask import Flask, request
import os

app = Flask(__name__)


class Master:
    def __init__(self):
        # addresses are of the form "<IP>:<PORT>"
        self.worker_addresses = [] # TODO: Request from zookeeper
        # ... rest of the class definition ...


# Create a singleton instance of Master
master = Master()


@app.route('/add-map', methods=['POST'])
def add_map_task():
    # Add a map task
    return 201


@app.route('/add-reduce', methods=['POST'])
def add_reduce_task():
    # Add a reduce task
    return 201


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

