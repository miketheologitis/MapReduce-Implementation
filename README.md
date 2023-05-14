# Usage

Modify `/etc/hosts`, add the following:
```
127.0.0.1       datanode
```
This is needed because the hadoop namenode (that we talk to add files to hdfs) returns
the hostname of the datanode (i.e., `datanode`) but this returned hostname is inside the docker-compose
network. Everything is ok when we make the requests from inside the docker-compose network, but when 
we make changes to hdfs from outside the network we have a problem. Modify it.

```bash
pip install -r requirements.txt
```

Zookeeper:
```bash
docker pull zookeeper
```

Create-Scale workers:
```bash
docker-compose up -d --scale master=1 --scale worker=1 --no-recreate
```

```bash
python -m unittest tests.unit_tests.test_worker
python -m unittest tests.unit_tests.test_master
python -m unittest tests.integration_tests.test_worker_zookeeper_integration
```

```bash
docker-compose down
```

```bash
docker exec -it <CONTAINERID>
```

### Map Function
Map Function assumes to return a list of key-value pairs for a single fed element.
Takes as input a tuple with +1 elements `(X,)` is enough.

Example:
- `f(("mike",))` -> `[('m', 1), ('i', 1), ('k', 1), ('e', 1)]`
- `f(("george",))` -> `[('g', 1), ('e', 1), ('o', 1), ('r', 1), ('g', 1), ('e', 1)]`
- `f(("m",))` -> `[('m', 1)]`

Input:
```
[("mike",), ("george",), ("123",)]
```
Result:
```
[('m', 1), ('i', 1), ('k', 1), ('e', 1), ('g', 1), ('e', 1),
('o', 1), ('r', 1), ('g', 1), ('e', 1), ('1', 1), ('2', 1),
('3', 1)]
```

### Reduce Function
Reduce function assumes `(key, value)` tuples as input and does the obvious.

Notice that in the map function we allow more freedom, i.e., `(X, ..., ...)`.


## HDFS

```python
from hdfs import InsecureClient
hdfs = InsecureClient('http://localhost:9870', user='mapreduce')
hdfs.status('/')
```

# Current repo
```markdown
MapReduce-Implementation/
├── src/
│   ├── __init__.py
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── master.py
│   │   └── worker.py  (+)
│   ├── zookeeper/
│   │   ├── __init__.py
│   │   └── zookeeper_client.py
│   ├── hadoop/
│   │   ├── __init__.py
│   │   └── hdfs_client.py
├── tests/
│   ├── __init__.py
│   ├── integration_tests/
│   │   ├── __init__.py
│   │   ├── test_hdfs_client.py
│   │   └── test_zookeeper_client.py (++)
│   ├── unit_tests/
│   │   ├── __init__.py
│   │   ├── test_worker.py  (+)
│   │   └── test_master.py (+)
├── README.md
├── requirements.txt
├── Dockerfile.worker
├── Dockerfile.master
├── TODO.txt
├── docker-compose.yaml
├── hadoop.env
```
