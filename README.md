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

### Map Function

`map([x1, x2, ...]) -> [(k1, v2), (k2, v2), ...]` 

Example:
```python
def map_func(data):
    result = []
    for string in data:
        for char in string:
            result.append((char, 1))
    return result
```

```python
Input: ["mike", "george", "meg"]
Output: [('m', 1), ('i', 1), ('k', 1), ('e', 1), ('g', 1), ('e', 1), ('o', 1),
         ('r', 1), ('g', 1), ('e', 1), ('m', 1), ('e', 1), ('g', 1)]
```

### Shuffle
Intermediate results of the *map* function are shuffled (sorted and grouped by **key**). This operation is straightforward.
```python
Input: [('m', 1), ('i', 1), ('k', 1), ('e', 1), ('g', 1), ('e', 1),
        ('o', 1), ('r', 1), ('g', 1), ('e', 1), ('m', 1)]
Output: [('e', [1, 1, 1, 1]), ('g', [1, 1]), ('i', [1]), ('k', [1]),
         ('m', [1, 1]), ('o', [1]), ('r', [1])]
```


### Reduce Function
`reduce([(k1, [v1, v2, ...]), (k2, [y1, y2, ...]), ...]) -> [(k1, x1), (k2, x2), ...)]`

```python
def reduce_func(values):
    return sum(values)
```
```python
Input: [('e', [1, 1, 1, 1]), ('g', [1, 1]), ('i', [1]), ('k', [1]),
         ('m', [1, 1]), ('o', [1]), ('r', [1])]
Output: [('e', 4), ('g', 2), ('i', 1), ('k', 1), ('m', 2), ('o', 1), ('r', 1)]
```

# Current repo
```markdown
MapReduce-Implementation/
├── src/
│   ├── __init__.py
│   ├── cluster/
│   │   ├── __init__.py
│   │   └── local_cluster.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   └── local_monitoring.py
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── master.py
│   │   └── worker.py
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
│   │   ├── test_zookeeper_client.py
│   │   └── test_local_cluster_local_monitoring.py
│   ├── unit_tests/
│   │   ├── __init__.py
│   │   └── test_worker.py
├── examples/
│   ├── __init__.py
│   ├── testing.ipynb
├── README.md
├── requirements.txt
├── Dockerfile.worker
├── Dockerfile.master
├── TODO.txt
├── docker-compose.yaml
├── hadoop.env
```

### Tests

```bash
python -m unittest tests.unit_tests.test_worker
python -m unittest tests.integration_tests.test_hdfs_client
python -m unittest tests.integration_tests.test_zookeeper_client
python -m unittest tests.integration_tests.test_local_cluster_local_monitoring
python -m unittest tests.integration_tests.test_local_cluster
```
