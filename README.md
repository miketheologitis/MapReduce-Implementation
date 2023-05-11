# Usage
```bash
pip install -r requirements.txt
```

Zookeeper:
```bash
docker pull zookeeper
```

Create-Scale workers:
```bash
docker-compose up -d --scale worker=6 --no-recreate
```

```bash
python -m unittest tests.unit_tests.test_worker
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

# Current repo
```markdown
MapReduce-Implementation/
├── src/
│   ├── __init__.py
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── master.py
│   │   └── worker.py
│   └── zookeeper/
│       ├── __init__.py
│       └── zookeeper_client.py
├── tests/
│   ├── __init__.py
│   ├── integration_tests/
│   │   ├── __init__.py
│   │   ├── test_worker.py
│   │   ├── ...
│   ├── unit_tests/
│   │   ├── __init__.py
│   │   ├── test_worker.py
│   │   ├── ...
├── README.md
├── requirements.txt
├── Dockerfile.worker
├── TODO.txt
├── docker-compose.yaml
└── main.py
```
