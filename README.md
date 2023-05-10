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
python -m unittest integration_tests.unit_tests.test_worker
```

```bash
docker-compose down
```

```bash
docker exec -it <CONTAINERID>
```

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
│   └── integration_tests/
│   │   ├── __init__.py
│   │   ├── test_worker.py
│       ├── ...
├── README.md
├── requirements.txt
├── Dockerfile.worker
├── TODO.txt
├── docker-compose.yaml
└── main.py
```
