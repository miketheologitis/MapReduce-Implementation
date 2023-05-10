# Usage

Zookeeper:
```bash
docker pull zookeeper
```

```bash
docker-compose -f docker-compose.yaml up -d```
```

Scale workers:
```bash
docker-compose up -d --scale worker=5
```

```bash
python -m unittest tests.unit_tests.test_worker
```

```bash
docker-compose down
```

```bash
docker exec -it <CONTAINERID>
```

# Initial Idea (will definitely change)
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
│   ├── unit_tests/
│   │   ├── __init__.py
│   │   ├── test_worker.py
│       ├── ...
│   └── integration_tests/
│       ├── __init__.py
│       ├── ...
├── README.md
├── requirements.txt
├── Dockerfile.worker
├── TODO.txt
├── docker-compose.zookeeper.yaml
└── main.py
```
