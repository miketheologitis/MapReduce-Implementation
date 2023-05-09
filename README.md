# Usage

```bash
~/MapReduce-Implementation$ python -m unittest tests.unit_tests.test_worker
```

# Initial Idea (will definitely change)
```markdown
MapReduce-Implementation/
├── src/
│   ├── __init__.py
│   ├── workers/
│   │   ├── __init__.py
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
└── main.py
```
