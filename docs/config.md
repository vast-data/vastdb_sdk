# Configuration

## Query tuning

It is possible to tune `Table.select()` behaviour using [`QueryConfig`](https://github.com/vast-data/vastdb_sdk/blob/main/vastdb/config.py):

```python
from vastdb.config import QueryConfig
cfg = QueryConfig()  # default configuration values
# <modify configuration>
table.select(columns=['c1'], predicate=(_.c2 > 2), config=cfg)
```

### Multiple endpoints

In order to split the scanning work of a query across multiple CNode connections, it is possible to explicitly list the CNodes URLs (both via VIPs and/or domain names):

```python
cfg.data_endpoints=[
    "http://172.19.196.1",
    "http://172.19.196.2",
    "http://172.19.196.3",
    "http://172.19.196.4",
]
```

### Internal CNode concurrency

When an RPC is being handled by a CNode, it spawns `num_sub_splits` worker threads that allow us to process a single request from multiple CNode cores.
Using higher internal CNode concurrency can help selective queries:
```python
cfg.num_sub_splits = 8
```
