# **VastdbApi Documentation**

**NOTE**
- Please note that this API & the documentation is currently in a pre-release stage. Until version 1.x is officially released, the API should be considered unstable.

## VastdbApi - Content Overview. 

- **[Introduction](#what-is-a-vastdbapi)**
- **[Getting Started](#setting-up-getting-started)**
   - **[Requirements](#requirements)**
   - **[Build and Install](#build-and-install-the-vastdbapi)**
- **[Creating a Vastdb session](#creating-the-initial-session-with-vastdbapi)**
- **[Supported Methods on VastdbApi](#supported-methods-on-vastdbapi)**
   - **[Schema Management](#schema-management)**
   - **[Table Management](#table-management)**
   - **[Column Management](#column-management)**
   - **[Semi-Sorted Projections Management](#semi-sorted-projections-management)**
   - **[Data Querying and Manipulation Functions](#data-querying-and-manipulation)**
      - **[Filter Predicates in VastdbApi](#filter-predicates-in-vastdbapi)**
   - **[Snapshots Management](#snapshots-management)**
   - **[VastdbApi help function](#vastdbapi-help-function)**
- **[Advanced Examples](#advanced-examples)**
   - **[Create schema and table with columns](#create-schema-and-table-with-columns)**
   - **[Add columns to an existing table](#add-columns-to-an-existing-table)**
   - **[Import a tabular file from S3 bucket](#import-a-tabular-file-from-s3-bucket)**
   - **[List columns and data from a table](#list-columns-and-data-from-a-table)**
   - **[Query data with filters](#query-data-with-filters)**
   - **[Table population flow](#combined-example-with-a-few-functions)**
- **[Interacting with Vast Catalog](#interracting-with-vast-catalog-using-vastdbapi)**
   - **[What is Vast Catalog](#what-is-vast-catalog)**
   - **[Vast Catalog Schema Structure](#vast-catalog-schema-structure)**
   - **[Vast Catalog - Query examples with VastdbAPI](#vast-catalog-query-examples-with-vastdbapi)**
  

## What is a VastdbApi

**`VastdbApi`** is a Python based API designed for interacting with VastDB, enabling operations such as schema and table management, data querying, and transaction handling.  
Key libraries used in this API include requests for HTTP requests, pyarrow for handling Apache Arrow data formats, and flatbuffers for efficient serialization of data structures.


## Setting up & Getting started

### Requirements

- Linux / Windows / MacOS client server connected on data access with VAST cluster
- Virtual IP pool configured with DNS service
  - [Network Access through Virtual IPs](https://support.vastdata.com/hc/en-us/articles/5140602978844-Configuring-Network-Access)
- VIP DNS name - for reaching all availble Cnodes in the API requests
  - [Configuring the VAST Cluster DNS Service](https://support.vastdata.com/hc/en-us/articles/9859957831452-Configuring-the-VAST-Cluster-DNS-Service)
- Python 3.7 Or above with write permissions to the local python site-packages library
- pip installed packages:
  - pyarrow
- S3 User Access & Secret Keys on VAST cluster
  - [Managing S3 User Access ](https://support.vastdata.com/hc/en-us/articles/9859972743580#UUID-6a15026a-d0bd-ebe6-e281-4b980674cecc_section-idm4577174596542432775810296988)
- Tabular Identity Policy with the proper permissions 
  - [Creating Identity Policies](https://support.vastdata.com/hc/en-us/articles/9859958983708#UUID-c381c613-81bd-3c09-69c4-ba4dcd8e8d6d)


### Build and Install the VastdbApi

##### `NOTE:` Currently this API is in a pre-release stage, It can be installed with test.pypi until it will be published to the official PYPI index. 

```
pip install -i https://test.pypi.org/simple/ vastdb
```

## Creating the initial session with VastdbApi:

```python
from vastdb import vastdb_api
import pyarrow as pa
import vast_flatbuf
from vastdb.vastdb_api import VastdbApi

def create_vastdb_session(access_key, secret_key):
    return VastdbApi(host='VAST_VIP_POOL_DNS_NAME', access_key=access_key, secret_key=secret_key)


access_key='D8UDFDF...'
secret_key='B7bqMegmj+TDN..'
vastdb_session = create_vastdb_session(access_key, secret_key)

```

## Supported Methods on VastdbApi

### Schema Management

#### `create_schema`
- **Usage**: Create a new schema in a bucket.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema to create.
  - `tenant_id` (int, optional): Tenant ID (default is `0`). 
  - `schema_properties` (str, optional): Optional schema metadata - buffer of up to 4k
  - `txid` (int, optional): Transaction ID (default is `0`).
- **Example**:
```python
  vastdb_session.create_schema(bucket_name, schema_name)
```

#### `list_schema`
- **Usage**: List all schemas in a bucket.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): The schema to use (default is an empty string `""`).
  - `txid` (int): Transaction ID (default is `0`).  
  - `max_keys` (int, optional): Maximum number of keys to retrieve (default is `999`).
  - `next_key` (int, optional): Key to start retrieving data from (default is `0`).
  - `name_prefix` (str, optional): Prefix to filter keys by (default is an empty string `""`).
  - `exact_match` (bool, optional): If `True`, perform an exact match for the keys (default is `False`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
  - `count_only` (bool, optional): If `True`, only count the matching keys without retrieving the data.
- **Example**:
```python
  vastdb_session.list_schemas(bucket_name)
```

#### `alter_schema`
- **Usage**: Modify an existing schemain a bucket.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema to alter.
  - `new_schema_name` (str): New name for the schema.
  - `schema_properties` (str, optional): Optional schema metadata - buffer of up to 4k
  - `tenant_id` (int, optional): Tenant ID (default is `0`). 
  - `txid` (int, optional): Transaction ID (default is `0`).
- **Example**:
```python
  vastdb_session.alter_schema(bucket_name, schema_name, new_schema_name='renamed_schema')
```

#### `drop_schema`
- **Usage**: Delete a schema in a bucket.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema to delete.
  - `tenant_id` (int, optional): Tenant ID (default is `0`). 
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `client_tags` (list): List of client tags (default is `[]`).
- **Example**:
```python
  vastdb_session.drop_schema(bucket_name, schema_name)
```


### Table Management


#### `create_table`
- **Usage**: Create a new table in a specified schema.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table to create.
  - `txid` (int, optional): Transaction ID (default is `0`).  
  - `tenant_id` (int, optional): Tenant ID (default is `0`). 
- **Example**:
```python
  arrow_schema = pa.schema([('column1', pa.int32()), ('column2', pa.string())])
  vastdb_session.create_table(bucket_name, schema_name, table='new_table', arrow_schema=arrow_schema)
```

#### `list_tables`
- **Usage**: List all tables in a schema.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): The schema to use (default is an empty string `""`).
  - `txid` (int, optional)): Transaction ID (default is `0`).  
  - `max_keys` (int, optional): Maximum number of keys to retrieve (default is `999`).
  - `next_key` (int, optional): Key to start retrieving data from (default is `0`).
  - `name_prefix` (str): Prefix to filter keys by (default is an empty string `""`).
  - `exact_match` (bool, optional): If `True`, perform an exact match for the keys (default is `False`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
  - `count_only` (bool, optional): If `True`, only count the matching keys without retrieving the data.
- **Example**:
```python
  vastdb_session.list_tables(bucket_name, schema_name)
```

#### `alter_table`
- **Usage**: Modify an existing table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table to alter.
  - `new_table_name` (str): New name for the table.
  - `table_properties` (str, optional)): Optional table metadata - buffer of up to 4k
  - `txid` (int, optional)): Transaction ID (default is `0`).  
  - `tenant_id` (int, optional)): Tenant ID (default is `0`). 
- **Example**:
```python
  vastdb_session.alter_table(bucket_name, schema_name, table='my_table', new_table_name='renamed_table')
```

#### `drop_table`
- **Usage**: Delete a table from a schema.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table to delete.
  - `txid` (int, optiona)): Transaction ID (default is `0`).  
  - `tenant_id` (int, optional): Tenant ID (default is `0`). 
- **Example**:
```python
  vastdb_session.drop_table(bucket_name, schema_name, table_name)
```

#### `get_table_stats`
- **Usage**: Obtain statistics about a specific table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `txid` (int, optional): Transaction ID (default is `0`).  
  - `tenant_id` (int, optional): Tenant ID (default is `0`). 
- **Example**:
```python
  vastdb_session.get_table_stats(bucket_name, schema_name, table_name)
```


### Column Management

#### `add_columns`
- **Usage**: Add new columns to an existing table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `arrow_schema` (Apache Arrow Schema): Schema of the columns to add.
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  
- **Example**:
```python
  new_columns = pa.schema([('new_column', pa.int64())])
  vastdb_session.add_columns(bucket_name, schema_name, table_name, arrow_schema=new_columns)
```

#### `list_columns`
- **Usage**: List all columns of a table.
- **Parameters**:
  - `bucket` (str): The bucket containing the table.
  - `schema` (str): The schema where the table is located.
  - `table` (str): The name of the table.
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `max_keys` (int, optional): Maximum number of keys to retrieve (default is `999`).
  - `next_key` (int, optional): Key to start retrieving data from (default is `0`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
  - `bc_list_internals` (bool, optional): If `True`, list internal columns (default is `False`).
- **Example**:
```python
  vastdb_session.list_columns(bucket_name, schema_name, table_name)
```

#### `alter_column`
- **Usage**: Modify properties of a column in a table.
- **Parameters**:
  - `bucket` (str): The bucket to retrieve data from.
  - `schema` (str): The schema to use.
  - `table` (str): The name of the table.
  - `column_name` (str): The name of the column.
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `column_properties` (str, optional): Properties/metadata of the column (default is an empty string `""`).
  - `new_column_name` (str, optional): New column name (default is an empty string `""`).
  - `column_sep` (str, optional): Separator for columns (default is `"."`).
  - `column_stats` (str, optional): Statistics for the column (default is an empty string `""`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
- **Example**:
```python
  vastdb_session.alter_column(bucket_name, schema_name, table_name, column_name='existing_column', new_column_name='renamed_column')
```

#### `drop_columns`
- **Usage**: Remove columns from a table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `arrow_schema` (Apache Arrow Schema): Schema of the columns to remove.
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  
- **Example**:
```python
  columns_to_drop = pa.schema([('column_to_drop', pa.int64())])
  vastdb_session.drop_columns(bucket_name, schema_name, table_name, arrow_schema=columns_to_drop)
```

### Transaction Management

#### `begin_transaction`
- **Usage**: Initiate a new transaction.
- **Parameters**:
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
- **Example**:
```python
  vastdb_session.begin_transaction()
```

#### `commit_transaction`
- **Usage**: Commit an ongoing transaction.
- **Parameters**:
  - `txid` (int): Transaction ID.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
- **Example**:
```python
  transaction_id = 1234  # Replace with actual transaction ID
  vastdb_session.commit_transaction(txid=transaction_id)
```

#### `rollback_transaction`
- **Usage**: Rollback a transaction.
- **Parameters**:
  - `txid` (int): Transaction ID.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
- **Example**:
```python
  transaction_id = 1234  # Replace with actual transaction ID
  vastdb_session.rollback_transaction(txid=transaction_id)
```

### Data Querying and Manipulation

#### `query_data`
- **Usage**: Execute a data query on a specified table within a bucket and schema. This function allows for complex queries, including filters and projections, on the table data.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table to query.
  - `params` (bytes): Serialized query parameters, typically generated using a `build_query_data_request` function.
  - `split` (tuple, optional): Tuple indicating the split of data (default is `(0, 1, 1)`).
  - `num_sub_splits` (int, optional): Number of sub-splits for the query (default is `1`).
  - `response_row_id` (bool, optional): Whether to include row IDs in the response (default is `False`).
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `limit_rows` (int, optional): Limit on the number of rows to return (default is `0`).
  - `schedule_id` (int/None, optional): Schedule ID for the query (default is `None`).
  - `retry_count` (int, optional): Number of retries for the query (default is `0`).
  - `search_path` (str/None, optional): Search path for the query (default is `None`).
  - `sub_split_start_row_ids` (list, optional): List of start row IDs for each sub-split (default is `[]`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  

##### Filter Predicates in VastdbApi
- You can use these filters in the VastdbApi API:  

  - **`eq`**: Equal -> `'column_name': ['eq value']`
  - **`ge`**: Greater Than or Equal -> `'column_name': ['ge value']`
  - **`gt`**: Greater Than -> `'column_name': ['gt value']`
  - **`lt`**: Less Than -> `'column_name': ['lt value']`
  - **`le`**: Less Than or Equal -> `'column_name': ['le value']`
  - **`is_null`**: Checks for null values -> `'column_name': ['is_null']`
  - **`is_not_null`**: Checks for non-null values -> `'column_name': ['is_not_null']`

**Note:**
  - If using pandas dataframe, pandas predicates can also be used. (Perfomance might be reflected because it's not API-native filters)
    - **Example** [Query a table using Pandas predicates](#query-a-table-using-pandas-predicates)
    

- **Example**:
```python
  query_params = build_query_data_request(schema, filters={'column_name': ['gt 10']})
  res = vastdb_session.query_data(bucket_name, schema_name, table_name, params=query_params.serialized)
  parsed_results = parse_query_data_response(res.raw, query_params.response_schema)
```
  **[See more advanced examples on how to query data](#advanced-examples)**

#### `import_data`
- **Usage**: Import data into a table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `source_files` (list): List of file paths or locations to import.
  - `txid` (int, optional): Transaction ID.
  - `expected_retvals` (list, optional): Expected return values.
  - `case_sensitive` (bool, optional): Case sensitivity of the import operation.
  - `schedule_id` (str, optional): Schedule ID for the import.
  - `retry_count` (int, optional): Number of retries for the import.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  
- **Example**:
```python
  source_files = {('s3-bucket-name', '/path/to/file2.parquet'): b''}
  vastdb_session.import_data(bucket_name, schema_name, table_name, source_files=source_files)
```
  **[See more advanced examples on how to import a tabular file](#import-a-tabular-file-from-s3-bucket)**


#### `insert_rows`
- **Usage**: Insert rows into a table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `record_batch` (Apache Arrow RecordBatch): RecordBatch containing the rows to insert.
  - `txid` (int, optional): Transaction ID.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  
- **Example**:
```python
  record_batch = pa.RecordBatch.from_pandas(df)
  vastdb_session.insert_rows(bucket_name, schema_name, table_name, record_batch=record_batch)
```

#### `update_rows`
- **Usage**: Update existing rows in a table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `record_batch` (Apache Arrow RecordBatch): RecordBatch containing the updated rows.
  - `txid` (int, optional): Transaction ID.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  
- **Example**:
```python
  updated_record_batch = pa.RecordBatch.from_pandas(updated_df)
  vastdb_session.update_rows(bucket_name, schema_name, table_name, record_batch=updated_record_batch)
```

#### `delete_rows`
- **Usage**: Delete specific rows from a table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `record_batch` (Apache Arrow RecordBatch): RecordBatch identifying the rows to delete.
  - `txid` (int, optional): Transaction ID.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  
- **Example**:
```python
  rows_to_delete = pa.RecordBatch.from_pandas(df_to_delete)
  vastdb_session.delete_rows(bucket_name, schema_name, table_name, record_batch=rows_to_delete)
```

### Semi-Sorted Projections Management

#### `create_projection`
- **Usage**: Create a projection
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `name` (str): Name of the projection.
  - `columns` (str): Columns to project.
  - `txid` (int, optional): Transaction ID.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
- **Example**:
```python
  proj2_columns = [('b', 'Sorted'), ('c', 'Unsorted'), ('d', 'Unsorted')]
  res = vastdb_session.create_projection(bucket_name, schema_name, table_name, "proj2", columns=proj2_columns)
```

#### `alter_projection`
- **Usage**: Alter an existing projection
- **Parameters**:
  - `bucket` (str): The bucket containing the table.
  - `schema` (str): The schema where the table is located.
  - `table` (str): The name of the table.
  - `name` (str): The name of the table column to modify.
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `table_properties` (str, optional): Properties/metadata of the table (default is an empty string `""`).
  - `new_name` (str, optional): New name for the projection (default is an empty string `""`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).  
- **Example**:
```python
  vastdb_session.alter_projection(bucket_name, schema_name, table_name, "proj1" ,new_name="proj1_new")
```


#### `drop_projection`
- **Usage**: Drop a projection
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `name` (str): Name of the projection.
  - `txid` (int, optional): Transaction ID.
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
- **Example**:
```python
  vastdb_session.drop_projection(bucket_name, schema_name, table_name, "proj1_new")
```

#### `list_projections`
- **Usage**: list existing projections on a specific table
- **Parameters**:
  - `bucket` (str): The bucket to retrieve data from.
  - `schema` (str): The schema to use.
  - `table` (str): The name of the table.
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `max_keys` (int, optional): Maximum number of keys to retrieve (default is `999`).
  - `next_key` (int, optional): Key to start retrieving data from (default is `0`).
  - `name_prefix` (str, optional): Prefix to filter keys by (default is an empty string `""`).
  - `exact_match` (bool, optional): If `True`, perform an exact match for the keys (default is `False`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
  - `include_list_stats` (bool, optional): If `True`, include list statistics (default is `False`).
  - `count_only` (bool, optional): If `True`, only count the matching keys without retrieving the data.
- **Example**:
```python
  vastdb_session.list_projections(bucket_name, schema_name, table_name)
```

#### `list_projection_columns`
- **Usage**: list columns of a specific projection on a specific table
- **Parameters**:
  - `bucket` (str): The bucket to retrieve data from.
  - `schema` (str): The schema to use.
  - `table` (str): The name of the table.
  - `name` (str): Name of the projection.
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `max_keys` (int, optional): Maximum number of keys to retrieve (default is `999`).
  - `next_key` (int, optional): Key to start retrieving data from (default is `0`).
  - `name_prefix` (str, optional): Prefix to filter keys by (default is an empty string `""`).
  - `exact_match` (bool, optional): If `True`, perform an exact match for the keys (default is `False`).
  - `tenant_id` (int, optional): Tenant ID (default is `0`).
  - `count_only` (bool, optional): If `True`, only count the matching keys without retrieving the data.
- **Example**:
```python
  vastdb_session.list_projection_columns(bucket_name, schema_name, table_name, "proj2")
```


### Snapshots Management

#### `list_snapshots`
- **Usage**: List all snapshots in a bucket.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `max_keys` (int, optional): Maximum number of keys to return(default is `1000`).
  - `next_token` (str, optional): Token for the next set of items in the list.
- **Example**:
```python
  vastdb_session.list_snapshots(bucket_name, max_keys=1000, next_token='next_token_value')
```

## VastdbApi help function
- **Usage**: List all classes and functions in the vastdb_api
- **Example**:
```python
  help(VastdbApi)
```


## Advanced Examples


### Create schema and table with columns

```python
bucket_name='vastdb-bucket'
schema_name='vastdb-schema'
table_name='table-name'

# Create Schema
vastdb_session.create_schema(bucket_name, schema_name)

# Define Table Schema
arrow_schema = pa.schema([
    pa.field('column_name1', pa.int32()),
    pa.field('column_name2', pa.string()),
    # Add more fields as needed
])

# Create Table
vastdb_session.create_table(bucket_name, schema_name, table_name, arrow_schema)

# Verify Table Columns
cols = vastdb_session.list_columns(bucket_name, schema_name, table_name)
res_schema = pa.schema([(column[0], column[1]) for column in cols[0]])
print(res_schema)
```

### Add columns to an existing table

```python
# Define Additional Columns
new_columns_schema = pa.schema([
    pa.field('new_column_name1', pa.int64()),  # Example new column
    pa.field('new_column_name2', pa.string()),  # Another example new column
    # Add more fields as needed
])

# Add Columns
vastdb_session.add_columns(bucket_name, schema_name, table_name, new_columns_schema)

# Verify Added Columns
cols = vastdb_session.list_columns(bucket_name, schema_name, table_name)
res_schema = pa.schema([(column[0], column[1]) for column in cols[0]])
print(res_schema)

```

### Import a tabular file from S3 bucket

```python
s3_files = {
    ('s3-bucket-name', 'citizens_data.parquet'): b''  # 's3-bucket-name', 'file-name.parquet'
}

result = vastdb_session.import_data(
    bucket_name='vastdb-bucket-name',
    schema_name='schema-name',
    table_name='table-name',
    source_files=s3_files,
    txid=0,  # Use an appropriate transaction ID if necessary
    case_sensitive=False
)

if result.status_code == 200:
    print("Import successful")
else:
    print("Import failed:", result)
```

### List columns from a table

```python
bucket_name='vastdb-bucket'
schema_name='schema-name'

vastdb_session = create_vastdb_session(access_key, secret_key)

# List columns in the table
cols = vastdb_session.list_columns(bucket_name, schema_name, 'store_sales')

# Create a schema from the column list
res_schema = pa.schema([(column[0], column[1]) for column in cols[0]])
print(res_schema)
```

## Query data with filters

```python
from vastdb import vastdb_api
import pyarrow as pa
import pandas as pd
import vast_flatbuf
from vastdb.vastdb_api import VastdbApi

def create_vastdb_session(access_key, secret_key):
    return VastdbApi(host='pool1.vast55-kfs.vasteng.lab', access_key=access_key, secret_key=secret_key, port=80)


access_key='83SKB7G7...'
secret_key='00k0oQ4eG6...'

vastdb_session = create_vastdb_session(access_key, secret_key)
```

##### Query table without limit, filters and field_names, i.e. "SELECT * FROM table"

```python
bucket_name = "vastdb-bucket1"
schema_name = "citizens-schema"
table_name = "jersey-citizens-table"

table = vastdb_session.query(bucket_name, schema_name, table_name)
df = table.to_pandas()
print(df)
```

##### Query table with limit 10 rows, i.e. "SELECT Citizen_Age, Citizen_Name, Is_married FROM table WHERE Citizen_Age > 38 limit 10"

```python
field_names = ['Citizen_Age', 'Citizen_Name', 'Is_married']
filters = {'Citizen_Age': ['gt 38']}

table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names, limit_rows=5)

df = table.to_pandas()
print(df)
```

##### (Citizen_Age > 35) AND (Citizen_experience <= 9)

```python
field_names = ['Citizen_Age', 'Citizen_Name', 'Citizen_experience']
filters = {'Citizen_Age': ['gt 35'], 'Citizen_experience': ['le 9']}

table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)

df = table.to_pandas()
print(df)
```

##### (Citizen_Age = 50) OR (Citizen_Age = 55)

```python
field_names = ['Citizen_Age', 'Citizen_Name', 'Citizen_experience']
filters = {'Citizen_Age': ['eq 50', 'eq 55']}

table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)

df = table.to_pandas()
print(df)
```

##### (Citizen_experience => 25) AND (Citizen_Age between 55 to 75)

```python
field_names = ['Citizen_Age', 'Citizen_Name', 'Citizen_experience', 'Citizen_id']
filters = {'Citizen_experience': ['le 25'], 'Citizen_Age': [('ge 55' , 'le 75')]} 
table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)

df = table.to_pandas()
print(df)
```

##### Query a table using Pandas predicates: 

```python
table = vastdb_session.query(bucket_name, schema_name, table_name)
df = table.to_pandas()
pd.options.display.max_columns = None
display(df[(df['uid'] == 555) & (df['size'] > 4096)].head(10))
```


## Combined example with a few functions

##### Simulating a table population flow

```python

from vastdb import vastdb_api
import pyarrow as pa
import vast_flatbuf
from vastdb.vastdb_api import VastdbApi

def create_vastdb_session(access_key, secret_key):
    return VastdbApi(host='pool1.vast55-kfs.vasteng.lab', access_key=access_key, secret_key=secret_key)


access_key='83SKB7G...'
secret_key='00k0oQ4eG6o4/PLGAWL..'

vastdb_session = create_vastdb_session(access_key, secret_key)

bucket_name = "vastdb-bucket1"
schema_name = "my_schema_name"

# Create the schema
response = vastdb_session.create_schema(bucket_name, schema_name)
if response.status_code == 200:
    print("create_schema successful")
else:
    print("create_schema failed:", response)
    exit(1)


# Define columns schema (types) & create table with the columns included
table_name = "my-table-name"

arrow_schema = pa.schema([
        ('Citizen_Age', pa.int64()),
        ('Citizen_Name', pa.string()),
        ('Citizen_experience', pa.float64()),
        ('Is_married', pa.bool_()),
    ])

response = vastdb_session.create_table(bucket_name, schema_name, table_name, arrow_schema)
if response.status_code == 200:
    print("create_table successful")
else:
    print("create_table failed:", response)
    exit(1)


# Create a pyarrow record batch & insert rows to the created columns
arrays = [
        [45, 38, 27, 51],
        ['David', 'John', 'Simon', 'Koko'],
        [25.5, 17.9, 5.3, 28.2],
        [True, False, False, True]
    ]

arrays = [pa.array(col_arr, col_type) for col_arr, col_type in zip(arrays, arrow_schema.types)]
batch = pa.record_batch(arrays, arrow_schema)
insert_rows_req = vastdb_api.serialize_record_batch(batch)

response = vastdb_session.insert_rows(bucket=bucket_name, schema=schema_name, table=table_name, record_batch=insert_rows_req)
if response.status_code == 200:
    print("insert_rows successful")
else:
    print("insert_rows failed:", response)
    exit(1)

# query the table after insert
query_data_request = vastdb_api.build_query_data_request(arrow_schema)
res = vastdb_session.query_data(bucket_name, schema_name, table_name, params=query_data_request.serialized)

table, = vastdb_api.parse_query_data_response(res.raw, query_data_request.response_schema)
print(f'table after insert: {table.to_pydict()}')

# Update rows id 0 & 2 with, on 2 specific columns with new values
columns_to_update = [(pa.uint64(), '$row_id'), (pa.int64(), 'Citizen_Age'), (pa.bool_(), 'Is_married')]
column_values_to_update = {pa.uint64(): [0, 2], pa.int64(): [43, 28], pa.bool_(): [False, True]}

update_rows_req = vastdb_api.build_record_batch(columns_to_update, column_values_to_update)
vastdb_session.update_rows(bucket=bucket_name, schema=schema_name, table=table_name, record_batch=update_rows_req)

query_data_request = vastdb_api.build_query_data_request(arrow_schema)
res = vastdb_session.query_data(bucket_name, schema_name, table_name, params=query_data_request.serialized)
table, = vastdb_api.parse_query_data_response(res.raw, query_data_request.response_schema)
print(f'table after update: {table.to_pydict()}')

# Add additional columns to the table schema
additional_columns_schema = pa.schema([('Citizen_id', pa.int64()), ('Citizen_street', pa.string())])
response = vastdb_session.add_columns(bucket_name, schema_name, table_name, additional_columns_schema)
if response.status_code == 200:
    print("add_columns successful")
else:
    print("add_columns failed:", response)
    exit(1)

# retrieve columns and new schema
cols = vastdb_session.list_columns(bucket_name, schema_name, table_name)
res_schema = pa.schema([(column[0], column[1]) for column in cols[0]])


# Insert data to rows
arrays = [
        [45, 38, 27, 51],
        ['David', 'John', 'Simon', 'Koko'],
        [25.5, 17.9, 5.3, 28.2],
        [True, False, False, True],
        [222333, 333222, 444333, 555444],
        ['fufu', 'fafu', 'kuki', 'kaku']
    ]

arrays = [pa.array(col_arr, col_type) for col_arr, col_type in zip(arrays, res_schema.types)]
batch = pa.record_batch(arrays, res_schema)
insert_rows_req = vastdb_api.serialize_record_batch(batch)

response = vastdb_session.insert_rows(bucket=bucket_name, schema=schema_name, table=table_name, record_batch=insert_rows_req)
if response.status_code == 200:
    print("insert_rows successful")
else:
    print("insert_rows failed:", response)
    exit(1)


# Query data select all
query_data_request = vastdb_api.build_query_data_request(res_schema)
res = vastdb_session.query_data(bucket_name, schema_name, table_name, params=query_data_request.serialized)
table, = vastdb_api.parse_query_data_response(res.raw, query_data_request.response_schema)
print(f'table after update: {table.to_pydict()}')

# Import multiple parquet files from s3 bucket to your table

s3_files = {('parquet-files-bucket', 'citizens_data_25.parquet'): b'',
            ('parquet-files-bucket', 'citizens_data_26.parquet'): b'',
            ('parquet-files-bucket', 'citizens_data_27.parquet'): b'',
           }
response = vastdb_session.import_data(bucket_name, schema_name, table_name, source_files=s3_files)
if response.status_code == 200:
    print("import_data successful")
else:
    print("import_data failed:", response)
    exit(1)


# Query all table after imports

res = vastdb_session.query_data(bucket_name, schema_name, table_name, params=query_data_request.serialized)
table, = vastdb_api.parse_query_data_response(res.raw, query_data_request.response_schema)
print(f'table after import: {table.to_pydict()}')

```

## Interracting with VAST Catalog using VastdbApi

### What is Vast Catalog

  - VAST Catalog is a database that indexes metadata attributes of all data on the cluster from periodic snapshots of the cluster's data. The database is stored on a dedicated S3 bucket on the cluster.
  - [VAST Catalog Overview](https://support.vastdata.com/hc/en-us/articles/9859973783964-VAST-Catalog-Overview)


### Vast Catalog Schema Structure

  - The Vast-Catalog has internal bucket, schema and table. To query the Catalog using the api, define these:

  ```python
  bucket_name='vast-big-catalog-bucket'
  schema_name='vast_big_catalog_schema'
  table_name='vast_big_catalog_table'
  ```

  **The schema  columns**

  - The Vast Catalog columns are consistent, and indexes the following attributes:
  
| Column             | Type                                    |
|--------------------|-----------------------------------------|
| phandle            | row(clone_id integer, handle_id bigint) |
| creation_time      | timestamp(9)                            |
| uid                | integer                                 |
| owner_sid          | varchar                                 |
| owner_name         | varchar                                 |


- **This is only a few columns, for a full schema structure check [VAST Catalog Overview](https://support.vastdata.com/hc/en-us/articles/9859973783964-VAST-Catalog-Overview)**


## Vast Catalog - Query examples with VastdbApi

#### Get all objects with uid 555 & size greater than 50k

```python
field_names = ['uid', 'size', 'search_path']
filters = {'uid': ['eq 555'], 'size': ['gt 50000']}
table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)
df = table.to_pandas()
print(df)
```

#### Query for Files Owned by vastdata with Size Greater Than 50000 and Created After a Specific Date:

```python
import time
import datetime

date_str = '2023-11-24'
pattern = '%Y-%m-%d'
epoch = int(time.mktime(time.strptime(date_str, pattern)))

filters = {
    'owner_name': ['eq vastdata'],
    'size': ['gt 50000'],
    'creation_time': [f'gt {epoch}']
}

field_names = ['creation_time', 'uid', 'owner_name', 'size']

table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)
df = table.to_pandas()
print(df)
```
#### Query for Specific File Types Across Different Users:

```python
field_names = ['uid', 'owner_name', 'element_type']
filters = {
    'element_type': ['eq FILE', 'eq TABLE', 'eq DIR'],
    'uid': ['eq 500', 'eq 1000']
}
table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)
df = table.to_pandas()
print(df)
```

#### Query for Objects Based on User and Specific Extensions

```python
field_names = ['uid', 'extension', 'size']
filters = {
    'uid': ['eq 1000', 'eq 555'],
    'extension': ['eq log', 'eq ldb']  # looking for log and ldb files
}
table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)
df = table.to_pandas()
print(df)
```

#### Query for Specific File Types with Size Constraints

```python
field_names = ['element_type', 'size', 'name']
filters = {
    'element_type': ['eq FILE'],
    'size': ['gt 50000', 'lt 1000000']  # size between 50 KB and 1 MB
}
table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)
df = table.to_pandas()
print(df)
```

#### Query for Large TABLE Objects by Specific Users

```python
field_names = ['uid', 'owner_name', 'size', 'element_type']
filters = {
    'uid': ['eq 555'],
    'element_type': ['eq TABLE'],
    'size': ['gt 10000000']  # greater than 10 MB
}
table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names)
df = table.to_pandas()
print(df)
```
