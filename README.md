# **VastdbApi Documentation**

**NOTE**
- Please note that this API & the documentation is currently in a pre-release stage. Until version 1.x is officially released, the API should be considered unstable.

## VastdbApi - Content Overview

- **[Introduction](#what-is-a-vastdbapi)**
- **[Getting Started](#getting-started)**
   - **[Requirements](#requirements)**
   - **[Install](#install-the-vastdbapi)**
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
   - **[Import a parquet file from S3 bucket](#import-a-parquet-file-from-s3-bucket)**
   - **[List columns and data from a table](#list-columns-from-a-table)**
   - **[Query data with filters](#query-data-with-filters)**
   - **[Table population flow](#combined-example-with-a-few-functions)**
- **[Interacting with Vast Catalog](#interracting-with-vast-catalog-using-vastdbapi)**
   - **[What is Vast Catalog](#what-is-vast-catalog)**
   - **[Vast Catalog Schema Structure](#vast-catalog-schema-structure)**
   - **[Vast Catalog - Query examples with VastdbAPI](#vast-catalog-query-examples-with-vastdbapi)**
      - **[Basic counts of things](#basic-counts-of-things)**
         - **[Parallel Pagination execution example](#simplified-example-of-count-of-elements-returned-from-parallel-execution)**
      - **[Simple Filtering](#simple-filtering)**
      - **[Timestamp Filtering](#timestamp-filtering)**
      - **[Reporting](#reporting)**
      - **[Catalog Snapshots Comparisons](#catalog-snapshots-comparisons)**

  

## What is a VastdbApi

**`VastdbApi`** is a Python based API designed for interacting with VastDB & Vast Catalog, enabling operations such as schema and table management, data querying, and transaction handling.
Key libraries used in this API include requests for HTTP requests, pyarrow for handling Apache Arrow data formats, and flatbuffers for efficient serialization of data structures.


## Getting started

### Requirements

- Linux / Windows / MacOS client server connected on data access with VAST cluster
- Virtual IP pool configured with DNS service / List of all availble VIP's
  - [Network Access through Virtual IPs](https://support.vastdata.com/hc/en-us/articles/5140602978844-Configuring-Network-Access)
- VIP DNS name - for reaching all availble Cnodes in the API requests
  - [Configuring the VAST Cluster DNS Service](https://support.vastdata.com/hc/en-us/articles/9859957831452-Configuring-the-VAST-Cluster-DNS-Service)
- Python 3.7 or above
- S3 User Access & Secret Keys on VAST cluster
  - [Managing S3 User Access ](https://support.vastdata.com/hc/en-us/articles/9859972743580#UUID-6a15026a-d0bd-ebe6-e281-4b980674cecc_section-idm4577174596542432775810296988)
- Tabular Identity Policy with the proper permissions 
  - [Creating Identity Policies](https://support.vastdata.com/hc/en-us/articles/9859958983708#UUID-c381c613-81bd-3c09-69c4-ba4dcd8e8d6d)


### Install the VastdbApi


#### Prepare your python env
- Make sure you have python 3.7 or above
- Recommended to have the latest pip and setuptools:
  - `pip install --upgrade pip setuptools`

#### Install the 'vastdb' package
```
pip install vastdb
```


## Creating the initial session with VastdbApi:


### Understending Multithreaded API functions:

- Multithreading in the VastDB API is by default, if a range of VIP's was provided.

##### **Session Creation with Multiple VIPs:**

- When initializing the VastDB session (VastdbApi class), use the `host` parameter to specify a range of VIP addresses or a list.
  - Range of VIP's between 172.25.54.1 to 172.25.54.16: `172.25.54.1:172.25.54.16`
  - A list of VIP's: `172.25.54.1,172.25.54.2`

- This format automatically expands to include each VIP in the specified range, which may be disributed on different Cnodes in your cluster.
- Automatic Load Balancing: Each VIP address(or DNS name) corresponds to a separate thread, VastDB API automatically distributes the workload across these threads.
- Group of threads operates on different Cnodes, allowing parallel processing and faster execution.
- Multithreaded setup enhances the performance of **[query](#query)**, **[query_iterator](#query_iterator)**, and **[insert](#insert)** functions.


### Initializing a session with VastdbApi:


```python
import pyarrow as pa
from vastdb.api import VastdbApi

def create_vastdb_session(access_key, secret_key):
    return VastdbApi(host='172.25.54.1:172.16.54.16', access_key=access_key, secret_key=secret_key)


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
  - `new_name` (str): New name for the schema.
  - `schema_properties` (str, optional): Optional schema metadata - buffer of up to 4k
  - `tenant_id` (int, optional): Tenant ID (default is `0`). 
  - `txid` (int, optional): Transaction ID (default is `0`).
- **Example**:
```python
  vastdb_session.alter_schema(bucket_name, schema_name, new_name='renamed_schema')
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
  - `name` (str): Name of the table to create.
  - `arrow_schema` (pyarrow.Schema): A pyarrow schema which defines the table columns
  - `txid` (int, optional): Transaction ID (default is `0`).  
- **Example**:
```python
  arrow_schema = pa.schema([('column1', pa.int32()), ('column2', pa.string())])
  vastdb_session.create_table(bucket_name, schema_name, 'new_table', arrow_schema=arrow_schema)
```

#### `create_table_from_parquet_file`

- **Usage**: Create a new table using the schema of a specified parquet file.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `name` (str): Name of the table to create.
  - 'parquet_path' (str, optional): Parquet file from which to build the pyarrow.Schema which define the table columns
  - 'parquet_bucket_name' (str, optional): Name of bucket that contains the Parquet object from which to build the pyarrow.Schema which define the table columns
  - 'parquet_object_name' (str, optional): Name of the Parquet object from which to build the pyarrow.Schema which define the table columns
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `client_tags` (list of str, optional): Strings identifying the client (default is empty `[]`).

- **Example**:
```python
  # Create from parquet file on a filesystem
  vastdb_session.create_table(bucket_name, schema_name, 'new_table', parquet_path='path/to/file.parquet')
  # Create from parquet object on as s3 bucket
  vastdb_session.create_table(bucket_name, schema_name, 'new_table',
                              parquet_bucket_name='s3-bucket-name',
                              parquet_object_name='path/to/object.parquet')
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
  - `name` (str): Name of the table to alter.
  - `new_name` (str): New name for the table.
  - `table_properties` (str, optional)): Optional table metadata - buffer of up to 4k
  - `txid` (int, optional)): Transaction ID (default is `0`).  
  - `tenant_id` (int, optional)): Tenant ID (default is `0`). 
- **Example**:
```python
  vastdb_session.alter_table(bucket_name, schema_name, name='my_table', new_name='renamed_table')
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
  txid=0
  res = vastdb_session.begin_transaction()
  txid = res.headers.get('tabular-txid')
  print(res)
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

#### `query`

- **Usage**: Execute a data query on a specified table within a bucket and schema. This function allows for complex queries, including filters and projections, on the table data.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table to query.
  - `num_sub_splits` (int, optional): Number of sub-splits for the query (default: `1`).
  - `response_row_id` (bool, optional): Whether to include row IDs in the response (default: `False`).
  - `txid` (int, optional): Transaction ID (default is `0`).
  - `limit` (int, optional): Limit on the number of rows to return (default: `0`).
  - `filters` (dict, optional): A dictionary whose keys are column names, and values are lists of string expressions that represent filter conditions on the column.
  - `filed_names` (list, optional): A list of column names to be returned in the output table

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
  - OR of filters on the same column is done as follows `filters = {'Citizen_Age': ['eq 38', 'eq 90')]`
  - AND of filters on the same column requires () around the filter, such as `filters = {'Citizen_Age': [('gt 38', 'lt 90')]`
  - It is possible to filter and project on a nested data type whose full ancesestry are structs.
  - If using pandas dataframe, pandas predicates can also be used. (Perfomance might be reflected because it's not API-native filters)
    - **Example** [Query a table using Pandas predicates](#query-a-table-using-pandas-predicates)
  - It is possible to filter and project on a nested data type whose full ancesestry are structs.

- **Example**:
```python
  # i.e. "SELECT Citizen_Age, Citizen_Name, Is_married, Citizen_Address.Street, Citizen_Address.Number FROM table WHERE Citizen_Age > 38 AND Citizen_Age < 90 AND Citizen_Address.Street = 'Sesame' LIMIT 10"

  field_names = ['Citizen_Age', 'Citizen_Name', 'Is_married', 'Citizen_Address.Street', 'Citizen_Address.Number']
  filters = {'Citizen_Age': [('gt 38', 'lt 90')], 'Citizen_Address.Street': ['eq Sesame']}

  table = vastdb_session.query(bucket_name, schema_name, table_name, filters=filters, field_names=field_names, limit=10, num_sub_splits=10)
  print(table)
```
  **[See more advanced examples on how to query data](#advanced-examples)**


#### `query_iterator`

- **Usage**: Iteratively execute a data query, across multiple splits and subsplits.
   - This function is designed for efficient data retrieval from large datasets.
   - Allowing for parallel processing and handling large volumes of data that might not fit into memory if loaded all at once.
- **Parameters**:
  - `bucket` (str): Name of the bucket where the table is stored.
  - `schema` (str): Name of the schema within the bucket.
  - `table` (str): Name of the table to perform the query on.
  - `num_sub_splits` (int, optional): The number of subsplits within each split. (default: 1)
  - `response_row_id` (bool, optional): If set to True, the query response will include a column with the internal row IDs of the table (default: False).
  - `txid` (int, optional): Transaction ID for the query.
  - `filters` (dict, optional): A dictionary whose keys are column names, and values are lists of string expressions that represent filter conditions on the column.
  - `filed_names` (list, optional): A list of column names to be returned in the output table
  - `record_batch` - PyArrow chunk objects for each subsplit

- **Returns**:
A generator that yields PyArrow `record_batch` objects for each subsplit.
   - Each `record_batch` contains a portion of the query result, allowing the user to process large datasets in smaller, manageable chunks.

- **Example**:
```python
filters = {'column_name': ['eq value1', 'lt value2']}

for record_batch in vastdb_session.query_iterator('my_bucket', 'my_schema', 'my_table', num_sub_splits=8, filters=filters):
        # Process each record batch as needed
        df = record_batch.to_pandas()
        # Perform operations on DataFrame
        print(df)
```


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


#### `insert`

- **Usage**: Insert rows into a table.
- **Parameters**:
  - `bucket` (str): Name of the bucket.
  - `schema` (str): Name of the schema.
  - `table` (str): Name of the table.
  - `rows` (dict): Array of cell values to insert. Dict-key are columns & Dict-values are values, i.e `{'column': ['value', 'value']}`
  - `record_batch` (pyarrow.RecordBatch): pyarrow RecordBatch to insert
  - `rows_per_insert`: (int, optional) Split the operation so that each insert command will be limited to this value. default: None (will be selected
  automatically)
  - `txid` (int, optional): Transaction ID.
- **Example**:
```python
  vastdb_session.insert(bucket_name, schema_name, table_name, {'name': ['Alice','Bob'], 'age': [25,24]})
  vastdb_session.insert(bucket_name, schema_name, table_name, record_batch)
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
  from vastdb.api import build_record_batch

  column_to_delete = [(pa.uint64(), '$row_id')]  
  delete_rows = [9963, 9964] # row's id's to delete
  delete_rows_req = build_record_batch(column_to_delete, {pa.uint64(): delete_rows})

  vastdb_session.delete_rows(bucket_name, schema_name, table_name, record_batch=delete_rows_req)
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

- **Usage**: List all classes and functions in the vastdb.api
- **Example**:
```python
  help(VastdbApi)
```


## Advanced Examples


### Import a parquet file from S3 bucket

```python
s3_files = {
    ('s3-bucket-name', 'citizens_data.parquet'): b''  # 's3-bucket-name', 'file-name.parquet'
}

result = vastdb_session.import_data(
    'vastdb-bucket-name',
    'schema-name',
    'table-name',
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


vastdb_session = create_vastdb_session(access_key, secret_key)

# List columns in the table
cols = vastdb_session.list_columns('bucket_name', 'schema_name', 'table_name')

# Create a schema from the column list
res_schema = pa.schema([(column[0], column[1]) for column in cols[0]])
print(res_schema)
```

## Query data with filters

##### Query table without limit, filters and field_names, i.e. "SELECT * FROM table"

```python

table = vastdb_session.query('vastdb-bucket1', 'citizens-schema', 'jersey-citizens-table')
df = table.to_pandas()
print(df)
```

##### (Citizen_Age > 35) AND (Citizen_experience <= 9) limit 10

```python
field_names = ['Citizen_Age', 'Citizen_Name', 'Citizen_experience']
filters = {'Citizen_Age': ['gt 35'], 'Citizen_experience': ['le 9']}

table = vastdb_session.query('vastdb-bucket1', 'citizens-schema', 'jersey-citizens-table', filters=filters, field_names=field_names, limit=10)

df = table.to_pandas()
print(df)
```

##### (Citizen_Age = 50) OR (Citizen_Age = 55)

```python
field_names = ['Citizen_Age', 'Citizen_Name', 'Citizen_experience']
filters = {'Citizen_Age': ['eq 50', 'eq 55']}

table = vastdb_session.query('vastdb-bucket1', 'citizens-schema', 'jersey-citizens-table', filters=filters, field_names=field_names)

df = table.to_pandas()
print(df)
```

##### (Citizen_experience => 25) AND (Citizen_Age between 55 to 75)

```python
field_names = ['Citizen_Age', 'Citizen_Name', 'Citizen_experience', 'Citizen_id']
filters = {'Citizen_experience': ['le 25'], 'Citizen_Age': [('ge 55' , 'le 75')]}
table = vastdb_session.query('vastdb-bucket1', 'citizens-schema', 'jersey-citizens-table', filters=filters, field_names=field_names)

df = table.to_pandas()
print(df)
```

##### Query a table using Pandas predicates:

```python
table = vastdb_session.query('bucket_name', 'schema_name', 'table_name')
df = table.to_pandas()
pd.options.display.max_columns = None
display(df[(df['uid'] == 555) & (df['size'] > 4096)].head(10))
```


## Combined example with a few functions

#### Simulating a table population flow using pandas and pyarrow
  - To simulate the same flow, run the examples below one by one

```python

import pyarrow as pa
from vastdb.api import VastdbApi

def create_vastdb_session(access_key, secret_key):
    return VastdbApi(host='pool1.vast55-kfs.vasteng.lab', access_key=access_key, secret_key=secret_key)


access_key='83SKB7G...'
secret_key='00k0oQ4eG6o4/PLGAWL..'

vastdb_session = create_vastdb_session(access_key, secret_key)


# Create the schema
response = vastdb_session.create_schema('bucket_name', 'schema_name')
if response.status_code == 200:
    print("create_schema successful")
else:
    print("create_schema failed:", response)
    exit(1)


# Define columns schema (types) & create table with the columns included

arrow_schema = pa.schema([
        ('Citizen_Age', pa.int64()),
        ('Citizen_Name', pa.string()),
        ('Citizen_experience', pa.float64()),
        ('Is_married', pa.bool_()),
    ])

response = vastdb_session.create_table('bucket_name', 'schema_name', 'table_name', arrow_schema)
if response.status_code == 200:
    print("create_table successful")
else:
    print("create_table failed:", response)
    exit(1)


# INSERT DATA TO THE CREATED COLUMNS

rows = {
        'Citizen_Name': ['Alice', 'Bob', 'Koko', 'Menny'],
        'Citizen_Age': [45, 38, 27, 51],
        'Citizen_experience': [25.5, 17.9, 5.3, 28.2],
        'Is_married': [True, False, False, True]
}

vastdb_session.insert('bucket_name', 'schema_name', 'table_name', rows=rows)

# query table without limit, filters and field_names, i.e. "SELECT * FROM table"

table = vastdb_session.query('bucket_name', 'schema_name', 'table_name')
df = table.to_pandas()
print(df)

# Update rows id 0 & 2 with, on 2 specific columns with new values
columns_to_update = [(pa.uint64(), '$row_id'), (pa.int64(), 'Citizen_Age'), (pa.bool_(), 'Is_married')]
column_values_to_update = {pa.uint64(): [0, 2], pa.int64(): [43, 28], pa.bool_(): [False, True]}

update_rows_req = build_record_batch(columns_to_update, column_values_to_update)
vastdb_session.update_rows('bucket_name', 'schema_name', 'table_name', record_batch=update_rows_req)

table = vastdb_session.query('bucket_name', 'schema_name', 'table_name')
print(f'table after update: {table.to_pydict()}')

# Add additional columns to the table schema
additional_columns_schema = pa.schema([('Citizen_id', pa.int64()), ('Citizen_street', pa.string())])
response = vastdb_session.add_columns('bucket_name', 'schema_name', 'table_name', additional_columns_schema)
if response.status_code == 200:
    print("add_columns successful")
else:
    print("add_columns failed:", response)
    exit(1)

# retrieve columns and new schema
cols = vastdb_session.list_columns('bucket_name', 'schema_name', 'table_name')
res_schema = pa.schema([(column[0], column[1]) for column in cols[0]])
print(res_schema)


# INSERT DATA TO THE ADDED COLUMNS

rows = {
        'Citizen_Name': ['Alice', 'Bob', 'Koko', 'Menny'],
        'Citizen_Age': [45, 38, 27, 51],
        'Citizen_experience': [25.5, 17.9, 5.3, 28.2],
        'Is_married': [True, False, False, True],
        'Citizen_id': [222333, 333222, 444333, 555444],
        'Citizen_street': ['street1', 'street4', 'street3', 'street2']
}

vastdb_session.insert('bucket_name', 'schema', 'table_name', rows=rows)


# query table without limit, filters and field_names, i.e. "SELECT * FROM table"

table = vastdb_session.query('bucket_name', 'schema_name', 'table_name')
df = table.to_pandas()
print(df)

# Import multiple parquet files from s3 bucket to your table

s3_files = {('parquet-files-bucket', 'citizens_data_24.parquet'): b'',
            ('parquet-files-bucket', 'citizens_data_25.parquet'): b'',
            ('parquet-files-bucket', 'citizens_data_26.parquet'): b'',
            ('parquet-files-bucket', 'citizens_data_27.parquet'): b'',
           }
response = vastdb_session.import_data('bucket_name', 'schema_name', 'table_name', source_files=s3_files)
if response.status_code == 200:
    print("import_data successful")
else:
    print("import_data failed:", response)
    exit(1)


# query table after importing files, without limit, filters and field_names, i.e. "SELECT * FROM table"

table = vastdb_session.query('bucket_name', 'schema_name', 'table_name')
df = table.to_pandas()
print(df)

```

## Interracting with VAST Catalog using VastdbApi

### What is Vast Catalog

  - VAST Catalog is a database that indexes metadata attributes of all data on the cluster from periodic snapshots of the cluster's data. The database is stored on a dedicated S3 bucket on the cluster.
  - [VAST Catalog Overview](https://support.vastdata.com/hc/en-us/articles/9859973783964-VAST-Catalog-Overview)


### Vast Catalog Schema Structure

  - The Vast-Catalog has internal bucket, schema and table. To query the Catalog using the api, use these values:

  ```python
  'vast-big-catalog-bucket'
  'vast_big_catalog_schema'
  'vast_big_catalog_table'
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


### Basic counts of things

##### Initiate session with the catalog:

```python
field_names = ['element_type'] # Only need the element_type field for counting
table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', field_names=field_names, num_sub_splits=8)
df = table.to_pandas()
```

##### How many elements are in the catalog

```python
total_elements = len(df)
print(f"Total elements in the catalog: {total_elements}")
```

##### How many files/objects?

```python
file_count = len(df[df['element_type'] == 'FILE'])
print(f"Number of files/objects: {file_count}")
```

##### How many directories?

```python
dir_count = len(df[df['element_type'] == 'DIR'])
print(f"Number of directories: {dir_count}")
```

##### Database tables?

```python
table_count = len(df[df['element_type'] == 'TABLE'])
print(f"Number of database tables: {table_count}")
```

##### What are all of the elements on my system anyway?

```python
distinct_elements = df['element_type'].unique()
print("Distinct element types on the system:")
print(distinct_elements)
```

### Simplified example of count of elements returned from parallel execution

   - The `query_iterator` iteratively executes a query on a database table, returning results in chunks as PyArrow RecordBatches, enabling efficient handling of large datasets by processing data in smaller, manageable segments.
   - Simplified example of count of elements returned from parallel execution.

```python
def query_and_count_elements(session, bucket, schema, table, field_names):
    elements_count = 0

    for record_batch in session.query_iterator(bucket, schema, table, field_names=field_names, num_sub_splits=8):
        elements_count += len(record_batch)

    return elements_count

# Query Parameters
field_names = ['element_type']  # Only need the element_type field for counting

# Perform the query
total_elements = query_and_count_elements(
    vastdb_session, 'vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', field_names
)
print(f"Total elements in the catalog: {total_elements}")
```


### Simple Filtering


##### Query for Files Owned by vastdata with Size Greater Than 50000 and Created After a Specific Date:

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

table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)

df = table.to_pandas()
print(df)
```

##### Query for Specific File Types Across Different Users:

```python
field_names = ['uid', 'owner_name', 'element_type']
filters = {
    'element_type': ['eq FILE', 'eq TABLE', 'eq DIR'],
    'uid': ['eq 500', 'eq 1000']
}
table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)
df = table.to_pandas()
print(df)
```

##### Query for Objects Based on User and Specific Extensions

```python
field_names = ['uid', 'extension', 'size']
filters = {
    'uid': ['eq 1000', 'eq 555'],
    'extension': ['eq log', 'eq ldb']  # looking for log and ldb files
}
table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)
df = table.to_pandas()
print(df)
```

##### Query for Specific File Types with Size Constraints

```python
field_names = ['element_type', 'size', 'name']
filters = {
    'element_type': ['eq FILE'],
    'size': ['gt 50000', 'lt 1000000']  # size between 50 KB and 1 MB
}
table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)
df = table.to_pandas()
print(df)
```

##### Query for Large TABLE Objects by Specific Users

```python
field_names = ['uid', 'owner_name', 'size', 'element_type']
filters = {
    'uid': ['eq 555'],
    'element_type': ['eq TABLE'],
    'size': ['gt 10000000']  # greater than 10 MB
}
table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)
df = table.to_pandas()
print(df)
```

### Timestamp Filtering

##### Query by birthdate: VAST uses a “creation_time” column to indicate when a new element is created:
  - This will output all objects linked after noon on September 1st. It will not output files that have been moved to a new path.

```python
# i.e: SELECT CONCAT(parent_path, name) FROM vast_big_catalog_table WHERE creation_time > TIMESTAMP '2023-09-01 12:00:01'

# Set the timestamp for comparison
timestamp_birthdate = pd.Timestamp('2023-09-01 12:00:01')

# Convert the timestamp to an integer
timestamp_birthdate_int = int(timestamp_birthdate.timestamp())

# Query the database
field_names = ['creation_time', 'parent_path', 'name']
filters = {'creation_time': [f'gt {timestamp_birthdate_int}']}
table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)
df = table.to_pandas()

# Filter and concatenate paths
df_filtered = df[df['creation_time'] > timestamp_birthdate]
df_filtered['full_path'] = df_filtered['parent_path'] + df_filtered['name']

# Print result
print("Objects created after 2023-09-01 12:00:01:")
display(df_filtered['full_path'])
```
  - **NOTE** : Same method can be applied for acces-time (atime), modification-time (mtime) & metadata-update-times (ctime).


### Reporting


##### Simple queries to tell you basic statistics on a section of the namespace
  -  Report statistics on parts of the namespace
    -  Summarizing files of a certain type (FILE), belonging to a specific user (uid=555), and located in a certain path (/parquet-files-bucket)

```python
import numpy as np

# Query the database
field_names = ['uid', 'used', 'size']
filters = {
    'search_path': ['eq /parquet-files-bucket'],
    'uid': ['eq 555'],
    'element_type': ['eq FILE']
}
table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)
df = table.to_pandas()

# Check if DataFrame is empty
if df.empty:
    print("No data returned from query. Please check filters and field names.")
else:
    # Perform aggregations
    users_count = df['uid'].nunique()
    files_count = len(df)
    kb_used_sum = df['used'].sum() / 1000
    avg_size_kb = df['size'].mean() / 1000

    # Formatting results
    formatted_results = {
        'users': f"{users_count:,d}",
        'Files': f"{files_count:,d}",
        'KB_Used': f"{kb_used_sum:,.0f}",
        'Avg_Size_KB': f"{avg_size_kb:,.2f}"
    }

    # Print formatted results
    print("Aggregated Results:")
    print(formatted_results)
```

##### Capacity Grouping & Usage report
  - Here’s a report on all the users on the system:
      - Get Files across whole system('/'), group by owner_name, sum files, total and average size in kilobytes, oldest creation
        time, and most recent access time for each file owner.
      - Note - `display` is a `IPython` function which aggregates results in table format

```python
from IPython.display import display

# Querying the database
filters = {
    'element_type': ['eq FILE'],
    'search_path': ['eq /']
}
field_names = ['owner_name', 'used', 'size', 'creation_time', 'atime']

table = vastdb_session.query('vast-big-catalog-bucket', 'vast_big_catalog_schema', 'vast_big_catalog_table', filters=filters, field_names=field_names, num_sub_splits=8)
df = table.to_pandas()
pd.options.display.max_columns = None

# Aggregating data
aggregated_data = df.groupby('owner_name').agg(
    Files=('owner_name', 'count'),
    KB_Used=('used', lambda x: np.sum(x)/1000),
    Avg_Size_KB=('size', lambda x: np.mean(x)/1000),
    Oldest_data=('creation_time', 'min'),
    Last_access=('atime', 'max')
)

# Formatting results
aggregated_data['Files'] = aggregated_data['Files'].apply(lambda x: f"{x:,d}")
aggregated_data['KB_Used'] = aggregated_data['KB_Used'].apply(lambda x: f"{x:,.0f}")
aggregated_data['Avg_Size_KB'] = aggregated_data['Avg_Size_KB'].apply(lambda x: f"{x:,.2f}")

display(aggregated_data)
```

### Catalog Snapshots Comparisons

  - You can access catalog snapshot by navigating the schema space.
  - The most obvious use of snapshot comparisons is delete detection, followed by move detection.

##### Delete detection
  - Query Returns: This script compares the current state with a specific historical snapshot, identifying files present in the current table but not in the snapshot, based on their element_type and search_path.
  - Access to Snapshot: Access to a snapshot works by querying a specific schema directory (representing the snapshot) within the bucket

```python
def query_table(schema):
    table = vastdb_session.query('vast-big-catalog-bucket', schema, 'vast_big_catalog_table', filters=filters, num_sub_splits=8)
    df = table.to_pandas()
    df['full_path'] = df['parent_path'] + df['name']
    return set(df['full_path'])

# Query Filters
filters = {
    'element_type': ['eq FILE'],
    'search_path': ['eq /']
}

# Query the current table and the snapshot
current_set = query_table('vast_big_catalog_schema')
snapshot_set = query_table('.snapshot/bc_table_2023-12-10_13_53_36/vast_big_catalog_schema')

# Find differences (Current Table vs Snapshot)
difference = current_set - snapshot_set

# Output
if difference:
    print(f"[INFO] Found {len(difference)} files in the current table but not in the snapshot:")
    for item in difference:
        print(item)
else:
    print("[INFO] No differences found")
```
