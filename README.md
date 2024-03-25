## LakeScum
<img src="https://github.com/danielbeach/lakescum/blob/main/imgs/lakescum.webp" width="300">

A Python pacakge to help Databricks Unity Catalog users to read and query
Delta Lake tables with `Polars`, `DuckDb`, or `PyArrow`.

Unity Catalog does not place nice out-of-the-box with many of
these tools using built in features like `polars.read_delta()` for
example.

`LakeScum` takes that difficulty away.

### Installation
`LakeScum` can be installed for Python with a simple `pip` command.

`pip install lakescum` 


### Usage
There are currently the methods to read and query a Unity Catalog `Delta Lake` with ...

- `Polars`
- `DuckDb`
- `PyArrow`


### Polars
You can query and return a `Polars` Dataframe from a Unity Catalog `Delta Lake` table with
the following method.
`unity_catalog_delta_to_polars()`

It takes 2 required parameters, and one optional.
```
spark: str - Spark Session 
table_name: str - Unity Catalog table name
sql_filter - Optional SQL WHERE clause filter
```

Example ...
```
polars_df = unity_catalog_delta_to_polars(spark, 
    'production.default.fact_orders',
    sql_filter="year = 2024 and month = 3 and day = 10")

print(polars_df.head(10))

order_id | product_id | order_date | quantity
1 | 4567 | '2024-03-10' | 5
```

##### DuckDb
This method will register a Unity Catalog Delta Table as a `DuckDB` table so you
can query it with `DuckDB`.
`unity_catalog_delta_register_to_duckdb()`

It takes 3 required parameters, and one optional.
```
spark: str - Spark Session 
unity_table_name: str - Unity Catalog table name
duck_table_name: str - Desired DuckDB table name
sql_filter - Optional SQL WHERE clause filter
```

Example ...
```
unity_catalog_delta_register_to_duckdb(spark, 
    "production.default.fact_orders",
    "test", 
    sql_filter="year = 2024 and month =3 and day = 19")

results = duckdb.sql("SELECT * FROM test")

print(results)
order_id | product_id | order_date | quantity
1 | 4567 | '2024-03-10' | 5
```

##### PyArrow
This method will return a `PyArrow` Table from a Unity Catalog Delta Table.
`unity_catalog_delta_to_pyarrow()`

It takes 2 required parameters, and one optional.

```
pa = unity_catalog_delta_to_pyarrow(spark,
 "production.default.fact_orders",
  sql_filter="year = 2024 and month =3 and day = 19")

print(pa)
order_id | product_id | order_date | quantity
1 | 4567 | '2024-03-10' | 5
```