import polars as pl
import duckdb
import pyarrow as pa

    
def unity_catalog_delta_to_polars(spark: str, 
                                  table_name: str,
                                  sql_filter: str = None
                                  ) -> pl.DataFrame:
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    if not sql_filter:
        pandas_df = spark.sql(f"SELECT * FROM {table_name}").toPandas()
    elif sql_filter:
        pandas_df = spark.sql(f"SELECT * FROM {table_name} WHERE {sql_filter}").toPandas()
    polars_df  = pl.from_pandas(pandas_df)
    return polars_df


def unity_catalog_delta_register_to_duckdb(spark: str, 
                                  unity_table_name: str,
                                  duck_table_name: str,
                                  sql_filter: str = None
                                  ):
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    duckdb.default_connection.execute("SET GLOBAL pandas_analyze_sample=100000")
    if not sql_filter:
        duckdb.register(duck_table_name, spark.sql(f"SELECT * FROM {unity_table_name}").toPandas())
    elif sql_filter:
        duckdb.register(duck_table_name, spark.sql(f"SELECT * FROM {unity_table_name} WHERE {sql_filter}").toPandas())


def unity_catalog_delta_to_pyarrow(spark: str, 
                                  unity_table_name: str,
                                  sql_filter: str = None
                                  ):
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    if not sql_filter:
        pandas_df = spark.sql(f"SELECT * FROM {unity_table_name}").toPandas()
    elif sql_filter:
        pandas_df = spark.sql(f"SELECT * FROM {unity_table_name} WHERE {sql_filter}").toPandas()
    pyarrow_table  = pa.Table.from_pandas(pandas_df)
    return pyarrow_table