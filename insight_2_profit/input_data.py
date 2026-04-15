from pyspark.sql import SparkSession, DataFrame


def _read_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    return spark.read.csv(csv_path, header=True)


def _write_raw(jdbc_url, table_name, df: DataFrame):
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
    )


def load_products(jdbc_url, spark: SparkSession):
    products_df = _read_csv(spark, "input/products.csv")
    _write_raw(jdbc_url, "raw_products", products_df)


def load_sales_order_headers(jdbc_url, spark: SparkSession):
    header_df = _read_csv(spark, "input/sales_order_header.csv")
    _write_raw(jdbc_url, "raw_sales_order_header", header_df)


def load_sales_order_details(jdbc_url, spark: SparkSession):
    detail_df = _read_csv(spark, "input/sales_order_detail.csv")
    _write_raw(jdbc_url, "raw_sales_order_detail", detail_df)
