from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import trim, col


@dataclass(frozen=True)
class MigrationProcessed:
    success: bool
    table_name: str
    rows_processed: int = 0
    error: str | None = None
    duplicates_found: DataFrame | None = None


def clean_dataframe(df: DataFrame, schema) -> DataFrame:
    cleaned_df = df
    for field in schema.fields:
        column_name = field.name
        if field.dataType.typeName() == "string":
            cleaned_df = cleaned_df.withColumn(column_name, trim(col(column_name)))

        cleaned_df = cleaned_df.withColumn(
            column_name, col(column_name).cast(field.dataType)
        )

    return cleaned_df.select(*[f.name for f in schema.fields])


def migrate_products_to_store_table(
    spark: SparkSession, jdbc_url: str
) -> MigrationProcessed:
    store_table = "store_products"
    try:
        raw_df = spark.read.jdbc(url=jdbc_url, table="raw_products")
        store_schema = spark.read.jdbc(url=jdbc_url, table=store_table).schema

        cleaned_df = clean_dataframe(raw_df, store_schema)
        final_df = cleaned_df.dropDuplicates(["ProductID"])

        final_df.write.option(
            "preActions",
            f"""
            BEGIN;
            ALTER TABLE {store_table} DISABLE TRIGGER ALL;
            TRUNCATE TABLE {store_table};
            ALTER TABLE {store_table} ENABLE TRIGGER ALL;
            COMMIT;
            """,
        ).jdbc(
            url=jdbc_url,
            table=store_table,
            mode="append",
            properties={
                "stringtype": "unspecified",
            },
        )

        return MigrationProcessed(
            success=True,
            table_name=store_table,
            rows_processed=final_df.count(),
            duplicates_found=cleaned_df.exceptAll(final_df),
        )
    except Exception as error:
        return MigrationProcessed(
            success=False, table_name=store_table, error=str(error)
        )


def migrate_sales_order_header_to_store_table(
    spark: SparkSession, jdbc_url: str
) -> MigrationProcessed:
    store_table = "store_sales_order_header"
    try:
        raw_df = spark.read.jdbc(url=jdbc_url, table="raw_sales_order_header")
        store_schema = spark.read.jdbc(url=jdbc_url, table=store_table).schema

        cleaned_df = clean_dataframe(raw_df, store_schema)
        final_df = cleaned_df.dropDuplicates(["SalesOrderID"])

        final_df.write.option(
            "preActions",
            f"""
            BEGIN;
            ALTER TABLE {store_table} DISABLE TRIGGER ALL;
            TRUNCATE TABLE {store_table};
            ALTER TABLE {store_table} ENABLE TRIGGER ALL;
            COMMIT;
            """,
        ).jdbc(
            url=jdbc_url,
            table=store_table,
            mode="append",
            properties={
                "stringtype": "unspecified",
            },
        )

        return MigrationProcessed(
            success=True,
            table_name=store_table,
            rows_processed=final_df.count(),
            duplicates_found=cleaned_df.exceptAll(final_df),
        )
    except Exception as error:
        return MigrationProcessed(
            success=False, table_name=store_table, error=str(error)
        )


def migrate_sales_order_detail_to_store_table(
    spark: SparkSession, jdbc_url: str
) -> MigrationProcessed:
    store_table = "store_sales_order_detail"
    try:
        raw_df = spark.read.jdbc(url=jdbc_url, table="raw_sales_order_detail")
        products_df = spark.read.jdbc(url=jdbc_url, table="store_products")
        store_schema = spark.read.jdbc(url=jdbc_url, table=store_table).schema

        cleaned_df = clean_dataframe(raw_df, store_schema)

        valid_df = cleaned_df.join(products_df, "ProductID", "left_semi")

        final_df = valid_df.dropDuplicates(["SalesOrderDetailID"])

        final_df.write.option(
            "preActions",
            f"""
            BEGIN;
            ALTER TABLE {store_table} DISABLE TRIGGER ALL;
            TRUNCATE TABLE {store_table};
            ALTER TABLE {store_table} ENABLE TRIGGER ALL;
            COMMIT;
            """,
        ).jdbc(
            url=jdbc_url,
            table=store_table,
            mode="append",
            properties={
                "stringtype": "unspecified",
            },
        )

        return MigrationProcessed(
            success=True,
            table_name=store_table,
            rows_processed=final_df.count(),
            duplicates_found=valid_df.exceptAll(final_df),
        )
    except Exception as error:
        return MigrationProcessed(
            success=False, table_name=store_table, error=str(error)
        )


def process_all_raw_to_store(
    spark: SparkSession, jdbc_url: str
) -> list[MigrationProcessed]:
    results = [
        migrate_products_to_store_table(spark, jdbc_url),
        migrate_sales_order_header_to_store_table(spark, jdbc_url),
        migrate_sales_order_detail_to_store_table(spark, jdbc_url),
    ]
    return results
