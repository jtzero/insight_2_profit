from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, when
from dataclasses import dataclass


@dataclass(frozen=True)
class OrdersPublished:
    success: bool
    rows_processed: int = 0
    error: Exception | None = None


SUNDAY = 1
SATURDAY = 7


def calculate_business_days(order_date_col: str, ship_date_col: str):
    business_days = expr(
        f"""
        size(
            filter(
                sequence({order_date_col}, {ship_date_col}),
                x -> dayofweek(x) NOT IN ({SUNDAY}, {SATURDAY})
            )
        ) - 1
    """
    )
    return when(
        col(order_date_col).isNull() | col(ship_date_col).isNull(), None
    ).otherwise(business_days)


def transform_sales_orders(detail_df: DataFrame, header_df: DataFrame) -> DataFrame:
    joined_df = detail_df.join(header_df, "SalesOrderID")

    joined_df = joined_df.withColumn(
        "LeadTimeInBusinessDays", calculate_business_days("OrderDate", "ShipDate")
    )

    joined_df = joined_df.withColumn(
        "TotalLineExtendedPrice",
        col("OrderQty") * (col("UnitPrice") - col("UnitPriceDiscount")),
    )

    joined_df = rename_freight_to_total_order_freight(joined_df)

    return joined_df


def rename_freight_to_total_order_freight(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("Freight", "TotalOrderFreight")


def process(spark: SparkSession, jdbc_url: str) -> OrdersPublished:
    try:
        detail_df = spark.read.jdbc(url=jdbc_url, table="store_sales_order_detail")
        header_df = spark.read.jdbc(url=jdbc_url, table="store_sales_order_header")

        published_df = transform_sales_orders(detail_df, header_df).drop("SalesOrderID")

        published_df.write.option(
            "preActions",
            """
            BEGIN;
            ALTER TABLE publish_orders DISABLE TRIGGER ALL;
            TRUNCATE TABLE publish_orders;
            ALTER TABLE publish_orders ENABLE TRIGGER ALL;
            COMMIT;
            """,
        ).jdbc(url=jdbc_url, table="publish_orders", mode="append")

        return OrdersPublished(success=True, rows_processed=published_df.count())
    except Exception as error:
        return OrdersPublished(success=False, error=error)
