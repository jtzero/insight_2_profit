import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType
from insight_2_profit.store_to_publish.sales_order import calculate_business_days


schema = StructType([
    StructField("OrderDate", DateType(), True),
    StructField("ShipDate", DateType(), True),
])


def test_business_days_excludes_weekends(spark: SparkSession):
    df = spark.createDataFrame(
        [(date(2024, 1, 8), date(2024, 1, 12))],  # Mon to Fri = 4 business days
        schema,
    )
    result = df.withColumn("BusinessDays", calculate_business_days("OrderDate", "ShipDate"))
    row = result.collect()[0]
    assert row.BusinessDays == 4


def test_business_days_same_day_returns_zero(spark: SparkSession):
    df = spark.createDataFrame(
        [(date(2024, 1, 8), date(2024, 1, 8))],  # Same day
        schema,
    )
    result = df.withColumn("BusinessDays", calculate_business_days("OrderDate", "ShipDate"))
    row = result.collect()[0]
    assert row.BusinessDays == 0


def test_business_days_across_weekend_skips_saturday_sunday(spark: SparkSession):
    df = spark.createDataFrame(
        [(date(2024, 1, 5), date(2024, 1, 9))],  # Fri to Tue = 2 business days (Mon, Tue)
        schema,
    )
    result = df.withColumn("BusinessDays", calculate_business_days("OrderDate", "ShipDate"))
    row = result.collect()[0]
    assert row.BusinessDays == 2


def test_business_days_returns_null_when_order_date_null(spark: SparkSession):
    df = spark.createDataFrame(
        [(None, date(2024, 1, 9))],
        schema,
    )
    result = df.withColumn("BusinessDays", calculate_business_days("OrderDate", "ShipDate"))
    row = result.collect()[0]
    assert row.BusinessDays is None


def test_business_days_returns_null_when_ship_date_null(spark: SparkSession):
    df = spark.createDataFrame(
        [(date(2024, 1, 8), None)],
        schema,
    )
    result = df.withColumn("BusinessDays", calculate_business_days("OrderDate", "ShipDate"))
    row = result.collect()[0]
    assert row.BusinessDays is None


def test_business_days_returns_null_when_both_dates_null(spark: SparkSession):
    df = spark.createDataFrame(
        [(None, None)],
        schema,
    )
    result = df.withColumn("BusinessDays", calculate_business_days("OrderDate", "ShipDate"))
    row = result.collect()[0]
    assert row.BusinessDays is None
