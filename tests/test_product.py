import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from insight_2_profit.store_to_publish.product import transform_product_data

schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("Color", StringType(), True),
    StructField("ProductCategoryName", StringType(), True),
    StructField("ProductSubCategoryName", StringType(), True),
])


def test_null_color_replaced_with_na(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, "Red", "Bikes", None), (2, "Blue", "Bikes", "Bikes"), (3, None, "Bikes", "Bikes")],
        schema,
    )
    result = transform_product_data(df)
    colors = [row.Color for row in result.collect()]
    assert colors == ["Red", "Blue", "N/A"]


def test_null_category_filled_from_subcategory_clothing(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, "Red", None, "Shorts")],
        schema,
    )
    result = transform_product_data(df)
    row = result.collect()[0]
    assert row.ProductCategoryName == "Clothing"


def test_null_category_filled_from_subcategory_accessories(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, "Red", None, "Helmets")],
        schema,
    )
    result = transform_product_data(df)
    row = result.collect()[0]
    assert row.ProductCategoryName == "Accessories"


def test_null_category_filled_from_subcategory_components_frames(
    spark: SparkSession,
):
    df = spark.createDataFrame(
        [(1, "Red", None, "Road Frames")],
        schema,
    )
    result = transform_product_data(df)
    row = result.collect()[0]
    assert row.ProductCategoryName == "Components"


def test_null_category_filled_from_subcategory_components_wheels(
    spark: SparkSession,
):
    df = spark.createDataFrame(
        [(1, "Red", None, "Wheels")],
        schema,
    )
    result = transform_product_data(df)
    row = result.collect()[0]
    assert row.ProductCategoryName == "Components"


def test_existing_category_not_overwritten(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, "Red", "ExistingCategory", "Shorts")],
        schema,
    )
    result = transform_product_data(df)
    row = result.collect()[0]
    assert row.ProductCategoryName == "ExistingCategory"


def test_unknown_subcategory_keeps_null_category(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, "Red", None, "UnknownProduct")],
        schema,
    )
    result = transform_product_data(df)
    row = result.collect()[0]
    assert row.ProductCategoryName is None
