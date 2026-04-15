from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, coalesce, lit
from dataclasses import dataclass


@dataclass(frozen=True)
class ProductPublished:
    success: bool
    rows_processed: int = 0
    error: Exception | None = None
    rows_without_a_product_category: DataFrame | None = None


def transform_product_data(df: DataFrame) -> DataFrame:
    df = df.withColumn("Color", coalesce(col("Color"), lit("N/A")))

    clothing_subcats = ["Gloves", "Shorts", "Socks", "Tights", "Vests"]
    accessories_subcats = ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"]
    components_subcats = ["Wheels", "Saddles"]

    df = df.withColumn(
        "ProductCategoryName",
        when(
            col("ProductCategoryName").isNull(),
            when(col("ProductSubCategoryName").isin(clothing_subcats), "Clothing")
            .when(
                col("ProductSubCategoryName").isin(accessories_subcats), "Accessories"
            )
            .when(
                col("ProductSubCategoryName").contains("Frames")
                | col("ProductSubCategoryName").isin(components_subcats),
                "Components",
            )
            .otherwise(col("ProductCategoryName")),
        ).otherwise(col("ProductCategoryName")),
    )

    return df


def process(spark: SparkSession, jdbc_url: str) -> ProductPublished:
    try:
        store_df = spark.read.jdbc(url=jdbc_url, table="store_products")

        published_df = transform_product_data(store_df)

        no_product_category = published_df.where(col("ProductCategoryName").isNull())

        published_df.write.option(
            "preActions",
            """
            BEGIN;
            ALTER TABLE publish_product DISABLE TRIGGER ALL;
            TRUNCATE TABLE publish_product;
            ALTER TABLE publish_product ENABLE TRIGGER ALL;
            COMMIT;
            """,
        ).jdbc(
            url=jdbc_url,
            table="publish_product",
            mode="append",
            properties={
                "stringtype": "unspecified",
            },
        )

        return ProductPublished(
            success=True,
            rows_processed=published_df.count(),
            rows_without_a_product_category=no_product_category,
        )
    except Exception as error:
        return ProductPublished(success=False, error=error)
