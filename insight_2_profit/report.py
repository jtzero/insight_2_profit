from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum, avg, desc, row_number
from pyspark.sql.window import Window
from dataclasses import dataclass


@dataclass
class Inquery:
    question: str
    retsult: object


def highest_revenue_products(combined_df) -> Inquery:
    revenue_by_year_color = (
        combined_df.withColumn("OrderYear", year(col("OrderDate")))
        .groupBy("OrderYear", "Color")
        .agg(sum("TotalLineExtendedPrice").alias("TotalRevenue"))
    )

    window_spec = Window.partitionBy("OrderYear").orderBy(desc("TotalRevenue"))

    highest_revenue_color = (
        revenue_by_year_color.withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .select("OrderYear", "Color", "TotalRevenue")
        .orderBy("OrderYear")
    )

    return Inquery(
        question="Which color generated the highest revenue each year?",
        retsult=highest_revenue_color,
    )


def average_lead_time(combined_df) -> Inquery:
    avg_lead_time = (
        combined_df.groupBy("ProductCategoryName")
        .agg(avg("LeadTimeInBusinessDays").alias("AvgLeadTime"))
        .orderBy("AvgLeadTime")
    )

    return Inquery(
        question="What is the average lead time for each product category?",
        retsult=avg_lead_time,
    )


def run(spark: SparkSession, jdbc_url: str) -> list[Inquery]:
    products_df = spark.read.jdbc(url=jdbc_url, table="publish_product")
    orders_df = spark.read.jdbc(url=jdbc_url, table="publish_orders")
    combined_df = orders_df.join(products_df, "ProductID")

    report_sections = [
        highest_revenue_products(combined_df),
        average_lead_time(combined_df),
    ]
    return report_sections
