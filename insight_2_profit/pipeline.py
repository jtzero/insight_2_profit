import os
import sys

from pyspark.sql import SparkSession

from . import input_data
from . import store_tables
from . import publish_tables
from . import raw_to_store
from . import store_to_publish
from . import report


def run():
    ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    jdbc_url = os.environ["JDBC_URL"]

    spark = (
        SparkSession.builder.appName("insight_2_profit")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .config(
            "spark.driver.extraClassPath",
            f"{ROOT_DIR}/jars/postgresql-42.7.1.jar",
        )
        .getOrCreate()
    )
    postgres_dsn = jdbc_url.replace("jdbc:postgresql://", "postgresql://")

    # normally would use something like sqitch or sqlalchemy
    # however no need to add another dependency for this example
    # this table also then becomes the source of truth for the schema
    print("=== Step 0: Creating store tables with PostgreSQL DDL ===")
    store_result = store_tables.create(postgres_dsn)
    if not store_result.success:
        raise store_result.error

    publish_result = publish_tables.create(postgres_dsn)
    for table in publish_result.created:
        print(f"✓ Created table {table}")
    if not publish_result.success:
        raise publish_result.error

    print("=== Step 1: Loading CSV data into raw tables (bronze) ===")
    print("Loading products into raw_products...")
    input_data.load_products(jdbc_url, spark)
    print("Loading sales order headers into raw_sales_order_header...")
    input_data.load_sales_order_headers(jdbc_url, spark)
    print("Loading sales order details into raw_sales_order_detail...")
    input_data.load_sales_order_details(jdbc_url, spark)

    print("=== Step 2: Migrating raw tables to cleaned store tables (silver) ===")
    migration_results = raw_to_store.process_all_raw_to_store(spark, jdbc_url)

    migration_successes = [res for res in migration_results if res.success]
    migration_failures = [res for res in migration_results if not res.success]

    if migration_failures:
        print(f"!!! Migration step failed with {len(migration_failures)} errors:")
        for failure in migration_failures:
            print(f"  - {failure.table_name}: {failure.error}")
        # normally would route the failure to a holding pond so they can be inspected and
        # rerun without having to run all the data again
        sys.exit(1)

    print(f"✓ Migration successful: {len(migration_successes)} tables processed.")

    print("=== Step 3: Publishing product master data (gold) ===")
    product_result = store_to_publish.product.process(spark, jdbc_url)

    if not product_result.success:
        print(f"!!! Product publishing failed: {product_result.error}")
        sys.exit(1)

    print(
        f"✓ Product master published: {product_result.rows_processed} rows processed."
    )
    if product_result.rows_without_a_product_category:
        print("⚠  rows without a product category")
        product_result.rows_without_a_product_category.dropDuplicates(
            ["ProductSubCategoryName"]
        ).select(["ProductSubCategoryName", "productid"]).show()

    print("=== Step 4: Publishing sales orders (gold) ===")
    orders_result = store_to_publish.sales_order.process(spark, jdbc_url)

    if not orders_result.success:
        raise orders_result.error

    print(f"✓ Sales orders published: {orders_result.rows_processed} rows processed.")

    print("\n=== Step 5: Data Insights Report ===")
    report_result = report.run(spark, jdbc_url)

    for report_section in report_result:
        print(f"\n{report_section.question}")
        report_section.retsult.show()

    print("=== Pipeline completed successfully ===")
