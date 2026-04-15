# to run

docker-compose up
poetry install
poetry run poe pipeline

## results

```
=== Step 0: Creating store tables with PostgreSQL DDL ===
✓ Created table products
✓ Created table orders
✓ Created table order_indexes
=== Step 1: Loading CSV data into raw tables (bronze) ===
Loading products into raw_products...
26/04/15 17:47:49 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
Loading sales order headers into raw_sales_order_header...
Loading sales order details into raw_sales_order_detail...
=== Step 2: Migrating raw tables to cleaned store tables (silver) ===
✓ Migration successful: 3 tables processed.
=== Step 3: Publishing product master data (gold) ===
✓ Product master published: 295 rows processed.
⚠  rows without a product category
+----------------------+---------+
|ProductSubCategoryName|productid|
+----------------------+---------+
|       Bottom Brackets|      994|
|           Derailleurs|      894|
|                Chains|      952|
|       Hydration Packs|      880|
|     Bottles and Cages|      870|
|                 Forks|      802|
|                Brakes|      907|
|               Jerseys|      713|
|              Panniers|      842|
|            Bike Racks|      876|
|               Fenders|      878|
|                  Caps|      712|
|             Cranksets|      949|
|           Bike Stands|      879|
|            Bib-Shorts|      855|
|       Tires and Tubes|      873|
|              Cleaners|      877|
+----------------------+---------+

=== Step 4: Publishing sales orders (gold) ===
✓ Sales orders published: 121317 rows processed.

=== Step 5: Data Insights Report ===

Which color generated the highest revenue each year?
+---------+------+-------------+
|OrderYear| Color| TotalRevenue|
+---------+------+-------------+
|     2021|   Red| 6019614.0157|
|     2022| Black|14005242.9752|
|     2023| Black|15047694.3692|
|     2024|Yellow| 6368158.4789|
+---------+------+-------------+


What is the average lead time for each product category?
+-------------------+-----------------+
|ProductCategoryName|      AvgLeadTime|
+-------------------+-----------------+
|         Components|4.667113624438874|
|              Bikes|4.667897567632656|
|        Accessories|4.702787804316105|
|           Clothing|4.711666367068129|
|               NULL|4.717621086432968|
+-------------------+-----------------+

=== Pipeline completed successfully ===
```
