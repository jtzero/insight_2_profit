import psycopg2
from dataclasses import dataclass

CREATE_TYPES_DDL = """
DROP TYPE IF EXISTS size_unit_measure CASCADE;
CREATE TYPE size_unit_measure AS ENUM ('CM', 'IN', 'FT');

DROP TYPE IF EXISTS weight_unit_measure CASCADE;
CREATE TYPE weight_unit_measure AS ENUM ('LB', 'G');
"""

STORE_PRODUCTS_DDL = """
DROP TABLE IF EXISTS store_products CASCADE;
CREATE TABLE store_products (
    ProductID INT NOT NULL PRIMARY KEY,
    ProductDesc TEXT,
    ProductNumber TEXT,
    MakeFlag BOOLEAN,
    Color TEXT,
    SafetyStockLevel INT,
    ReorderPoint INT,
    StandardCost DECIMAL(19,4),
    ListPrice DECIMAL(19,4),
    Size TEXT,
    SizeUnitMeasureCode size_unit_measure,
    Weight DECIMAL(19,4),
    WeightUnitMeasureCode weight_unit_measure,
    ProductCategoryName TEXT,
    ProductSubCategoryName TEXT
);
"""

STORE_SALES_ORDER_HEADER_DDL = """
DROP TABLE IF EXISTS store_sales_order_header CASCADE;
CREATE TABLE store_sales_order_header (
    SalesOrderID INT NOT NULL PRIMARY KEY,
    OrderDate DATE,
    ShipDate DATE,
    OnlineOrderFlag BOOLEAN,
    AccountNumber TEXT,
    CustomerID INT,
    SalesPersonID INT,
    Freight DECIMAL(19,4)
);
"""

STORE_SALES_ORDER_DETAIL_DDL = """
DROP TABLE IF EXISTS store_sales_order_detail CASCADE;
CREATE TABLE store_sales_order_detail (
    SalesOrderID INT NOT NULL,
    SalesOrderDetailID INT NOT NULL PRIMARY KEY,
    OrderQty INT,
    ProductID INT,
    UnitPrice DECIMAL(19,4),
    UnitPriceDiscount DECIMAL(19,4),
    CONSTRAINT fk_products FOREIGN KEY (ProductID) REFERENCES store_products(ProductID)
);
"""

STORE_SALES_ORDER_HEADER_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_store_header_customer_id ON store_sales_order_header(CustomerID);
CREATE INDEX IF NOT EXISTS idx_store_sales_person_id ON store_sales_order_header(SalesPersonID);
"""

STORE_SALES_ORDER_DETAIL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_store_detail_product_id ON store_sales_order_detail(ProductID);
"""

CREATE_QUERIES = [
    CREATE_TYPES_DDL,
    STORE_PRODUCTS_DDL,
    STORE_SALES_ORDER_HEADER_DDL,
    STORE_SALES_ORDER_DETAIL_DDL,
    STORE_SALES_ORDER_HEADER_INDEXES,
    STORE_SALES_ORDER_DETAIL_INDEXES,
]


@dataclass(frozen=True)
class TablesCreated:
    success: bool
    error: Exception | None = None


def create(postgres_dsn: str) -> TablesCreated:
    try:
        with psycopg2.connect(postgres_dsn) as connection:
            with connection.cursor() as cursor:
                for query in CREATE_QUERIES:
                    cursor.execute(query)
        return TablesCreated(success=True)
    except Exception as error:
        return TablesCreated(success=False, error=error)
