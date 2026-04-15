import psycopg2
from dataclasses import dataclass, field

PUBLISH_PRODUCT_DDL = """
DROP TABLE IF EXISTS publish_product CASCADE;
CREATE TABLE publish_product (
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

PUBLISH_ORDERS_DDL = """
DROP TABLE IF EXISTS publish_orders CASCADE;
CREATE TABLE publish_orders (
    SalesOrderDetailID INT NOT NULL UNIQUE,
    OrderQty INT,
    ProductID INT,
    UnitPrice DECIMAL(19,4),
    UnitPriceDiscount DECIMAL(19,4),
    OrderDate DATE,
    ShipDate DATE,
    OnlineOrderFlag BOOLEAN,
    AccountNumber TEXT,
    CustomerID INT,
    SalesPersonID INT,
    TotalOrderFreight DECIMAL(19,4),
    LeadTimeInBusinessDays INT,
    TotalLineExtendedPrice DECIMAL(19,4),
    CONSTRAINT fk_products FOREIGN KEY (ProductID) REFERENCES publish_product(ProductID)
);
"""

PUBLISH_ORDERS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_publish_customer_id ON publish_orders(CustomerID);
CREATE INDEX IF NOT EXISTS idx_publish_sales_person_id ON publish_orders(SalesPersonID);
CREATE INDEX IF NOT EXISTS idx_publish_product_id ON publish_orders(ProductID);
"""


@dataclass(frozen=True)
class QuerySet:
    name: str
    query: str


CREATE_QUERIES = (
    QuerySet("products", PUBLISH_PRODUCT_DDL),
    QuerySet("orders", PUBLISH_ORDERS_DDL),
    QuerySet("order_indexes", PUBLISH_ORDERS_INDEXES),
)


@dataclass(frozen=True)
class TablesCreated:
    success: bool
    created: list[str] = field(default_factory=list)
    error: Exception | None = None


def create(postgres_dsn: str) -> TablesCreated:
    created = []
    try:
        with psycopg2.connect(postgres_dsn) as connection:
            with connection.cursor() as cursor:
                for queryset in CREATE_QUERIES:
                    cursor.execute(queryset.query)
                    created.append(queryset.name)
        return TablesCreated(success=True, created=created)
    except Exception as error:
        return TablesCreated(success=False, error=error, created=created)
