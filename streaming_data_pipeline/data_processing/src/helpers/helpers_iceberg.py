from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog
from loguru import logger
from pyiceberg.schema import Schema


def if_iceberg_table_exists(catalog: Catalog, table_name: str, namespace: str) -> bool:
    identifier = catalog.list_tables(namespace)
    for entry in identifier:
        if entry[1] == table_name:
            return True
    return False


def get_iceberg_catalog(iceberg_catalog: str) -> Catalog:
    try:
        catalog = load_catalog(iceberg_catalog)
        return catalog
    except Exception as e:
        raise ValueError(f"Failed to load Iceberg catalog '{iceberg_catalog}': {e}")


def create_namespace(catalog: Catalog, namespace: str):
    for ns in catalog.list_namespaces():
        if ns[0] == namespace:
            logger.info(f"Namespace {namespace} already exists")
            return
    catalog.create_namespace_if_not_exists(namespace)


def create_table(catalog: Catalog, namespace: str, table_name: str, schema: Schema):
    """Create an Iceberg table if it does not exist."""

    if if_iceberg_table_exists(catalog, table_name, namespace):
        logger.info(f"Table {table_name} already exists in namespace {namespace}")
        return
    logger.info(f"Creating table {table_name}")

    catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        properties={"namespace": namespace},
    )


def list_tables(catalog: Catalog, namespace: str):
    """List all tables in a given namespace."""

    tables = catalog.list_tables(namespace=namespace)
    for table in tables:
        logger.info(f"Table: {table}")


# local testing
# if __name__ == '__main__':
#     catalog = get_iceberg_catalog("mentor_cruise")
#     create_namespace(catalog, "mentor")
#     from pyiceberg.types import IntegerType, StringType
#     from pyiceberg.schema import Schema, NestedField
#
#     schema = Schema(
#         NestedField(1, "id", IntegerType()),
#         NestedField(2, "name", StringType())
#     )
#     create_table(catalog, "mentor",table_name='sample1',schema= schema)
#     print(if_iceberg_table_exists(catalog, "sample1", namespace="mentor"))
