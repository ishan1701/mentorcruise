from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from loguru import logger


def schema_registry_client(conf: dict) -> SchemaRegistryClient:
    return SchemaRegistryClient(conf=conf)


def list_schemas(client: SchemaRegistryClient) -> list[str]:
    """
    List all schemas in the schema registry.
    """

    if client is None:
        raise ValueError("Schema Registry client is not initialized.")

    return client.get_subjects()


def register_schema(client: SchemaRegistryClient, schema: dict, subject: str) -> int :
    """
    Register a new schema in the schema registry.
    """
    import json
    if client is None:
        raise ValueError("Schema Registry client is not initialized.")



    avro_schema = Schema(schema_str=json.dumps(schema), schema_type="AVRO")

    try:
        schema_id = client.register_schema(subject_name=subject, schema=avro_schema)
        logger.info(f"Registered schema for {subject}")
        return schema_id

    except Exception as ex:
        logger.error(f"Failed to register schema for {subject}: {ex}")

def delete_schema(client: SchemaRegistryClient, subject: str) -> None:
    """
    Delete a schema from the schema registry.
    """
    if client is None:
        raise ValueError("Schema Registry client is not initialized.")

    try:
        client.delete_subject(subject_name=subject)
        logger.info(f"Deleted schema for {subject}")

    except Exception as ex:
        logger.error(f"Failed to delete schema for {subject}: {ex}")


def set_compatibility(client: SchemaRegistryClient, subject_name: str, level: str='backward') -> str:
    """
    Set the compatibility level for a schema subject.
    """
    if client is None:
        raise ValueError("Schema Registry client is not initialized.")

    try:
        new_compatibility_level = client.set_compatibility(subject_name=subject_name, level=level)
        logger.info(f"Set compatibility level for {subject_name} to {level}")
        return new_compatibility_level

    except Exception as ex:
        logger.error(f"Failed to set compatibility for {subject_name}: {ex}")

#
# # testing
if __name__ == '__main__':
    client = schema_registry_client({'url': 'http://localhost:8081'})

    print(client.get_versions(subject_name='product_sales'))

    versions= client.get_versions(subject_name='product_sales')
    for version in versions:
        print(client.get_version(subject_name='product_sales', version=version).schema.schema_str)
    #
    # set_compatibility(client, subject_name='product_sales', level='forward')
    #
    # schema = {
    #     "type": "record",
    #     "name": "product_sales",
    #     "fields": [
    #         {"name": "product_id", "type": "string"},
    #         {"name": "quantity", "type": "int"},
    #         {"name": "price", "type": "float"},
    #         {"name": "timestamp", "type": "string"},
    #     ]
    # }
    #
    #
    # schema_id= register_schema(client, schema, "product_sales")
    #
    # logger.info(f"Registered schema for {schema_id}")
    # print(client.get_schema(schema_id))
    #
    # print(list_schemas(client))
