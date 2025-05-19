## A random data should be generated for the following columns:
1. The column should be product_id, quantity, price and timestamp
2. For data holding, a pydantic model should be created.
3. As the data needs to be serialized to push to Kafka, pydantic model should be converted to a dictionary and then to avro format.
4. Define sink, where to push the data. For example, Kafka or filesystem. Here use a strategy design pattern
5. to stimulate the data generation, use asyncio to generate the data and push to sink.
6. use click to create a command line interface to run the data generation.
7. create a module for stimulating late data arrival create a click command line interface to run the late data arrival.
