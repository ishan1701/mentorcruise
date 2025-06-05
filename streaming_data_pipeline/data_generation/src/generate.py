# # create writer context
# # define the Writer
# import asyncio
# from loguru import logger
#
# from streaming_data_pipeline.data_generation.src.sink.writer_factory import (
#     WriterFactory,
# )
# from streaming_data_pipeline.data_generation.src.sink.writer_factory import Context
# from streaming_data_pipeline.data_generation.src.data_generator import (
#     DataGeneratorFactory,
# )
# from streaming_data_pipeline.settings import MODEL_MAP
#
#
#
#
# def generate(**kwargs):
#     # Create a writer context based on the writer type
#
#     writer_type = kwargs.get("writer_type", "kafka")
#     data_model = kwargs["model"]
#     data_generator_type = kwargs.get("generator_type", "streaming")
#
#     if not data_model:
#         raise ValueError("Model must be provided in kwargs")
#
#     else:
#         model_class = MODEL_MAP[data_model]['model']
#
#
#
#     writer = WriterFactory.get_writer(writer_type=writer_type)
#
#     #{'bootstrap_servers': 'localhost:9092', 'topic': 'mentor_cruise',
#     # 'serialization_format': 'json',
#     # 'model': 'product_sales', 'writer_type': 'streaming'}
#
#
#     context = Context(writer=writer)
#     generator = DataGeneratorFactory.data_generator(generator_type=data_generator_type,
#         model_schema=data_model.schema()
#     )
#
#
#
#     generator.generate_data(
#         num_records=kwargs.get("num_of_records", 1000),
#         model=kwargs["model_class"],
#         writer_context=context,
#         kwargs=kwargs,
#     )
#
#     # write data to the writer with help of context
