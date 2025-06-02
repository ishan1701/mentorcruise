# create writer context
# define the Writer
import asyncio
from streaming_data_pipeline.data_generation.src.sink.writer_factory import (
    WriterFactory,
)
from streaming_data_pipeline.data_generation.src.sink.writer_factory import Context
from streaming_data_pipeline.data_generation.src.data_generator import (
    DataGeneratorFactory,
)


# from streaming_data_pipeline.data_generation.src.serializer.serializer_factory import SerializerFactory


def generate(**kwargs):
    # Create a writer context based on the writer type
    writer = WriterFactory.get_writer(writer_type=kwargs["writer_type"])
    context = Context(writer=writer)
    generator = DataGeneratorFactory.data_generator(
        kwargs["generator_type"],
        model_schema=kwargs["model"].schema(),
        avro_schema=kwargs["avro_schema"],
    )

    # create serialzation context

    generator.generate_data(
        num_records=kwargs["num_of_records"],
        model=kwargs["mode"],
        writer_context=context,
        kwargs=kwargs,
    )
