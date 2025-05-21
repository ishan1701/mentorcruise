# create writer context
# define the Writer
from streaming_data_pipeline.data_generation.src.settings import SERIALIZATION_FORMAT, WRITER_TYPE
import asyncio
from streaming_data_pipeline.data_generation.src.sink.writer_factory import WriterFactory
from streaming_data_pipeline.data_generation.src.sink.writer import Context
from streaming_data_pipeline.data_generation.src.data_generator import DataGeneratorFactory
from streaming_data_pipeline.data_generation.src.models.product_sales import ProductSales
from streaming_data_pipeline.data_generation.schemas.product_sales import product_price_schema

async def main():
    writer=WriterFactory.get_writer(writer_type=WRITER_TYPE)
    context=Context(writer=writer)
    generator = DataGeneratorFactory.data_generator( "streaming", model_schema=ProductSales.schema(), avro_schema=product_price_schema)
    await generator.generate_data(num_records=1, model=ProductSales, writer_context=context)


if __name__ == '__main__':
    pass

    asyncio.run(main())


