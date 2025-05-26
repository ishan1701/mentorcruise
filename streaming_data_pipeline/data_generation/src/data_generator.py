import asyncio
from abc import ABC, abstractmethod
from pydantic import BaseModel
from streaming_data_pipeline.data_generation.src.random_data_generator import (
    data_generator,
)
from streaming_data_pipeline.data_generation.src.sink.writer_factory import Context


class DataGenerator(ABC):
    def __init__(self, model_schema: dict, avro_schema: dict):
        self.model_schema = model_schema
        self.avro_schema = avro_schema

    @abstractmethod
    async def generate_data(
        self,
        num_records: int,
        model: type[BaseModel],
        writer_context: Context,
        **kwargs,
    ):
        """
        Generate data based on the schema.
        """
        pass


class StreamingDataGenerator(DataGenerator):
    """
    A class to generate streaming data asynchronously.
    """

    async def generate_data(
        self,
        num_records: int,
        model: type[BaseModel],
        writer_context: Context,
        **kwargs,
    ):
        """
        Generate data based on the schema.
        """
        records = await data_generator(self.model_schema, num_records, sleep_seconds=0)
        for record in records:
            # Convert the dictionary to a Pydantic model instance
            # data.append(model(**record).model_dump())
            writer = writer_context.write_data()
            writer(
                data=record, avro_schema=self.avro_schema, file_path=kwargs["file_path"]
            )


class LateArrivingDataGenerator(DataGenerator):
    """
    A class to generate late arriving data asynchronously.
    """

    def __init__(
        self,
        model_schema: dict,
        avro_schema: dict,
        delay_by_day: int = 0,
        delay_by_hour: int = 0,
    ):
        super().__init__(model_schema, avro_schema)
        self.delay_by_day = delay_by_day
        self.delay_by_hour = delay_by_hour

    async def generate_data(
        self,
        num_records: int,
        model: type[BaseModel],
        writer_context: Context,
        **kwargs,
    ):
        """
        Generate data based on the schema.
        """
        records = await data_generator(
            self.model_schema,
            num_records,
            sleep_seconds=0,
            data_delay_by_day=self.delay_by_day,
            data_delay_by_hour=self.delay_by_hour,
        )
        for record in records:
            # Convert the dictionary to a Pydantic model instance
            # data.append(model(**record).model_dump())
            writer = writer_context.write_data()
            writer(
                data=record, avro_schema=self.avro_schema, file_path=kwargs["file_path"]
            )


class DataGeneratorFactory:
    """
    A factory class to create data generators.
    """

    @staticmethod
    def data_generator(
        generator_type: str, model_schema: dict, avro_schema: dict
    ) -> DataGenerator:
        if generator_type == "streaming":
            return StreamingDataGenerator(
                model_schema=model_schema, avro_schema=avro_schema
            )
        elif generator_type == "late_arriving":
            return LateArrivingDataGenerator(
                model_schema=model_schema, avro_schema=avro_schema
            )
        else:
            raise ValueError(f"Unknown generator type: {generator_type}")
