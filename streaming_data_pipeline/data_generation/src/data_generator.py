import asyncio
import random
from abc import ABC, abstractmethod
from pydantic import BaseModel
from streaming_data_pipeline.data_generation.src.random_data_generator import (
    random_data_generator,
)
from loguru import logger
from random import randint


class DataGenerator(ABC):
    """
    An abstract base class for data generators.
    currently supports streaming and late arriving data generation.
    """

    def __init__(self, data_model: type[BaseModel]):
        self.data_model = data_model
        self.generated_data: list[BaseModel] = list()

    @abstractmethod
    async def generate_data(
        self,
        num_records: int,
        **kwargs,
    ) -> list[BaseModel]:
        pass


class StreamingDataGenerator(DataGenerator):
    """
    A class to generate streaming data asynchronously.
    """

    async def generate_data(
        self,
        num_records: int,
        **kwargs,
    ) -> list[BaseModel]:
        records = await random_data_generator(
            self.data_model.model_schema(),
            num_records,
            sleep_seconds=kwargs.get("sleep_seconds", 0),
        )

        for record in records:
            # Convert the dictionary to a Pydantic model instance
            self.generated_data.append(self.data_model(**record))
        return self.generated_data


class LateArrivingDataGenerator(DataGenerator):
    """
    A class to generate late arriving data asynchronously.
    """

    def __init__(
        self,
        data_model: type[BaseModel],
    ):
        super().__init__(data_model=data_model)

    async def generate_data(
        self,
        num_records: int,
        **kwargs,
    ) -> list[BaseModel]:
        records = await random_data_generator(
            self.data_model.model_schema(),
            num_records,
            sleep_seconds=kwargs.get("sleep_seconds", 0),
            data_delay_by_day=kwargs.get("data_delay_by_day", 0),
            data_delay_by_hour=kwargs.get("data_delay_by_hour", 0),
            data_delay_by_mins=kwargs.get("data_delay_by_mins", 0),
            data_delay_by_secs=kwargs.get("data_delay_by_secs", 0),
        )

        for record in records:
            # Convert the dictionary to a Pydantic model instance
            self.generated_data.append(self.data_model(**record))
        return self.generated_data


class DataGeneratorFactory:
    """
    A factory class to create data generators.
    """

    @staticmethod
    def data_generator(
        generator_type: str,
        data_model: type[BaseModel],
    ) -> DataGenerator:
        if generator_type == "streaming":
            return StreamingDataGenerator(data_model=data_model)
        elif generator_type == "late_arriving":
            return LateArrivingDataGenerator(
                data_model=data_model,
            )
        else:
            raise ValueError(f"Unknown generator type: {generator_type}")



async def stream_data_generator(generator):
    print(f"Pushing  data generation event loop of stream_data_generator")
    data: list[BaseModel] = await generator.generate_data(
        num_records=randint(1, 200), sleep_seconds=randint(1, 10)
    )
    return data


async def late_data_generator_by_day(generator):
    print(f"Pushing  of data generation event loop of main_late_data_generator_by_day")
    data: list[BaseModel] = await generator.generate_data(
        num_records=randint(1, 200),
        sleep_seconds=randint(1, 10),
        data_delay_by_day=randint(1, 2),
    )
    return data


async def late_data_generator_by_hour(generator):
    print(f"Pushing  of data generation event loop of main_late_data_generator_by_hour")
    data: list[BaseModel] = await generator.generate_data(
        num_records=randint(1, 200),
        sleep_seconds=randint(1, 10),
        data_delay_by_hour=randint(1, 5),
    )
    return data


async def late_data_generator_by_minutes(generator):
    print(
        f"Pushing of data generation event loop of main_late_data_generator_by_minutes"
    )
    data: list[BaseModel] = await generator.generate_data(
        num_records=randint(1, 200),
        sleep_seconds=randint(1, 10),
        data_delay_by_mins=randint(1, 1000),
    )
    return data


async def late_data_generator_by_seconds(generator):
    print(
        f"Pushing of data generation event loop of main_late_data_generator_by_seconds"
    )
    data: list[BaseModel] = await generator.generate_data(
        num_records=randint(1, 200),
        sleep_seconds=randint(1, 10),
        data_delay_by_secs=randint(1, 4555),
    )
    return data

#
# if __name__ == '__main__':
#     from streaming_data_pipeline.settings import DATA_GENERATION_MODEL, DATA_GENERATOR_TYPE, \
#         MODEL_MAP
#
#     generator = DataGeneratorFactory.data_generator(DATA_GENERATOR_TYPE,
#                                                     data_model=MODEL_MAP[DATA_GENERATION_MODEL]['model'])
#
#     functions = [stream_data_generator,
#                  late_data_generator_by_day,
#                  late_data_generator_by_hour,
#                  late_data_generator_by_minutes,
#                  late_data_generator_by_seconds]
#
#     for _ in range(10):
#         data = asyncio.run(random.choice(functions)(generator))
#         print(data)
