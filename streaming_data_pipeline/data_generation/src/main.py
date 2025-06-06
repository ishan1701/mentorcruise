from pathlib import Path
from streaming_data_pipeline.data_generation.src.utils import load_yaml_file
from streaming_data_pipeline.settings import (
    DATA_GENERATOR_WRITER as WRITER_TYPE,
    DATA_GENERATION_MODEL,
    DATA_GENERATOR_TYPE,
    MODEL_MAP
)
import asyncio

from streaming_data_pipeline.data_generation.src.data_generator import (
    DataGeneratorFactory,
    stream_data_generator,
    late_data_generator_by_hour,
    late_data_generator_by_seconds,
    late_data_generator_by_minutes,
    late_data_generator_by_day,
)
import random
from streaming_data_pipeline.data_generation.src.sink.writer_factory import (
    WriterFactory,
)

from streaming_data_pipeline.data_generation.src.sink.writer_factory import Context


def main():
    try:
        writer_config = load_yaml_file(Path(__file__).parent.joinpath("config.yaml"))
        writer_config = writer_config["writer"][WRITER_TYPE]
        writer_config["model"] = DATA_GENERATION_MODEL
        print(f"Writer config for {WRITER_TYPE}: {writer_config}")
    except KeyError:
        raise ValueError(f"some config for {WRITER_TYPE} not found in config.yaml")

    generator = DataGeneratorFactory.data_generator(
        DATA_GENERATOR_TYPE, data_model=MODEL_MAP[DATA_GENERATION_MODEL]["model"]
    )

    functions = [
        stream_data_generator,
        late_data_generator_by_day,
        late_data_generator_by_hour,
        late_data_generator_by_minutes,
        late_data_generator_by_seconds,
    ]

    # create a sink context

    writer = WriterFactory.get_writer(
        writer_type=WRITER_TYPE, **writer_config
    )
    context = Context(writer=writer)

    while True:
        data = asyncio.run(random.choice(functions)(generator))
        for record in data:
            # serializing the pydantic model before pushing to kafka
            # currently only json serialization is supported
            context.writer.write(record.serialize())


if __name__ == "__main__":
    # hardcoded for now, but can be extended to take command line arguments via click

    main()
