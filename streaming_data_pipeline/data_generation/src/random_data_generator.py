import random
import asyncio
from datetime import datetime, timedelta
from loguru import logger


def random_int(min: int, max: int) -> int:
    return random.randint(min, max)


def random_float(min: float, max: float) -> float:
    return round(random.uniform(min, max), 2)


def random_datetime(
    data_delay_by_day, data_delay_by_hour, data_delay_by_mins, data_delay_by_secs
) -> str:

    if data_delay_by_secs > 0:
        return str(datetime.now() - timedelta(seconds=data_delay_by_secs))
    if data_delay_by_mins > 0:
        return str(datetime.now() - timedelta(minutes=data_delay_by_mins))
    if data_delay_by_day > 0:
        return str(datetime.now() - timedelta(days=data_delay_by_day))
    if data_delay_by_hour > 0:
        return str(datetime.now() - timedelta(hours=data_delay_by_hour))
    else:
        return str(datetime.now())


async def random_data_generator(
    schema: dict,
    num_records: int,
    sleep_seconds=0,
    data_delay_by_day=0,
    data_delay_by_hour: int = 0,
    data_delay_by_mins: int = 0,
    data_delay_by_secs: int = 0,
) -> list[dict]:
    logger.info(f"Generating {num_records} records from random data generator module")
    data = list()

    for _ in range(num_records):
        record = dict()
        for key, datatype in schema.items():
            if datatype == "int":
                record[key] = random_int(min=10, max=10)
            elif datatype == "float":
                record[key] = random_float(min=1.0, max=1000.00)
            elif datatype == "str":
                record[key] = str(random_int(min=1000, max=1000000))

            elif datatype == "datetime":
                record[key] = random_datetime(
                    data_delay_by_day=data_delay_by_day,
                    data_delay_by_hour=data_delay_by_hour,
                    data_delay_by_mins=data_delay_by_mins,
                    data_delay_by_secs=data_delay_by_secs,
                )
            else:
                raise ValueError(f"Unknown datatype: {datatype}")
        data.append(record)
    logger.info(f"Sleep for {sleep_seconds} seconds")
    await asyncio.sleep(sleep_seconds)
    return data
