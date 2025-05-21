from streaming_data_pipeline.data_generation.src.sink.file_writer import FileWriter
from streaming_data_pipeline.data_generation.src.sink.kafka_writer import KafkaWriter
from streaming_data_pipeline.data_generation.src.sink.writer import Writer


class WriterFactory:
    """
    A factory class to create writers.
    """

    @staticmethod
    def get_writer(writer_type: str) -> Writer:
        if writer_type == "file":

            return FileWriter()
        elif writer_type == "kafka":
            return KafkaWriter()
        else:
            raise ValueError(f"Unknown writer type: {writer_type}")
