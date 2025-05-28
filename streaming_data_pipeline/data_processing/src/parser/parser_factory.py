from streaming_data_pipeline.data_processing.src.parser.parser import (
    AvroParser,
    JsonParser,
    Parser,
)
import loguru
from pyspark.sql import DataFrame


class ParserFactory:
    @staticmethod
    def get_parser(parser_type: str) -> Parser:
        if parser_type == "avro":
            return AvroParser()
        elif parser_type == "json":
            return JsonParser()
        else:
            loguru.logger.error(f"Unknown parser type: {parser_type}")
            raise ValueError(f"Unknown parser type: {parser_type}")


# context
class ParserContext:
    def __init__(self, parser: Parser):
        self._parser = parser

    @property
    def parser(self) -> Parser:
        return self._parser

    @parser.setter
    def parser(self, parser: Parser):
        if not isinstance(parser, Parser):
            raise TypeError("Parser must be an instance of Parser class")
        self._parser = parser

    def parse(self, spark, df, column, parsed_column_name, **kwargs) -> DataFrame:
        return self.parser.parse(
            spark=spark,
            df=df,
            column=column,
            parsed_column_name=parsed_column_name,
            **kwargs,
        )
