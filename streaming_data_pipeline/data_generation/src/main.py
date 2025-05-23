from streaming_data_pipeline.data_generation.src.generate import generate
from pathlib import Path
from streaming_data_pipeline.data_generation.src.utils import load_yaml_file
from streaming_data_pipeline.data_generation.src.settings import WRITER_TYPE


def main():
    try:
        writer_config = load_yaml_file(Path(__file__).parent.joinpath("config.yaml"))['writer_config'][WRITER_TYPE]
    except KeyError:
        raise ValueError(f"Writer config for {WRITER_TYPE} not found in config.yaml")




if __name__ == '__main__':
    main()
