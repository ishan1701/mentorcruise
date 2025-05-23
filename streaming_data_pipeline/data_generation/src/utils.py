"utils functions"
import yaml
from pathlib import Path


def load_yaml_file(file_path: Path) -> dict:
    """Load a YAML file and return its contents as a dictionary."""
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
    return data