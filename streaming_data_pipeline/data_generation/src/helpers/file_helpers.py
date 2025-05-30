"""Helpers for file operations."""

import yaml


def load_yaml_file(file_path: str) -> dict:
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
    return data
