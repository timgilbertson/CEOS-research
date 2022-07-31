import argparse

from .generate_indices import indices


def main():
    parser = argparse.ArgumentParser(description="CEOS Vegetation Indices Tools")
    parser.add_argument("--input-images", help="Raw satellite image path", required=True)
    parser.add_argument("--input-tower", help="Flux tower input path", required=True)
    parser.add_argument("--output-path", help="Generated artifact output path", required=True)

    args = parser.parse_args()
    params = {arg: getattr(args, arg) for arg in vars(args)}
    indices(params)
