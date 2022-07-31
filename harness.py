from ceos_indices.generate_indices import indices


def main():
    params = {
        "input_images": "data/inputs/images",
        "input_tower": "data/inputs/tower/",
        "output_path": "data/outputs/",
    }

    indices(params)


if __name__ == "__main__":
    main()
