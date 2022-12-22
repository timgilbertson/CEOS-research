import pandas as pd


def write_outputs(outputs: pd.DataFrame, output_path: str, output_type: str):
    """Write final pixel data to CSV.

    Args:
        outputs (pd.DataFrame): Calculated sensor location indices
        output_path (str): Path to write data
        output_type (str): Data type to write
    """
    outputs.to_csv(output_path + output_type + ".csv", index=True)
