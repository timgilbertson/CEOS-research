import pandas as pd


def write_outputs(outputs: pd.DataFrame, output_path: str):
    """Write final pixel calculated indices to CSV.

    Args:
        outputs (pd.DataFrame): Calculated sensor location indices
        output_path (str): Path to write data
    """
    outputs.to_csv(output_path + "sensor_indices.csv", index=False)
