
from ray_shuffling_data_loader.data_generation import generate_data
from ray_shuffling_data_loader.stats import human_readable_size

from pathlib import Path


def generate_dummy_data(num_files: int=4,
                  num_rows:int = 10000,
                  num_row_groups_per_file:int = 5,
                  max_row_group_skew:float=0.0,
                  data_dir:str= "/root/data"):
    # create data_dir if not exists
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    print(
        f"Generating {num_rows} rows over {num_files} files, with "
        f"{num_row_groups_per_file} row groups per file and at most "
        f"{100 * max_row_group_skew:.1f}% row group skew.")
    filenames, num_bytes = generate_data(
        num_rows, num_files, num_row_groups_per_file, max_row_group_skew,
        data_dir)
    print(
        f"Generated {len(filenames)} files containing {num_rows} rows "
        f"with {num_row_groups_per_file} row groups per file, totalling "
        f"{human_readable_size(num_bytes)}.")

if __name__ == '__main__':
    generate_dummy_data()