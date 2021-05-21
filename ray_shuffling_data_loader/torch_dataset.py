import functools
from typing import Any, Callable, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from torch.utils.data import IterableDataset

from ray_shuffling_data_loader.dataset import ShufflingDataset


class TorchShufflingDataset(IterableDataset):
    """
    A PyTorch shuffling dataset that yields batches upon iteration. This is
    a thin wrapper around ShufflingDataset.

    This dataset will kick off shuffling for max_concurrent_epochs epochs at
    construction time in the master process (rank 0).

    Args:
        filenames (str): Paths to input Parquet files.
        num_epochs (int): Number of training epochs.
        num_trainers (int): Number of trainer workers.
        batch_size (int): Size of the batches that the iterator should yield.
        rank (int): The worker rank of the current process.
        drop_last (Optional[bool]): Whether to drop the last batch if it's
            incomplete (smaller than batch_size). Default is False.
        num_reducers (Optional[int]): The number of shuffler reducers. Default
            is the number of trainers x the number of cores on the master
            (rank 0) worker x 0.6.
        max_concurrent_epochs (Optional[int]): The maximum number of epochs
            whose shuffling stages should execute concurrently. Default is 2.
        feature_columns (List[Any]): The feature columns' names.
        feature_shapes (Optional[List[Any]]): The shape for each
            feature. If provided, it should match the size of feature_columns.
        feature_types (Optional[List[torch.dtype]]): The data type for each
            feature. If provided, it should match the size of feature_columns.
        label_column (Any): The label column name.
        label_shape (Optional[int]): The shape for the label data.
        label_type (Optional[torch.dtype]): The data type for the label data.
    """

    def __init__(self,
                 filenames: List[str],
                 num_epochs: int,
                 num_trainers: int,
                 batch_size: int,
                 rank: int,
                 drop_last: bool = False,
                 num_reducers=None,
                 batch_queue=None,
                 shuffle_result=None,
                 max_concurrent_epochs=2,
                 feature_columns: List[Any] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: Any = None,
                 label_shape: Optional[int] = None,
                 label_type: Optional[torch.dtype] = None):
        super().__init__()
        self._ds = ShufflingDataset(
            filenames,
            num_epochs,
            num_trainers,
            batch_size,
            rank,
            drop_last=drop_last,
            num_reducers=num_reducers,
            max_concurrent_epochs=max_concurrent_epochs,
            batch_queue=batch_queue,
            shuffle_result=shuffle_result)
        self._batch_transform = dataframe_to_tensor_factory(
            feature_columns=feature_columns,
            feature_shapes=feature_shapes,
            feature_types=feature_types,
            label_column=label_column,
            label_shape=label_shape,
            label_type=label_type)

    def set_epoch(self, epoch):
        """
        Set the current training epoch. This should be called before
        constructing the iterator on this dataset (e.g. before the
        enumerate(train_loader) call).

        Args:
            epoch (int) The epoch number for the training epoch that is about
                to start.
        """
        self._ds.set_epoch(epoch)

    def __iter__(self):
        for df in iter(self._ds):
            yield self._batch_transform(df)


def dataframe_to_tensor_factory(
        feature_columns: List[Any] = None,
        feature_shapes: Optional[List[Any]] = None,
        feature_types: Optional[List[torch.dtype]] = None,
        label_column: Any = None,
        label_shape: Optional[int] = None,
        label_type: Optional[torch.dtype] = None,
) -> Callable[[pd.DataFrame], Tuple[torch.Tensor, torch.Tensor]]:
    """
    Returns a Pandas DataFrame --> PyTorch Tensor converter, using the
    provided data spec to do the conversion. This can be provided as the
    batch_transform argument to TorchIterableShufflingDataset, and will
    convert each DataFrame GPU batch into a (features, labels) tuple of
    Torch tensors.

    Args:
        feature_columns (List[Any]): The feature columns' names.
        feature_shapes (Optional[List[Any]]): The shape for each
            feature. If provided, it should match the size of feature_columns.
        feature_types (Optional[List[torch.dtype]]): The data type for each
            feature. If provided, it should match the size of feature_columns.
        label_column (Any): The label column name.
        label_shape (Optional[int]): The shape for the label data.
        label_type (Optional[torch.dtype]): The data type for the label data.

    Returns:
        A function that will convert training data DataFrames into a
        (features, labels) tuple of Torch tensors.
    """
    (
        feature_columns,
        feature_shapes,
        feature_types,
        label_column,
        label_shape,
        label_type,
    ) = _normalize_torch_data_spec(feature_columns, feature_shapes,
                                   feature_types, label_column, label_shape,
                                   label_type)
    return functools.partial(
        convert_to_tensor,
        feature_columns=feature_columns,
        feature_shapes=feature_shapes,
        feature_types=feature_types,
        label_column=label_column,
        label_shape=label_shape,
        label_type=label_type)


def _normalize_torch_data_spec(
        feature_columns: List[Any] = None,
        feature_shapes: Optional[List[Any]] = None,
        feature_types: Optional[List[torch.dtype]] = None,
        label_column: Any = None,
        label_shape: Optional[int] = None,
        label_type: Optional[torch.dtype] = None):
    """
    Normalize the provided Torch data spec, returning sensible defaults for
    unspecified parameters.

    Args:
        feature_columns (List[Any]): The feature columns' names.
        feature_shapes (Optional[List[Any]]): The shape for each
            feature. If provided, it should match the size of feature_columns.
        feature_types (Optional[List[torch.dtype]]): The data type for each
            feature. If provided, it should match the size of feature_columns.
        label_column (Any): The label column name.
        label_shape (Optional[int]): The shape for the label data.
        label_type (Optional[torch.dtype]): The data type for the label data.

    Returns:
        Each input, normalized.
    """
    # Convert to list for convenience.
    if not isinstance(feature_columns, list):
        feature_columns = [feature_columns]

    if feature_shapes:
        if not isinstance(feature_shapes, list):
            feature_shapes = [feature_shapes]

        assert len(feature_columns) == len(feature_shapes), \
            "The feature_shapes size must match the feature_columns"
        for i in range(len(feature_shapes)):
            if not isinstance(feature_shapes[i], Iterable):
                feature_shapes[i] = [feature_shapes[i]]
    else:
        feature_shapes = [None] * len(feature_columns)

    if feature_types:
        if not isinstance(feature_types, list):
            feature_types = [feature_types]

        assert len(feature_columns) == len(feature_types), \
            "The feature_types size must match the feature_columns"
        for i in range(len(feature_types)):
            assert (all(isinstance(dtype, torch.dtype)
                        for dtype in feature_types)), \
                "All value in feature_types should be torch.dtype instance"
    else:
        feature_types = [torch.float] * len(feature_columns)

    if not label_type:
        label_type = torch.float

    return (feature_columns, feature_shapes, feature_types, label_column,
            label_shape, label_type)


def convert_to_tensor(df: pd.DataFrame, feature_columns: List[Any],
                      feature_shapes: List[Any],
                      feature_types: List[torch.dtype], label_column: Any,
                      label_shape: Optional[int], label_type: torch.dtype):
    feature_tensor = []
    for col, shape, dtype in zip(feature_columns, feature_shapes,
                                 feature_types):
        column = df[col].values
        if column.dtype == np.object:
            if isinstance(column[0], np.ndarray):
                column = np.stack(column)
            elif isinstance(column[0], (list, tuple)):
                column = list(column)
            else:
                raise Exception(
                    f"Column {col}'s type: {type(column[0])} is not supported."
                    " It must be numpy built in type or numpy object of "
                    "(ndarray, list, tuple)")

        t = torch.as_tensor(column, dtype=dtype)
        if shape is not None:
            t = t.view(*(-1, *shape))
        else:
            t = t.view(-1, 1)
        feature_tensor.append(t)

    label_df = df[label_column].values
    label_tensor = torch.as_tensor(label_df, dtype=label_type)
    if label_shape:
        label_tensor = label_tensor.view(-1, label_shape)
    else:
        label_tensor = label_tensor.view(-1, 1)
    return feature_tensor, label_tensor


if __name__ == "__main__":
    import ray
    from ray_shuffling_data_loader.stats import human_readable_size
    from ray_shuffling_data_loader.data_generation import (
        generate_data, DATA_SPEC)
    print("Starting Ray...")
    ray.init(_system_config={"idle_worker_killing_time_threshold_ms": 10**6})
    num_rows = 10**6
    num_files = 10
    num_row_groups_per_file = 1
    max_row_group_skew = 0.0
    data_dir = "data"
    print(f"Generating {num_rows} rows over {num_files} files, with "
          f"{num_row_groups_per_file} row groups per file and at most "
          f"{100 * max_row_group_skew:.1f}% row group skew.")
    filenames, num_bytes = generate_data(num_rows, num_files,
                                         num_row_groups_per_file,
                                         max_row_group_skew, data_dir)
    print(f"Generated {len(filenames)} files containing {num_rows} rows "
          f"with {num_row_groups_per_file} row groups per file, totalling "
          f"{human_readable_size(num_bytes)}.")
    num_epochs = 4
    num_trainers = 1
    batch_size = 20000
    rank = 0
    num_reducers = 8
    max_concurrent_epochs = 2
    feature_columns = list(DATA_SPEC.keys())
    numpy_to_torch_dtype = {
        np.bool: torch.bool,
        np.uint8: torch.uint8,
        np.int8: torch.int8,
        np.int16: torch.int16,
        np.int32: torch.int32,
        np.int64: torch.int64,
        np.float16: torch.float16,
        np.float32: torch.float32,
        np.float64: torch.float64,
        np.complex64: torch.complex64,
        np.complex128: torch.complex128
    }
    feature_types = [
        numpy_to_torch_dtype[dtype] for _, _, dtype in DATA_SPEC.values()
    ]
    label_column = feature_columns.pop()
    label_type = feature_types.pop()
    print(f"Creating Torch shuffling dataset with {batch_size} batch size, "
          f"{num_epochs} epochs, {num_reducers} reducers, and {num_trainers} "
          "trainers.")
    print(f"Should consume {num_rows // batch_size} batches.")
    ds = TorchShufflingDataset(
        filenames,
        num_epochs,
        num_trainers,
        batch_size,
        rank,
        num_reducers=num_reducers,
        max_concurrent_epochs=max_concurrent_epochs,
        feature_columns=feature_columns,
        feature_types=feature_types,
        label_column=label_column,
        label_type=label_type)

    for epoch in range(num_epochs):
        ds.set_epoch(epoch)

        for batch_idx, (data, targets) in enumerate(ds):
            print(f"Epoch {epoch} - consuming batch {batch_idx}: "
                  f"{len(data)} features, {len(targets)} samples")
    print("Done consuming batches.")
