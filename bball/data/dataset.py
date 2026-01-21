from torch.utils.data import Dataset
import torch
import pandas as pd


class BasketballDataset(Dataset):
    """
    Torch-compatible wrapper that converts a Pandas DataFrame + Series
    into tensors on demand.

    Parameters
    ----------
    X_df : pd.DataFrame
    y_sr : pd.Series or np.ndarray
    """

    def __init__(self, X_df: pd.DataFrame, y_sr):
        self.X = torch.tensor(X_df.values, dtype=torch.float32)
        self.y = torch.tensor(y_sr.values, dtype=torch.float32).view(-1, 1)

    def __len__(self) -> int:
        return len(self.X)

    def __getitem__(self, idx: int):
        return self.X[idx], self.y[idx]
