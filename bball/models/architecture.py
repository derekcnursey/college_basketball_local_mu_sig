import torch.nn as nn


def _hidden_stack(in_dim: int, h1: int = 256, h2: int = 128, dropout: float = 0.3) -> nn.Sequential:
    return nn.Sequential(
        nn.Linear(in_dim, h1),
        nn.BatchNorm1d(h1),
        nn.ReLU(),
        nn.Dropout(dropout),
        nn.Linear(h1, h2),
        nn.BatchNorm1d(h2),
        nn.ReLU(),
    )


class MLPRegressor(nn.Module):
    """
    Two-hidden-layer MLP for DISTRIBUTIONAL margin prediction.

    Outputs:
      - mu        : predicted mean margin
      - log_sigma : log standard deviation of margin

    Parameters
    ----------
    input_dim : int
        Number of input features.
    hidden : int, default 256
        Width of the **first** hidden layer.
    hidden2 : int, default None
        Width of the **second** hidden layer. If None, defaults to hidden // 2.
    dropout : float, default 0.3
        Drop-out rate applied after each hidden layer.
    """
    def __init__(
        self,
        input_dim: int,
        hidden: int = 256,
        hidden2: int | None = None,
        dropout: float = 0.3,
    ):
        super().__init__()
        hidden2 = hidden2 or hidden // 2

        self.features = nn.Sequential(
            nn.Linear(input_dim, hidden),
            nn.BatchNorm1d(hidden),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(hidden, hidden2),
            nn.BatchNorm1d(hidden2),
            nn.ReLU(),
        )

        # head outputs: [mu, log_sigma]
        self.head = nn.Linear(hidden2, 2)

    def forward(self, x):
        return self.head(self.features(x))  # (N,2)


class MLPClassifier(nn.Module):
    """
    Two-hidden-layer MLP classifier for home-win prediction.

    IMPORTANT:
      - outputs LOGITS (no sigmoid)
      - apply torch.sigmoid(logits) at inference time
    """
    def __init__(
        self,
        input_dim: int,
        hidden: int = 256,
        dropout: float = 0.3,
    ):
        super().__init__()
        self.features = nn.Sequential(
            nn.Linear(input_dim, hidden),
            nn.BatchNorm1d(hidden),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden, hidden // 2),
            nn.BatchNorm1d(hidden // 2),
            nn.ReLU(),
        )
        self.head = nn.Linear(hidden // 2, 1)

    def forward(self, x):
        return self.head(self.features(x))  # (N,1) logits
