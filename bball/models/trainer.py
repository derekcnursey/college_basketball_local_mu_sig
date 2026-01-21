from __future__ import annotations
from pathlib import Path
import math

import torch
import torch.nn as nn
import torch.optim as optim
import torch.backends.cudnn as cudnn
from torch.utils.data import DataLoader
from torch.cuda.amp import GradScaler, autocast

from .architecture import MLPRegressor, MLPClassifier
from ..data.dataset import BasketballDataset

import torch.nn.functional as F
cudnn.benchmark = True  # autotune kernels once batch size is fixed

torch.set_float32_matmul_precision("high")
torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True
# -----------------------------------------------------------------------------
# Losses
# -----------------------------------------------------------------------------

def gaussian_nll(preds: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
    """
    Gaussian NLL with sigma parameterized via softplus for stability.

    preds: (N,2) [mu, raw_sigma]
    y    : (N,) target margin in points
    """
    mu = preds[:, 0].view(-1)
    raw = preds[:, 1].view(-1)

    # positive + stable
    sigma = F.softplus(raw) + 1e-3

    # clamp to prevent degeneracy (tuneable bounds)
    sigma = torch.clamp(sigma, 0.5, 30.0)

    z = (y - mu) / sigma
    const = 0.5 * math.log(2.0 * math.pi)

    return (0.5 * z.pow(2) + torch.log(sigma) + const).mean()


# -----------------------------------------------------------------------------
# Training helpers
# -----------------------------------------------------------------------------

def _train_loop(model, loader, criterion, optimizer, device, scaler, task: str):
    """One epoch over `loader` with mixed precision support on CPU & GPU."""
    model.train()
    running = 0.0

    from contextlib import nullcontext
    amp_ctx = autocast() if torch.cuda.is_available() else nullcontext()

    for xb, yb in loader:
        xb = xb.to(device, non_blocking=True)
        # dataset yields y as (N,1)
        yb = yb.to(device, non_blocking=True).view(-1)

        optimizer.zero_grad(set_to_none=True)

        with amp_ctx:
            preds = model(xb)
            if task == "reg":
                loss = criterion(preds, yb)              # preds (N,2), y (N,)
            else:
                loss = criterion(preds.squeeze(), yb)    # logits (N,), y (N,)

        scaler.scale(loss).backward()
        scaler.step(optimizer)
        scaler.update()

        running += loss.item()

    return running / len(loader)


# -----------------------------------------------------------------------------
# Core fit routine (shared by regressor / classifier)
# -----------------------------------------------------------------------------

def _fit(
    model_cls,
    X_train,
    y_train,
    cfg: dict,
    loss_fn,
    checkpoint_name: str,
    task: str,
) -> Path:
    """Fit `model_cls` on the given data and return the checkpoint path."""
    ds = BasketballDataset(X_train, y_train)
    workers = cfg.get("num_workers", 4)
    loader = DataLoader(
        ds,
        batch_size=cfg.get("batch_size", 4096),
        shuffle=True,
        num_workers=workers,
        pin_memory=True,
        persistent_workers=(workers > 0),
        prefetch_factor=4,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Build model kwargs
    kw = dict(
        input_dim=X_train.shape[1],
        hidden=cfg.get("hidden", 256),
        dropout=cfg.get("dropout", 0.3),
    )
    if model_cls is MLPRegressor:
        kw["hidden2"] = cfg.get("hidden2", 128)

    model = model_cls(**kw).to(device)

    optimizer = optim.Adam(model.parameters(), lr=cfg.get("lr", 1e-3))
    criterion = loss_fn
    scaler = GradScaler(enabled=torch.cuda.is_available())

    best_state, best_loss = None, float("inf")

    for _ in range(cfg.get("epochs", 50)):
        epoch_loss = _train_loop(model, loader, criterion, optimizer, device, scaler, task=task)
        if epoch_loss < best_loss:
            best_loss = epoch_loss
            # unwrap torch.compile OptimizedModule for a clean state_dict
            core = model._orig_mod if hasattr(model, "_orig_mod") else model
            best_state = core.state_dict()

    wrapper = {
        "state_dict": best_state,              # tensors only
        "feature_order": list(X_train.columns),
        "hparams": kw,                         # to rebuild the net
    }

    ckpt_dir = Path(cfg.get("ckpt_dir", "checkpoints"))
    ckpt_dir.mkdir(exist_ok=True)
    ckpt_path = ckpt_dir / checkpoint_name
    torch.save(wrapper, ckpt_path)
    torch.cuda.empty_cache()
    return ckpt_path


# -----------------------------------------------------------------------------
# Public entry points
# -----------------------------------------------------------------------------

def fit_regressor(X_train, y_train, cfg: dict | None = None) -> Path:
    cfg = cfg or {}
    return _fit(
        MLPRegressor,
        X_train,
        y_train,
        cfg,
        gaussian_nll,
        "mlp_regressor.pth",
        task="reg",
    )


def fit_classifier(X_train, y_train, cfg: dict | None = None) -> Path:
    cfg = cfg or {}
    return _fit(
        MLPClassifier,
        X_train,
        y_train.astype("float32"),
        cfg,
        nn.BCEWithLogitsLoss(),
        "mlp_classifier.pth",
        task="cls",
    )
