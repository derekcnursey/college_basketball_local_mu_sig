"""
Load trained checkpoints and generate predictions.
"""
from pathlib import Path
import torch
import pandas as pd
import numpy as np
import torch.nn.functional as F

from .architecture import MLPRegressor, MLPClassifier


def _load(model_cls, ckpt_path: Path, default_input_dim: int, device="cpu"):
    """
    Returns (model, feature_order)
    – Works with both *new* (wrapper) and *old* checkpoints.
    """
    bundle = torch.load(ckpt_path, map_location=device)

    # --- unwrap ---------------------------------------------------------
    if isinstance(bundle, dict) and "state_dict" in bundle:
        state_dict = bundle["state_dict"]
        feature_order = bundle.get("feature_order")          # None for legacy
        hparams = bundle.get("hparams", {}).copy()           # make it mutable
    else:                                                   # legacy checkpoint
        state_dict, feature_order, hparams = bundle, None, {}

    # --- decide input dimension ----------------------------------------
    if feature_order is not None:                            # new checkpoints
        in_dim = len(feature_order)
    else:                                                   # fall back to stored
        in_dim = hparams.pop("input_dim", default_input_dim)

    # ensure we don't pass duplicates
    hparams.pop("input_dim", None)
    hparams.pop("in_dim", None)

    model = model_cls(input_dim=in_dim, **hparams).to(device)
    model.load_state_dict(state_dict, strict=False)
    model.eval()

    return model, feature_order


def load_regressor(default_input_dim: int, ckpt_dir: str | Path = "checkpoints"):
    """
    Load regressor only (for reg-only inference runs).
    Returns: (reg_model, feature_order)
    """
    ckpt_dir = Path(ckpt_dir)
    reg, feat_order = _load(MLPRegressor, ckpt_dir / "mlp_regressor.pth", default_input_dim)
    return reg, feat_order


def load_models(default_input_dim: int, ckpt_dir: str | Path = "checkpoints"):
    """
    Load regressor + (optional) classifier.
    If classifier checkpoint doesn't exist, returns cls=None.
    Returns: (reg_model, cls_model_or_None, feature_order)
    """
    ckpt_dir = Path(ckpt_dir)

    reg, feat_order = _load(MLPRegressor, ckpt_dir / "mlp_regressor.pth", default_input_dim)

    cls_path = ckpt_dir / "mlp_classifier.pth"
    if cls_path.exists():
        cls, _ = _load(MLPClassifier, cls_path, default_input_dim)
    else:
        cls = None

    return reg, cls, feat_order


def _coerce_features(df_or_arr):
    if isinstance(df_or_arr, pd.DataFrame):
        return (
            df_or_arr.apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
            .to_numpy(dtype="float32")
        )
    return np.asarray(df_or_arr, dtype="float32")


@torch.no_grad()
def predict_margin_dist(df: pd.DataFrame, reg_model):
    """
    Return (mu, sigma) as numpy arrays.
    sigma is parameterized via softplus(raw_sigma) + eps and clamped for stability.
    """
    device = next(reg_model.parameters()).device
    arr = _coerce_features(df)

    x = torch.tensor(arr, dtype=torch.float32, device=device)
    out = reg_model(x)  # (N,2)
    mu = out[:, 0].detach().cpu().numpy().ravel()

    raw = out[:, 1]
    sigma = F.softplus(raw) + 1e-3
    sigma = torch.clamp(sigma, 0.5, 30.0)
    sigma = sigma.detach().cpu().numpy().ravel()

    return mu, sigma


@torch.no_grad()
def predict_margin(df: pd.DataFrame, reg_model):
    """
    Backward compatible: return only mu (expected margin).
    """
    mu, _sigma = predict_margin_dist(df, reg_model)
    return mu


@torch.no_grad()
def predict_home_win_prob(df: pd.DataFrame, cls_model):
    """
    Classifier outputs logits; we return probabilities via sigmoid.

    NOTE: cls_model may be None if you didn't ship classifier checkpoint.
    """
    if cls_model is None:
        raise ValueError("cls_model is None (mlp_classifier.pth not found).")

    device = next(cls_model.parameters()).device
    arr = _coerce_features(df)

    x = torch.tensor(arr, dtype=torch.float32, device=device)
    logits = cls_model(x).view(-1)
    probs = torch.sigmoid(logits)
    return probs.detach().cpu().numpy().ravel()
