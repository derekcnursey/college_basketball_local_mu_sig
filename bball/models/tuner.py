# bball/models/tuner.py
"""Optuna hyper-parameter tuner (GPU-aware).

Updates for distributional regression:
* Regressor trains/validates using Gaussian NLL (not MSE).
* Objective for regressor is validation NLL (minimize).
* Classifier remains BCE-with-logits; objective is log_loss.

Keeps:
* Throughput hyper-params (`batch_size`, `num_workers`) tuned with model shape.
* Mixed precision (`autocast` + `GradScaler`) and `torch.compile`.
* Successive-Halving pruning every 10 epochs.
"""
from __future__ import annotations

import json
import pathlib
from pathlib import Path
from typing import Any, Dict, Tuple

import optuna
import torch
import torch.backends.cudnn as cudnn
import torch.nn.functional as F
from sklearn.metrics import log_loss
from torch.amp import GradScaler, autocast
from torch.utils.data import DataLoader
import time
import os
from .architecture import MLPClassifier, MLPRegressor
from bball.data.dataset import BasketballDataset
torch.set_float32_matmul_precision("high")
torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True

# -----------------------------------------------------------------------------
# Global settings
# -----------------------------------------------------------------------------
if os.getenv("BBALL_OPTUNA_FRESH", "0") == "1":
    STORAGE_URI = f"sqlite:///optuna_studies_{int(time.time())}.db"
else:
    STORAGE_URI = "sqlite:///optuna_studies.db"

cudnn.benchmark = True  # autotune once batch shapes are fixed


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _load_seed_params(task: str) -> Dict[str, Any]:
    p = pathlib.Path(f"optuna_best_{task}.json")
    return json.loads(p.read_text()) if p.exists() else {}


SEED_REG: Dict[str, Any] = _load_seed_params("reg")
SEED_CLS: Dict[str, Any] = _load_seed_params("cls")
if os.getenv("BBALL_OPTUNA_FRESH", "0") == "1":
    SEED_REG = {}
    SEED_CLS = {}


def _gaussian_nll_torch(preds: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
    mu = preds[:, 0].view(-1)
    raw = preds[:, 1].view(-1)

    sigma = F.softplus(raw) + 1e-3
    sigma = torch.clamp(sigma, 0.5, 30.0)

    z = (y - mu) / sigma
    const = 0.5 * 1.8378770664093453  # log(2*pi)
    return (0.5 * z.pow(2) + torch.log(sigma) + const).mean()


# -----------------------------------------------------------------------------
# Training loop (single GPU, mixed precision, Optuna pruning)
# -----------------------------------------------------------------------------

def _train(
    model: torch.nn.Module,
    ds_train: torch.utils.data.Dataset,
    *,
    lr: float,
    epochs: int,
    task: str,
    batch_size: int,
    workers: int,
    trial: optuna.Trial,
    xv,
    yv,
) -> None:
    """Train `model` and report val-loss every 10 epochs for pruning."""
    loader = DataLoader(
        ds_train,
        batch_size=batch_size,
        shuffle=True,
        num_workers=workers,
        pin_memory=True,
        persistent_workers=False,
        prefetch_factor=4,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    if torch.cuda.is_available():
        model = torch.compile(model)

    optimiser = torch.optim.Adam(model.parameters(), lr=lr)
    scaler = GradScaler("cuda", enabled=torch.cuda.is_available())

    # cache validation tensors on device
    xv_t = torch.tensor(xv.values, dtype=torch.float32, device=device)
    yv_t = torch.tensor(yv.values, dtype=torch.float32, device=device).squeeze()

    for epoch in range(epochs):
        model.train()
        for xb, yb in loader:
            xb = xb.to(device, non_blocking=True)
            yb = yb.to(device, non_blocking=True).squeeze()

            optimiser.zero_grad(set_to_none=True)
            with autocast("cuda", enabled=torch.cuda.is_available()):
                out = model(xb)
                if task == "reg":
                    loss = _gaussian_nll_torch(out, yb)
                else:
                    logits = out.squeeze()
                    loss = F.binary_cross_entropy_with_logits(logits, yb)

            scaler.scale(loss).backward()
            scaler.step(optimiser)
            scaler.update()

        # ---- validation / pruning every 10 epochs -------------------------
        if epoch % 10 == 9:
            model.eval()
            with torch.no_grad():
                out_v = model(xv_t)
                if task == "reg":
                    val_loss = _gaussian_nll_torch(out_v, yv_t).item()
                else:
                    logits_v = out_v.squeeze()
                    val_loss = F.binary_cross_entropy_with_logits(logits_v, yv_t).item()

            trial.report(val_loss, step=epoch)
            if trial.should_prune():
                raise optuna.TrialPruned()

    torch.cuda.empty_cache()


# -----------------------------------------------------------------------------
# Objective function
# -----------------------------------------------------------------------------

def _objective(
    trial: optuna.Trial,
    Xtr,
    Xv,
    ytr,
    yv,
    task: str,
) -> float:
    # Architecture params
    hidden = trial.suggest_int("hidden", 512, 4096, step=256)
    hidden2 = trial.suggest_int("hidden2", 128, hidden, step=128)
    dropout = trial.suggest_float("dropout", 0.0, 0.5, step=0.05)

    # Optim params
    lr = trial.suggest_float("lr", 5e-5, 5e-3, log=True)
    epochs = trial.suggest_int("epochs", 30, 200)





    # Throughput params
    max_bs = int(len(ytr))
    cand_bs = [8192, 16384, 32768]
    cand_bs = [b for b in cand_bs if b <= max_bs] or [max_bs]
    batch_size = trial.suggest_categorical("batch_size", cand_bs)
    workers = trial.suggest_int("num_workers", 4, 12)

    if task == "reg":
        model = MLPRegressor(Xtr.shape[1], hidden=hidden, hidden2=hidden2, dropout=dropout)
    else:
        model = MLPClassifier(Xtr.shape[1], hidden=hidden, dropout=dropout)

    _train(
        model,
        BasketballDataset(Xtr, ytr),
        lr=lr,
        epochs=epochs,
        task=task,
        batch_size=batch_size,
        workers=workers,
        trial=trial,
        xv=Xv,
        yv=yv,
    )

    model.eval()
    device = next(model.parameters()).device
    with torch.no_grad():
        xv_t = torch.tensor(Xv.values, dtype=torch.float32, device=device)
        out = model(xv_t)

        if task == "reg":
            yv_t = torch.tensor(yv.values, dtype=torch.float32, device=device).squeeze()
            return _gaussian_nll_torch(out, yv_t).item()
        else:
            logits = out.squeeze()
            probs = torch.sigmoid(logits).detach().cpu().numpy()
            return log_loss(yv, probs)


# -----------------------------------------------------------------------------
# Study helpers
# -----------------------------------------------------------------------------

def _get_or_create_study(name: str, direction: str, pruner) -> optuna.Study:
    try:
        return optuna.load_study(study_name=name, storage=STORAGE_URI, pruner=pruner)
    except KeyError:
        return optuna.create_study(
            study_name=name,
            storage=STORAGE_URI,
            direction=direction,
            pruner=pruner,
        )


def _save_best_params(study: optuna.Study, task: str) -> None:
    Path(f"optuna_best_{task}.json").write_text(json.dumps(study.best_params, indent=2))


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

def tune(
    X_train,
    X_val,
    y_train_reg,
    y_val_reg,
    y_train_cls=None,
    y_val_cls=None,
    n_trials: int = 30,
    tune_classifier: bool = False,
):
    pruner = optuna.pruners.SuccessiveHalvingPruner(min_resource=30, reduction_factor=3)

    # --- regressor ---------------------------------------------------------
    study_reg = _get_or_create_study("regressor", "minimize", pruner)
    if SEED_REG:
        study_reg.enqueue_trial(SEED_REG)

    study_reg.optimize(
        lambda t: _objective(t, X_train, X_val, y_train_reg, y_val_reg, "reg"),
        n_trials=n_trials,
    )
    _save_best_params(study_reg, "reg")

    # --- optional classifier ----------------------------------------------
    if tune_classifier:
        if y_train_cls is None or y_val_cls is None:
            raise ValueError("tune_classifier=True requires y_train_cls and y_val_cls")

        study_cls = _get_or_create_study("classifier", "minimize", pruner)
        if SEED_CLS:
            study_cls.enqueue_trial(SEED_CLS)

        study_cls.optimize(
            lambda t: _objective(t, X_train, X_val, y_train_cls, y_val_cls, "cls"),
            n_trials=n_trials,
        )
        _save_best_params(study_cls, "cls")
        return study_reg, study_cls

    return study_reg, None

