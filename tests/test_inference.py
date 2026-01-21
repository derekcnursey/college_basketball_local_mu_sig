import numpy as np
import pandas as pd
from bball.data.loaders import load_training_dataframe, split_X_y
from bball.models.trainer import fit_regressor, fit_classifier
from bball.models.infer import load_models, predict_margin, predict_home_win_prob

def test_roundtrip_inference(tmp_path):
    df = load_training_dataframe().sample(1500, random_state=1)
    X, y_reg, y_cls = split_X_y(df, "spread_home", "home_win")
    ckpt_r = fit_regressor(X, y_reg, {"epochs": 1, "ckpt_dir": tmp_path})
    ckpt_c = fit_classifier(X, y_cls, {"epochs": 1, "ckpt_dir": tmp_path})
    reg, cls = load_models(X.shape[1], ckpt_dir=tmp_path)
    preds = predict_margin(X.head(), reg)
    probs = predict_home_win_prob(X.head(), cls)
    assert preds.shape == (5,)
    assert probs.shape == (5,)
    assert np.all((0 <= probs) & (probs <= 1))
