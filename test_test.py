from predict_games import build_today_feature_frame

info_df, X_df = build_today_feature_frame(season_year=2026)
print(info_df.head())
print(X_df.isna())
