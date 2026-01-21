# College Basketball Predictions Site

Minimal Next.js UI for daily prediction files.

## Local development

```bash
cd site
npm install
npm run dev
```

Open http://localhost:3000

## Add data files

Prediction files live in `site/public/data/` and must be named:

- `predictions_YYYY-MM-DD.json`

## Daily publish workflow

1. Generate the JSON from your CSV:

```bash
bash scripts/publish_daily.sh /path/to/preds.csv
```

Optionally pass a date if the CSV date column is missing or needs override:

```bash
bash scripts/publish_daily.sh /path/to/preds.csv 2025-12-20
```

If no CSV path is provided, it uses the newest file in `predictions/csv/csvfiles/`.

Outputs are written to:

- `site/public/data/predictions_YYYY-MM-DD.json`
- `predictions/json/predictions_YYYY-MM-DD.json`

2. Commit and push:

```bash
git add site/public/data

git commit -m "Add predictions for YYYY-MM-DD"

git push
```

## Deploy to Vercel

1. Push the repo to GitHub.
2. In Vercel, click **New Project** and import the repo.
3. Set the **Root Directory** to `site`.
4. Keep the default build settings (`npm run build`).
5. Deploy.

Last deploy test: 2026-01-21 16:27:48
