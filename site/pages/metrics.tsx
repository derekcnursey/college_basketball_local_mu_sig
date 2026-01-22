import { GetServerSideProps } from "next";
import Layout from "../components/Layout";
import {
  PredictionRow,
  getActualMargin,
  getTeams,
  normalizeRows,
  normalizeTeam
} from "../lib/data";
import { listFinalScoreFiles, listPredictionFiles, readJsonFile } from "../lib/server-data";

type DateSummary = {
  date: string;
  count: number;
  mse: number | null;
  mae: number | null;
  meanError: number | null;
  atsWinPct: number | null;
};

type MetricsProps = {
  mse: number | null;
  mae: number | null;
  meanError: number | null;
  atsWinPct: number | null;
  atsWinPctEdge: number | null;
  dateSummaries: DateSummary[];
};

export const getServerSideProps: GetServerSideProps<MetricsProps> = async () => {
  const finalFiles = listFinalScoreFiles();
  const todayStr = new Date().toISOString().slice(0, 10);
  const eligibleFinals = finalFiles.filter((file) => file.date < todayStr);
  const maxFinalDate = eligibleFinals.length
    ? eligibleFinals.map((file) => file.date).sort().at(-1) ?? null
    : null;
  const files = listPredictionFiles()
    .filter((file) => (maxFinalDate ? file.date <= maxFinalDate : true))
    .sort((a, b) => (a.date < b.date ? 1 : -1));
  const finalFilesByDate = new Map(finalFiles.map((file) => [file.date, file.filename]));
  const dateSummaries: DateSummary[] = [];
  const allSquaredErrors: number[] = [];
  const allAbsErrors: number[] = [];
  const allErrors: number[] = [];
  const allAtsResults: number[] = [];
  const allAtsEdgeResults: number[] = [];

  for (const file of files) {
    const payload = readJsonFile(file.filename);
    const rows = normalizeRows(payload);
    const finalFilename = finalFilesByDate.get(file.date);
    const finalRows = finalFilename ? normalizeRows(readJsonFile(finalFilename)) : [];
    const resultLookup = buildResultLookup(finalRows);

    if (!rows.length) {
      dateSummaries.push({
        date: file.date,
        count: 0,
        mse: null,
        mae: null,
        meanError: null,
        atsWinPct: null
      });
      continue;
    }

    const dateSquaredErrors: number[] = [];
    const dateAbsErrors: number[] = [];
    const dateErrors: number[] = [];
    const dateAtsResults: number[] = [];
    const dateAtsEdgeResults: number[] = [];

    for (const row of rows) {
      const modelSpread = getModelSpread(row);
      if (modelSpread === null) {
        continue;
      }
      const resultRow = findResultRow(row, resultLookup);
      if (!resultRow) {
        continue;
      }
      const actualSpread = getActualSpread(resultRow);
      if (actualSpread === null) {
        continue;
      }
      const error = modelSpread - actualSpread;
      dateErrors.push(error);
      dateSquaredErrors.push(error * error);
      dateAbsErrors.push(Math.abs(error));

      const ats = getAtsResult(row, -actualSpread);
      if (ats && ats !== "push") {
        const atsWin = ats === "win" ? 1 : 0;
        dateAtsResults.push(atsWin);
        const probEdge = getPickProbEdge(row);
        if (probEdge !== null && probEdge > 0.1) {
          dateAtsEdgeResults.push(atsWin);
        }
      }
    }

    dateSquaredErrors.forEach((value) => allSquaredErrors.push(value));
    dateAbsErrors.forEach((value) => allAbsErrors.push(value));
    dateErrors.forEach((value) => allErrors.push(value));
    dateAtsResults.forEach((value) => allAtsResults.push(value));
    dateAtsEdgeResults.forEach((value) => allAtsEdgeResults.push(value));

    dateSummaries.push({
      date: file.date,
      count: rows.length,
      mse: dateSquaredErrors.length ? average(dateSquaredErrors) : null,
      mae: dateAbsErrors.length ? average(dateAbsErrors) : null,
      meanError: dateErrors.length ? average(dateErrors) : null,
      atsWinPct: dateAtsResults.length ? average(dateAtsResults) : null
    });
  }

  return {
    props: {
      mse: allSquaredErrors.length ? average(allSquaredErrors) : null,
      mae: allAbsErrors.length ? average(allAbsErrors) : null,
      meanError: allErrors.length ? average(allErrors) : null,
      atsWinPct: allAtsResults.length ? average(allAtsResults) : null,
      atsWinPctEdge: allAtsEdgeResults.length ? average(allAtsEdgeResults) : null,
      dateSummaries
    }
  };
};

export default function Metrics({
  mse,
  mae,
  meanError,
  atsWinPct,
  atsWinPctEdge,
  dateSummaries
}: MetricsProps) {
  return (
    <Layout>
      <h1 className="page-title">Lifetime</h1>
      <section className="metrics-grid">
        <div className="metric-card">
          <span>MAE</span>
          <strong>{formatNumber(mae)}</strong>
        </div>
        <div className="metric-card">
          <span>MSE</span>
          <strong>{formatNumber(mse)}</strong>
        </div>
        <div className="metric-card">
          <span>Mean error</span>
          <strong>{formatNumber(meanError)}</strong>
        </div>
        <div className="metric-card">
          <span>ATS win %</span>
          <strong>{formatPercent(atsWinPct)}</strong>
        </div>
        <div className="metric-card">
          <span>ATS win % (Prob Edge &gt; 10%)</span>
          <strong>{formatPercent(atsWinPctEdge)}</strong>
        </div>
      </section>

      <section>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Games</th>
                <th>MAE</th>
                <th>MSE</th>
                <th>Mean error</th>
                <th>ATS win %</th>
              </tr>
            </thead>
            <tbody>
              {dateSummaries.map((summary) => (
                <tr key={summary.date}>
                  <td>{summary.date}</td>
                  <td>{summary.count}</td>
                  <td>{formatNumber(summary.mae)}</td>
                  <td>{formatNumber(summary.mse)}</td>
                  <td>{formatNumber(summary.meanError)}</td>
                  <td>{formatPercent(summary.atsWinPct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </Layout>
  );
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function formatNumber(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(3);
}

function formatPercent(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(1)}%`;
}

function getModelSpread(row: PredictionRow): number | null {
  const modelValue = row.model_mu_home ?? row.modelMuHome ?? row.model_home_spread;
  if (typeof modelValue === "number") {
    return Number(modelValue.toFixed(2));
  }
  if (typeof modelValue === "string" && modelValue.trim() !== "") {
    const parsed = Number(modelValue);
    if (!Number.isNaN(parsed)) {
      return Number(parsed.toFixed(2));
    }
  }
  return null;
}

function getActualSpread(row: PredictionRow): number | null {
  const homeScore = row.score_home ?? row.home_score ?? row.homeScore ?? row.scoreHome;
  const awayScore = row.score_away ?? row.away_score ?? row.scoreAway ?? row.awayScore;
  if (typeof homeScore === "number" && typeof awayScore === "number") {
    return awayScore - homeScore;
  }
  if (typeof homeScore === "string" && typeof awayScore === "string") {
    const home = Number(homeScore);
    const away = Number(awayScore);
    if (!Number.isNaN(home) && !Number.isNaN(away)) {
      return away - home;
    }
  }
  return null;
}

function getMarketSpreadHome(row: PredictionRow): number | null {
  const value = row.market_spread_home ?? row.home_spread_num ?? row.spread_home;
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return null;
}

function getPickSide(row: PredictionRow): string | null {
  const value = row.pick_side ?? row.pickSide;
  if (typeof value === "string" && value.trim() !== "") {
    return value.toUpperCase();
  }
  return null;
}

function getPickProbEdge(row: PredictionRow): number | null {
  const value = row.pick_prob_edge ?? row.pickProbEdge;
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return null;
}

function getAtsResult(
  row: PredictionRow,
  actualMargin: number
): "win" | "loss" | "push" | null {
  const pickSide = getPickSide(row);
  const marketSpread = getMarketSpreadHome(row);
  if (!pickSide || marketSpread === null) {
    return null;
  }

  const cover = actualMargin + marketSpread;
  if (cover === 0) {
    return "push";
  }
  const coverSide = cover > 0 ? "HOME" : "AWAY";
  return pickSide === coverSide ? "win" : "loss";
}

function buildResultLookup(rows: PredictionRow[]): Map<string, PredictionRow> {
  const lookup = new Map<string, PredictionRow>();
  for (const row of rows) {
    const gameId = getGameId(row);
    if (gameId) {
      lookup.set(gameId, row);
    }
    const key = getTeamKey(row);
    if (key) {
      lookup.set(key, row);
    }
  }
  return lookup;
}

function findResultRow(
  row: PredictionRow,
  lookup: Map<string, PredictionRow>
): PredictionRow | null {
  const gameId = getGameId(row);
  if (gameId && lookup.has(gameId)) {
    return lookup.get(gameId) ?? null;
  }
  const teamKey = getTeamKey(row);
  if (teamKey && lookup.has(teamKey)) {
    return lookup.get(teamKey) ?? null;
  }
  return null;
}

function getGameId(row: PredictionRow): string | null {
  const gameId = row.game_id ?? row.gameId;
  if (typeof gameId === "string" && gameId.trim() !== "") {
    return gameId;
  }
  const dateValue =
    (row.date as string | undefined) ||
    (row.game_date as string | undefined) ||
    (row.gameDate as string | undefined);
  const teams = getTeams(row);
  if (dateValue && teams.away && teams.home) {
    return slugify(`${dateValue}_${teams.away}_${teams.home}`);
  }
  return null;
}

function getTeamKey(row: PredictionRow): string | null {
  const teams = getTeams(row);
  if (!teams.home || !teams.away) {
    return null;
  }
  return `${normalizeTeam(teams.home)}__${normalizeTeam(teams.away)}`;
}

function slugify(text: string): string {
  const lowered = (text || "").toLowerCase();
  return lowered.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}
