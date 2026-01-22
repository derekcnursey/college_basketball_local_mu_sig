import { GetServerSideProps } from "next";
import Layout from "../components/Layout";
import { PredictionRow, getTeams, normalizeRows, normalizeTeam } from "../lib/data";
import { listFinalScoreFiles, listPredictionFiles, readJsonFile } from "../lib/server-data";

type DateSummary = {
  date: string;
  count: number;
  mse: number | null;
  mae: number | null;
  meanError: number | null;
  atsRecord: string | null;
  atsRecordEdge: string | null;
};

type MetricsProps = {
  mse: number | null;
  mae: number | null;
  meanError: number | null;
  atsWinPct: number | null;
  atsWinPctEdge: number | null;
  atsRecord: string | null;
  atsRecordEdge: string | null;
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
  let allAtsWins = 0;
  let allAtsLosses = 0;
  let allAtsEdgeWins = 0;
  let allAtsEdgeLosses = 0;

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
        atsRecord: null,
        atsRecordEdge: null
      });
      continue;
    }

    const dateSquaredErrors: number[] = [];
    const dateAbsErrors: number[] = [];
    const dateErrors: number[] = [];
    let dateAtsWins = 0;
    let dateAtsLosses = 0;
    let dateAtsEdgeWins = 0;
    let dateAtsEdgeLosses = 0;

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
        const isWin = ats === "win";
        if (isWin) {
          dateAtsWins += 1;
        } else {
          dateAtsLosses += 1;
        }
        const probEdge = getPickProbEdge(row);
        if (probEdge !== null && probEdge > 0.1) {
          if (isWin) {
            dateAtsEdgeWins += 1;
            allAtsEdgeWins += 1;
          } else {
            dateAtsEdgeLosses += 1;
            allAtsEdgeLosses += 1;
          }
        }
        if (isWin) {
          allAtsWins += 1;
        } else {
          allAtsLosses += 1;
        }
      }
    }

    dateSquaredErrors.forEach((value) => allSquaredErrors.push(value));
    dateAbsErrors.forEach((value) => allAbsErrors.push(value));
    dateErrors.forEach((value) => allErrors.push(value));
    if (dateAtsWins + dateAtsLosses > 0) {
      allAtsResults.push(dateAtsWins / (dateAtsWins + dateAtsLosses));
    }
    if (dateAtsEdgeWins + dateAtsEdgeLosses > 0) {
      allAtsEdgeResults.push(
        dateAtsEdgeWins / (dateAtsEdgeWins + dateAtsEdgeLosses)
      );
    }

    dateSummaries.push({
      date: file.date,
      count: rows.length,
      mse: dateSquaredErrors.length ? average(dateSquaredErrors) : null,
      mae: dateAbsErrors.length ? average(dateAbsErrors) : null,
      meanError: dateErrors.length ? average(dateErrors) : null,
      atsRecord:
        dateAtsWins + dateAtsLosses > 0
          ? `${dateAtsWins}-${dateAtsLosses}`
          : null,
      atsRecordEdge:
        dateAtsEdgeWins + dateAtsEdgeLosses > 0
          ? `${dateAtsEdgeWins}-${dateAtsEdgeLosses}`
          : null
    });
  }

  return {
    props: {
      mse: allSquaredErrors.length ? average(allSquaredErrors) : null,
      mae: allAbsErrors.length ? average(allAbsErrors) : null,
      meanError: allErrors.length ? average(allErrors) : null,
      atsWinPct:
        allAtsWins + allAtsLosses > 0
          ? allAtsWins / (allAtsWins + allAtsLosses)
          : null,
      atsWinPctEdge:
        allAtsEdgeWins + allAtsEdgeLosses > 0
          ? allAtsEdgeWins / (allAtsEdgeWins + allAtsEdgeLosses)
          : null,
      atsRecord:
        allAtsWins + allAtsLosses > 0 ? `${allAtsWins}-${allAtsLosses}` : null,
      atsRecordEdge:
        allAtsEdgeWins + allAtsEdgeLosses > 0
          ? `${allAtsEdgeWins}-${allAtsEdgeLosses}`
          : null,
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
  atsRecord,
  atsRecordEdge,
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
          <span>ATS</span>
          <strong>{formatRecordWithPercent(atsRecord, atsWinPct)}</strong>
        </div>
        <div className="metric-card">
          <span>ATS (Prob Edge &gt; 10%)</span>
          <strong>{formatRecordWithPercent(atsRecordEdge, atsWinPctEdge)}</strong>
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
                <th>ATS Record</th>
                <th>ATS Record (Prob Edge &gt; 10%)</th>
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
                  <td>{summary.atsRecord ?? "-"}</td>
                  <td>{summary.atsRecordEdge ?? "-"}</td>
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

function formatRecordWithPercent(record: string | null, pct: number | null): string {
  const recordText = record ?? "-";
  const pctText = formatPercent(pct);
  if (recordText === "-" && pctText === "-") {
    return "-";
  }
  if (recordText === "-") {
    return pctText;
  }
  if (pctText === "-") {
    return recordText;
  }
  return `${recordText} (${pctText})`;
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
  const direct = parseNumeric(row.market_spread_home);
  if (direct !== null) {
    return direct;
  }
  const fallback = parseNumeric(row.home_spread_num ?? row.spread_home);
  if (fallback !== null) {
    return fallback;
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
  const direct = parseNumeric(row.pick_prob_edge ?? row.pickProbEdge);
  if (direct !== null) {
    return direct;
  }
  const cover = parseNumeric(row.pick_cover_prob ?? row.pickCoverProb);
  const breakeven = parseNumeric(
    row.pick_breakeven_prob ?? row.pickBreakevenProb ?? row.pick_breakeven
  );
  if (cover !== null && breakeven !== null) {
    return cover - breakeven;
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

function parseNumeric(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
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
