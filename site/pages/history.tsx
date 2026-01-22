import { GetServerSideProps } from "next";
import { useRouter } from "next/router";
import { useMemo } from "react";
import Layout from "../components/Layout";
import {
  PredictionRow,
  getTeams,
  normalizeTeam,
  pickColumns
} from "../lib/data";
import {
  getLatestPredictionFile,
  getPredictionRowsByDate,
  listFinalScoreFiles,
  listPredictionFiles,
  readJsonFile
} from "../lib/server-data";

type HistoryProps = {
  date: string | null;
  rows: PredictionRow[];
  columns: string[];
  availableDates: string[];
};

export const getServerSideProps: GetServerSideProps<HistoryProps> = async (
  context
) => {
  const queryDate = typeof context.query.date === "string" ? context.query.date : null;
  const today = formatDate(new Date());
  const availableDates = listPredictionFiles()
    .map((file) => file.date)
    .filter((date) => date !== today)
    .sort((a, b) => (a < b ? 1 : -1));
  const latest = getLatestPredictionFile();
  const fallbackDate =
    latest && latest.date !== today ? latest.date : availableDates[0] ?? null;
  const date = queryDate || fallbackDate || null;
  const rows = date ? getPredictionRowsByDate(date) : [];
  const finalScores = date ? getFinalScoresByDate(date) : [];
  const rowsWithScores = attachFinalScores(rows, finalScores);
  const columns = pickColumns(rowsWithScores).filter(
    (column) => column !== "neutral_site"
  );
  if (!columns.includes("final_score")) {
    columns.splice(2, 0, "final_score");
  }
  if (!columns.includes("model_error")) {
    columns.splice(3, 0, "model_error");
  }

  return {
    props: {
      date,
      rows: rowsWithScores,
      columns,
      availableDates
    }
  };
};

export default function History({ date, rows, columns, availableDates }: HistoryProps) {
  const router = useRouter();
  const availableSet = useMemo(() => new Set(availableDates), [availableDates]);
  const summary = useMemo(() => buildDailySummary(rows), [rows]);

  function handleDateChange(nextDate: string) {
    if (!nextDate) {
      return;
    }
    router.push({ pathname: "/history", query: { date: nextDate } });
  }

  return (
    <Layout>
      {!date ? (
        <div className="empty">No prediction files found.</div>
      ) : !availableSet.has(date) ? (
        <div className="empty">No file found for {date}. Try another date.</div>
      ) : !rows.length ? (
        <div className="empty">File is empty for {date}.</div>
      ) : (
        <div className="data-panel">
          <section className="controls">
            <label className="control">
              <span>Date</span>
              <div className="date-row">
                <input
                  type="date"
                  value={date ?? ""}
                  onChange={(event) => handleDateChange(event.target.value)}
                  list="available-dates"
                />
                <span className="date-summary">{rows.length} games</span>
              </div>
              <datalist id="available-dates">
                {availableDates.map((value) => (
                  <option key={value} value={value} />
                ))}
              </datalist>
            </label>
          </section>
          {summary && (
            <section className="metrics-grid">
              <div className="metric-card">
                <span className="label">ATS Record</span>
                <span className="value">{summary.atsCount}</span>
              </div>
              <div className="metric-card">
                <span className="label">MAE</span>
                <span className="value">{summary.mae}</span>
              </div>
              <div className="metric-card">
                <span className="label">MSE</span>
                <span className="value">{summary.mse}</span>
              </div>
            </section>
          )}
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th key={column}>{columnLabels[column] ?? column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, index) => (
                  <tr key={index}>
                    {columns.map((column) => (
                      <td key={column} className={getCellClass(row, column)}>
                        {formatCell(row[column], column)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </Layout>
  );
}

function formatCell(value: unknown, column?: string): string {
  if (column === "market_spread_home" && (value === null || value === undefined)) {
    return "No Data";
  }
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "number") {
    return formatNumberByColumn(value, column);
  }
  if (typeof value === "string") {
    if (column === "market_spread_home" && value.trim() === "") {
      return "No Data";
    }
    const numeric = Number(value);
    if (!Number.isNaN(numeric)) {
      return formatNumberByColumn(numeric, column);
    }
    return value;
  }
  return JSON.stringify(value);
}

function formatNumberByColumn(value: number, column?: string): string {
  const showPlus = column === "model_mu_home" || column === "market_spread_home";
  const adjusted = column === "edge_home_points" ? Math.abs(value) : value;
  if (column === "model_error") {
    return adjusted.toFixed(2);
  }
  const formatted = Number.isInteger(adjusted)
    ? adjusted.toString()
    : adjusted.toFixed(2);
  if (showPlus && adjusted > 0) {
    return `+${formatted}`;
  }
  return formatted;
}

const columnLabels: Record<string, string> = {
  away_team: "Away",
  home_team: "Home",
  final_score: "Final",
  model_error: "Error",
  pick_prob_edge: "Prob Edge",
  model_mu_home: "Model Spread (Home)",
  market_spread_home: "Book Spread (Home)",
  edge_home_points: "Point Edge",
  pred_sigma: "Sigma",
  pick_ev_per_1: "EV per $1"
};

function getFinalScoresByDate(date: string): PredictionRow[] {
  const filename = `final_scores_${date}.json`;
  const payload = readJsonFile(filename);
  return Array.isArray(payload)
    ? (payload as PredictionRow[])
    : ((payload as { games?: PredictionRow[] } | null)?.games ?? []);
}

function attachFinalScores(
  rows: PredictionRow[],
  finalRows: PredictionRow[]
): PredictionRow[] {
  if (!rows.length || !finalRows.length) {
    return rows;
  }
  const lookup = buildFinalScoreLookup(finalRows);
  return rows.map((row) => {
    const key = getRowKey(row);
    const finalRow = key ? lookup.get(key) : null;
    if (!finalRow) {
      return row;
    }
    const awayScore = parseNumeric(finalRow.away_score ?? finalRow.score_away);
    const homeScore = parseNumeric(finalRow.home_score ?? finalRow.score_home);
    const scoreText =
      awayScore !== null && homeScore !== null ? `${awayScore}-${homeScore}` : null;
    const modelError =
      awayScore !== null && homeScore !== null
        ? getModelError(row, homeScore, awayScore)
        : null;
    return {
      ...row,
      final_score: scoreText,
      away_score: awayScore,
      home_score: homeScore,
      model_error: modelError
    };
  });
}

function buildFinalScoreLookup(rows: PredictionRow[]): Map<string, PredictionRow> {
  const lookup = new Map<string, PredictionRow>();
  for (const row of rows) {
    const gameId = getGameId(row);
    if (gameId) {
      lookup.set(gameId, row);
    }
    const teamKey = getTeamKey(row);
    if (teamKey) {
      lookup.set(teamKey, row);
    }
  }
  return lookup;
}

function getRowKey(row: PredictionRow): string | null {
  return getGameId(row) ?? getTeamKey(row);
}

function getGameId(row: PredictionRow): string | null {
  const gameId = row.game_id ?? row.gameId;
  if (typeof gameId === "string" && gameId.trim() !== "") {
    return gameId;
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

function getCellClass(row: PredictionRow, column: string): string {
  if (column !== "final_score") {
    return "";
  }
  const ats = getAtsResult(row);
  if (ats === "win") {
    return "final-score final-score--win";
  }
  if (ats === "loss") {
    return "final-score final-score--loss";
  }
  return "final-score";
}

function buildDailySummary(rows: PredictionRow[]): {
  atsCount: string;
  mae: string;
  mse: string;
} | null {
  if (!rows.length) {
    return null;
  }

  let atsWins = 0;
  let atsLosses = 0;
  let atsPushes = 0;
  let errorCount = 0;
  let absErrorSum = 0;
  let sqErrorSum = 0;

  for (const row of rows) {
    const ats = getAtsResult(row);
    if (ats === "win") {
      atsWins += 1;
    } else if (ats === "loss") {
      atsLosses += 1;
    } else if (ats === "push") {
      atsPushes += 1;
    }

    const actualSpread = getActualSpreadFromRow(row);
    const modelSpread = getModelSpreadFromRow(row);
    if (actualSpread !== null && modelSpread !== null) {
      const err = modelSpread - actualSpread;
      absErrorSum += Math.abs(err);
      sqErrorSum += err * err;
      errorCount += 1;
    }
  }

  const atsTotal = atsWins + atsLosses;
  const atsCount = atsTotal > 0 ? `${atsWins}-${atsLosses}` : "No ATS";

  const mae = errorCount > 0 ? (absErrorSum / errorCount).toFixed(2) : "—";
  const mse = errorCount > 0 ? (sqErrorSum / errorCount).toFixed(2) : "—";

  return {
    atsCount,
    mae,
    mse
  };
}

function getAtsResult(row: PredictionRow): "win" | "loss" | "push" | null {
  const actualSpread = getActualSpreadFromRow(row);
  const margin = actualSpread === null ? null : -actualSpread;
  const spread = parseNumeric(row.market_spread_home ?? row.home_spread_num);
  const pickSideRaw = row.pick_side ?? row.pickSide;
  if (margin === null || spread === null || typeof pickSideRaw !== "string") {
    return null;
  }

  const pickSide = pickSideRaw.trim().toUpperCase();
  if (pickSide !== "HOME" && pickSide !== "AWAY") {
    return null;
  }

  const cover = margin + spread;
  if (cover === 0) {
    return "push";
  }
  const coverSide = cover > 0 ? "HOME" : "AWAY";
  return pickSide === coverSide ? "win" : "loss";
}

function getModelError(
  row: PredictionRow,
  homeScore: number,
  awayScore: number
): number | null {
  const modelSpread = getModelSpreadFromRow(row);
  if (modelSpread === null) {
    return null;
  }
  const actualSpread = awayScore - homeScore;
  return modelSpread - actualSpread;
}

function getModelSpreadFromRow(row: PredictionRow): number | null {
  const candidates = [row.model_mu_home, row.model_home_spread];
  for (const value of candidates) {
    const parsed = parseNumeric(value);
    if (parsed !== null) {
      return Number(parsed.toFixed(2));
    }
  }
  return null;
}

function getActualSpreadFromRow(row: PredictionRow): number | null {
  const homeScore = parseNumeric(
    row.home_score ?? row.score_home ?? row.homeScore ?? row.scoreHome
  );
  const awayScore = parseNumeric(
    row.away_score ?? row.score_away ?? row.awayScore ?? row.scoreAway
  );
  if (homeScore === null || awayScore === null) {
    return null;
  }
  return awayScore - homeScore;
}

function parseNumeric(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") {
      return null;
    }
    const parsed = Number(trimmed);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function formatDate(value: Date): string {
  const year = value.getFullYear();
  const month = `${value.getMonth() + 1}`.padStart(2, "0");
  const day = `${value.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}
