import { GetServerSideProps } from "next";
import { useRouter } from "next/router";
import { useMemo } from "react";
import Layout from "../components/Layout";
import { PredictionRow, getTeams, normalizeTeam, pickColumns } from "../lib/data";
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
              <input
                type="date"
                value={date ?? ""}
                onChange={(event) => handleDateChange(event.target.value)}
                list="available-dates"
              />
              <datalist id="available-dates">
                {availableDates.map((value) => (
                  <option key={value} value={value} />
                ))}
              </datalist>
            </label>
          </section>
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
                      <td key={column}>{formatCell(row[column], column)}</td>
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
    const awayScore = finalRow.away_score ?? finalRow.score_away;
    const homeScore = finalRow.home_score ?? finalRow.score_home;
    const scoreText =
      typeof awayScore === "number" && typeof homeScore === "number"
        ? `${awayScore}-${homeScore}`
        : typeof awayScore === "string" && typeof homeScore === "string"
          ? `${awayScore}-${homeScore}`
          : null;
    return { ...row, final_score: scoreText };
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

function formatDate(value: Date): string {
  const year = value.getFullYear();
  const month = `${value.getMonth() + 1}`.padStart(2, "0");
  const day = `${value.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}
