import { GetServerSideProps } from "next";
import { useRouter } from "next/router";
import { useMemo } from "react";
import Layout from "../components/Layout";
import { PredictionRow, pickColumns } from "../lib/data";
import {
  getLatestPredictionFile,
  getPredictionRowsByDate,
  listPredictionFiles
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
  const availableDates = listPredictionFiles()
    .map((file) => file.date)
    .sort((a, b) => (a < b ? 1 : -1));
  const latest = getLatestPredictionFile();
  const date = queryDate || latest?.date || null;
  const rows = date ? getPredictionRowsByDate(date) : [];
  const columns = pickColumns(rows);

  return {
    props: {
      date,
      rows,
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
      <section className="hero">
        <div>
          <p className="eyebrow">History</p>
          <h1>Pick a date</h1>
          <p className="muted">Loads predictions_{`{date}`}.json from public/data.</p>
        </div>
        <div className="pill">{availableDates.length} dates on disk</div>
      </section>

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

      {!date ? (
        <div className="empty">No prediction files found.</div>
      ) : !availableSet.has(date) ? (
        <div className="empty">No file found for {date}. Try another date.</div>
      ) : !rows.length ? (
        <div className="empty">File is empty for {date}.</div>
      ) : (
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
  pick_prob_edge: "Prob Edge",
  model_mu_home: "Model Spread (Home)",
  market_spread_home: "Book Spread (Home)",
  edge_home_points: "Point Edge",
  pred_sigma: "Sigma",
  pick_ev_per_1: "EV per $1",
  neutral_site: "Neutral Site"
};
