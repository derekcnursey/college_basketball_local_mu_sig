import { GetServerSideProps } from "next";
import { useMemo, useState } from "react";
import Layout from "../components/Layout";
import {
  PredictionRow,
  getEdgeValue,
  getPickProbEdge,
  getTeams,
  pickColumns
} from "../lib/data";
import {
  getLatestPredictionFile,
  getPredictionRowsByFilename,
  listPredictionFiles
} from "../lib/server-data";

type HomeProps = {
  date: string | null;
  rows: PredictionRow[];
  columns: string[];
  availableDates: string[];
};

type SortState = {
  key: string;
  direction: "asc" | "desc";
  useAbs?: boolean;
  secondaryKey?: string;
  secondaryUseAbs?: boolean;
};

export const getServerSideProps: GetServerSideProps<HomeProps> = async () => {
  const latest = getLatestPredictionFile();
  if (!latest) {
    return {
      props: {
        date: null,
        rows: [],
        columns: [],
        availableDates: []
      }
    };
  }
  const rows = getPredictionRowsByFilename(latest.filename);
  const columns = pickColumns(rows);
  const availableDates = listPredictionFiles().map((file) => file.date);

  return {
    props: {
      date: latest.date,
      rows,
      columns,
      availableDates
    }
  };
};

function sortRows(rows: PredictionRow[], sort: SortState): PredictionRow[] {
  const sorted = [...rows];
  sorted.sort((a, b) => {
    const aValue = getSortValue(a, sort.key);
    const bValue = getSortValue(b, sort.key);

    if (typeof aValue === "number" && typeof bValue === "number") {
      const delta = sort.useAbs
        ? Math.abs(aValue) - Math.abs(bValue)
        : aValue - bValue;
      if (delta !== 0) {
        return sort.direction === "asc" ? delta : -delta;
      }
      if (sort.secondaryKey) {
        const aSecondary = getSortValue(a, sort.secondaryKey);
        const bSecondary = getSortValue(b, sort.secondaryKey);
        if (typeof aSecondary === "number" && typeof bSecondary === "number") {
          const secondaryDelta = sort.secondaryUseAbs
            ? Math.abs(aSecondary) - Math.abs(bSecondary)
            : aSecondary - bSecondary;
          return sort.direction === "asc" ? secondaryDelta : -secondaryDelta;
        }
      }
      return 0;
    }
    const aText = String(aValue).toLowerCase();
    const bText = String(bValue).toLowerCase();
    if (aText === bText) {
      return 0;
    }
    const result = aText > bText ? 1 : -1;
    return sort.direction === "asc" ? result : -result;
  });
  return sorted;
}

function getSortValue(row: PredictionRow, key: string): string | number {
  if (key === "edge_home_points" || key === "edge_points") {
    return getEdgeValue(row);
  }
  if (key === "pick_prob_edge") {
    return getPickProbEdge(row);
  }
  const value = row[key];
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
    return value;
  }
  return value ? String(value) : "";
}

export default function Home({ date, rows, columns, availableDates }: HomeProps) {
  const [search, setSearch] = useState("");
  const [team, setTeam] = useState("all");
  const [sort, setSort] = useState<SortState>({
    key: "pick_prob_edge",
    direction: "desc",
    useAbs: true,
    secondaryKey: "edge_home_points",
    secondaryUseAbs: true
  });

  const teams = useMemo(() => {
    const set = new Set<string>();
    rows.forEach((row) => {
      const { home, away } = getTeams(row);
      if (home) {
        set.add(home);
      }
      if (away) {
        set.add(away);
      }
    });
    return Array.from(set).sort();
  }, [rows]);

  const filteredRows = useMemo(() => {
    return rows.filter((row) => {
      const { home, away } = getTeams(row);
      if (team !== "all") {
        if (home !== team && away !== team) {
          return false;
        }
      }
      if (!search.trim()) {
        return true;
      }
      const haystack = JSON.stringify(row).toLowerCase();
      return haystack.includes(search.trim().toLowerCase());
    });
  }, [rows, team, search]);

  const sortedRows = useMemo(() => sortRows(filteredRows, sort), [filteredRows, sort]);

  function handleSort(key: string) {
    setSort((prev) => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === "asc" ? "desc" : "asc",
          useAbs: key === "edge_home_points" || key === "pick_prob_edge",
          secondaryKey: undefined,
          secondaryUseAbs: false
        };
      }
      return {
        key,
        direction: "desc",
        useAbs: key === "edge_home_points" || key === "pick_prob_edge",
        secondaryKey: undefined,
        secondaryUseAbs: false
      };
    });
  }

  return (
    <Layout>
      <section className="hero">
        <div>
          <p className="eyebrow">Latest file</p>
          <h1>Predictions {date ? `(${date})` : ""}</h1>
          <p className="muted">
            Sorted by absolute pick prob edge, then edge points by default.
          </p>
        </div>
        <div className="pill">{availableDates.length} dates on disk</div>
      </section>

      <section className="controls">
        <label className="control">
          <span>Search</span>
          <input
            type="search"
            placeholder="Team, matchup, or metric"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </label>
        <label className="control">
          <span>Team</span>
          <select value={team} onChange={(event) => setTeam(event.target.value)}>
            <option value="all">All teams</option>
            {teams.map((teamName) => (
              <option key={teamName} value={teamName}>
                {teamName}
              </option>
            ))}
          </select>
        </label>
      </section>

      {!rows.length ? (
        <div className="empty">Drop prediction JSON files into public/data.</div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                {columns.map((column) => (
                  <th key={column}>
                    <button
                      type="button"
                      onClick={() => handleSort(column)}
                      className={sort.key === column ? "active" : ""}
                    >
                      {columnLabels[column] ?? column}
                      {sort.key === column ? (
                        <span className="sort">
                          {sort.direction === "asc" ? "↑" : "↓"}
                        </span>
                      ) : null}
                    </button>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row, index) => (
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
