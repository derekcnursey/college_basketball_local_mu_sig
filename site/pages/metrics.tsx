import { GetServerSideProps } from "next";
import Layout from "../components/Layout";
import { PredictionRow, getEdgeValue, getPickProbEdge, normalizeRows } from "../lib/data";
import { listPredictionFiles, readJsonFile } from "../lib/server-data";

type DateSummary = {
  date: string;
  count: number;
  avgAbsEdgePoints: number | null;
  avgPickProbEdge: number | null;
};

type MetricsProps = {
  totalGames: number;
  avgAbsEdgePoints: number | null;
  avgPickProbEdge: number | null;
  avgAbsModelMuHome: number | null;
  dateSummaries: DateSummary[];
};

export const getServerSideProps: GetServerSideProps<MetricsProps> = async () => {
  const files = listPredictionFiles();
  const dateSummaries: DateSummary[] = [];
  const allEdges: number[] = [];
  const allPickProbEdges: number[] = [];
  const allModelMu: number[] = [];

  for (const file of files) {
    const payload = readJsonFile(file.filename);
    const rows = normalizeRows(payload);

    if (!rows.length) {
      dateSummaries.push({
        date: file.date,
        count: 0,
        avgAbsEdgePoints: null,
        avgPickProbEdge: null
      });
      continue;
    }

    const absEdges = rows.map((row) => Math.abs(getEdgeValue(row)));
    const pickProbEdges = rows.map(getPickProbEdge);
    const absModelMu = rows.map((row) => Math.abs(getModelMuHome(row)));

    absEdges.forEach((value) => allEdges.push(value));
    pickProbEdges.forEach((value) => allPickProbEdges.push(value));
    absModelMu.forEach((value) => allModelMu.push(value));

    dateSummaries.push({
      date: file.date,
      count: rows.length,
      avgAbsEdgePoints: average(absEdges),
      avgPickProbEdge: average(pickProbEdges)
    });
  }

  return {
    props: {
      totalGames: allEdges.length,
      avgAbsEdgePoints: allEdges.length ? average(allEdges) : null,
      avgPickProbEdge: allPickProbEdges.length ? average(allPickProbEdges) : null,
      avgAbsModelMuHome: allModelMu.length ? average(allModelMu) : null,
      dateSummaries
    }
  };
};

export default function Metrics({
  totalGames,
  avgAbsEdgePoints,
  avgPickProbEdge,
  avgAbsModelMuHome,
  dateSummaries
}: MetricsProps) {
  return (
    <Layout>
      <section className="hero">
        <div>
          <p className="eyebrow">Metrics</p>
          <h1>Model summary</h1>
          <p className="muted">
            Scans every predictions_YYYY-MM-DD.json in public/data.
          </p>
        </div>
        <div className="pill">{totalGames} games</div>
      </section>

      <section className="metrics-grid">
        <div className="metric-card">
          <span>Total games</span>
          <strong>{totalGames}</strong>
        </div>
        <div className="metric-card">
          <span>Avg |edge_points|</span>
          <strong>{formatNumber(avgAbsEdgePoints)}</strong>
        </div>
        <div className="metric-card">
          <span>Avg pick_prob_edge</span>
          <strong>{formatNumber(avgPickProbEdge)}</strong>
        </div>
        <div className="metric-card">
          <span>Avg |model_mu_home|</span>
          <strong>{formatNumber(avgAbsModelMuHome)}</strong>
        </div>
      </section>

      <section>
        <h2>By date</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Games</th>
                <th>Avg |edge_points|</th>
                <th>Avg pick_prob_edge</th>
              </tr>
            </thead>
            <tbody>
              {dateSummaries.map((summary) => (
                <tr key={summary.date}>
                  <td>{summary.date}</td>
                  <td>{summary.count}</td>
                  <td>{formatNumber(summary.avgAbsEdgePoints)}</td>
                  <td>{formatNumber(summary.avgPickProbEdge)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="muted">
          TODO: Add result-based accuracy metrics once final scores are stored.
        </p>
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

function getModelMuHome(row: PredictionRow): number {
  const value = row.model_mu_home ?? row.pred_margin ?? row.modelMuHome;
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return 0;
}
