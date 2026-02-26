import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine
} from "recharts";

type ChartData = { week: string; units: number };

const tickStyle = {
  fontSize: 10,
  fontFamily: "'IBM Plex Mono', monospace",
  fill: "#94a3b8"
};

export default function CumulativeChart({ data }: { data: ChartData[] }) {
  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart
        data={data}
        margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis dataKey="week" tick={tickStyle} interval={2} />
        <YAxis
          tickFormatter={(v: number) => `${v}u`}
          tick={tickStyle}
        />
        <ReferenceLine y={0} stroke="#e2e8f0" />
        <Tooltip
          contentStyle={{
            background: "#0f172a",
            border: "none",
            borderRadius: 6,
            fontFamily: "'IBM Plex Mono', monospace",
            fontSize: 12,
            color: "#fff"
          }}
          formatter={(value?: number) => {
            const v = value ?? 0;
            return [`${v >= 0 ? "+" : ""}${v.toFixed(1)}u`, "Units"];
          }}
          labelStyle={{ color: "#94a3b8" }}
        />
        <Line
          type="monotone"
          dataKey="units"
          stroke="#16a34a"
          strokeWidth={2.5}
          dot={false}
          activeDot={{
            stroke: "#fff",
            strokeWidth: 2,
            r: 4,
            fill: "#16a34a"
          }}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
