import React from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { RegionRow } from "../api";

interface Props {
  title: string;
  rows: RegionRow[];
}

function colorFor(region: string): string {
  if (region.startsWith("EMEA")) return "#5cf2c2";
  if (region === "AMER") return "#6aa9ff";
  if (region === "APAC") return "#ffc857";
  return "#a08cff";
}

export function RegionChart({ title, rows }: Props) {
  const data = rows
    .slice()
    .sort((a, b) => Number(b.orders) - Number(a.orders))
    .map((r) => ({
      region: r.region,
      orders: Number(r.orders),
      revenue: Number(r.revenue_eur),
      customers: Number(r.unique_customers),
    }));

  return (
    <div className="panel">
      <h2>{title}</h2>
      <ResponsiveContainer width="100%" height={260}>
        <BarChart data={data}>
          <CartesianGrid stroke="#232a44" strokeDasharray="3 3" />
          <XAxis dataKey="region" stroke="#8e96b0" tick={{ fontSize: 11 }} />
          <YAxis stroke="#8e96b0" tick={{ fontSize: 11 }} />
          <Tooltip
            cursor={{ fill: "rgba(255,255,255,0.04)" }}
            contentStyle={{
              background: "#131727",
              border: "1px solid #232a44",
              borderRadius: 8,
              color: "#e7eaf6",
            }}
            formatter={(v: number, name: string) =>
              name === "revenue"
                ? [`€${Intl.NumberFormat("en").format(v)}`, "revenue"]
                : [Intl.NumberFormat("en").format(v), name]
            }
          />
          <Bar dataKey="orders" radius={[6, 6, 0, 0]}>
            {data.map((d, i) => (
              <Cell key={i} fill={colorFor(d.region)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
