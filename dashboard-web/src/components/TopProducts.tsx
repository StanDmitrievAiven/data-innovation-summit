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
import type { ProductRow } from "../api";

interface Props {
  title: string;
  rows: ProductRow[];
  metric: "units_sold" | "revenue_eur";
  unit?: string;
}

const COLORS = ["#5cf2c2", "#6aa9ff", "#ffc857", "#ff6b8b", "#a08cff"];

export function TopProducts({ title, rows, metric, unit }: Props) {
  const data = rows.slice(0, 10).map((r) => ({
    product: r.product,
    category: r.category,
    value: Number(r[metric]),
  }));

  return (
    <div className="panel">
      <h2>{title}</h2>
      <ResponsiveContainer width="100%" height={Math.max(220, data.length * 28)}>
        <BarChart data={data} layout="vertical" margin={{ left: 10, right: 24 }}>
          <CartesianGrid horizontal={false} stroke="#232a44" />
          <XAxis
            type="number"
            stroke="#8e96b0"
            tick={{ fontSize: 11 }}
            tickFormatter={(v: number) =>
              unit === "eur"
                ? "€" + Intl.NumberFormat("en", { notation: "compact" }).format(v)
                : Intl.NumberFormat("en", { notation: "compact" }).format(v)
            }
          />
          <YAxis
            dataKey="product"
            type="category"
            width={170}
            stroke="#8e96b0"
            tick={{ fontSize: 12, fill: "#e7eaf6" }}
          />
          <Tooltip
            cursor={{ fill: "rgba(255,255,255,0.04)" }}
            contentStyle={{
              background: "#131727",
              border: "1px solid #232a44",
              borderRadius: 8,
              color: "#e7eaf6",
            }}
            formatter={(v: number) =>
              unit === "eur"
                ? `€${Intl.NumberFormat("en").format(v)}`
                : Intl.NumberFormat("en").format(v)
            }
          />
          <Bar dataKey="value" radius={[0, 6, 6, 0]}>
            {data.map((_, i) => (
              <Cell key={i} fill={COLORS[i % COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
