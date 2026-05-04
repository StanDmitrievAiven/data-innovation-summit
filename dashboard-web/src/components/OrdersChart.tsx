import React from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TimeseriesPoint } from "../api";

interface Props {
  title: string;
  points: TimeseriesPoint[];
}

function fmtTime(s: string): string {
  const d = new Date(s);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export function OrdersChart({ title, points }: Props) {
  const data = points.map((p) => ({
    time: fmtTime(p.bucket_at),
    orders: Number(p.orders),
    revenue: Number(p.revenue_eur),
  }));

  return (
    <div className="panel">
      <h2>{title}</h2>
      <ResponsiveContainer width="100%" height={260}>
        <AreaChart data={data}>
          <defs>
            <linearGradient id="ordersGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#5cf2c2" stopOpacity={0.6} />
              <stop offset="100%" stopColor="#5cf2c2" stopOpacity={0.05} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke="#232a44" strokeDasharray="3 3" />
          <XAxis dataKey="time" stroke="#8e96b0" tick={{ fontSize: 11 }} interval="preserveEnd" />
          <YAxis stroke="#8e96b0" tick={{ fontSize: 11 }} />
          <Tooltip
            contentStyle={{
              background: "#131727",
              border: "1px solid #232a44",
              borderRadius: 8,
              color: "#e7eaf6",
            }}
            formatter={(v: number, name: string) =>
              name === "revenue"
                ? [`€${Intl.NumberFormat("en").format(v)}`, "revenue"]
                : [Intl.NumberFormat("en").format(v), "orders"]
            }
          />
          <Area
            type="monotone"
            dataKey="orders"
            stroke="#5cf2c2"
            strokeWidth={2}
            fill="url(#ordersGrad)"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
