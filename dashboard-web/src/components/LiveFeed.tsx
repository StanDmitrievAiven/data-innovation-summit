import React from "react";
import type { LiveOrder } from "../api";

interface Props {
  orders: LiveOrder[];
}

function regionClass(r: string): string {
  if (r.startsWith("EMEA")) return "region-tag";
  if (r === "AMER") return "region-tag amer";
  if (r === "APAC") return "region-tag apac";
  return "region-tag";
}

function fmtTime(s: string): string {
  return new Date(s).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function LiveFeed({ orders }: Props) {
  return (
    <div className="panel">
      <h2>Live order feed</h2>
      <table className="list">
        <thead>
          <tr>
            <th>Time</th>
            <th>Customer</th>
            <th>Region</th>
            <th>Status</th>
            <th className="right">Total</th>
          </tr>
        </thead>
        <tbody>
          {orders.map((o) => (
            <tr key={String(o.order_id)}>
              <td>{fmtTime(o.created_at)}</td>
              <td>
                {o.customer_name ?? "—"}{" "}
                <span style={{ color: "var(--muted)", fontSize: 11 }}>
                  {o.country ?? ""}
                </span>
              </td>
              <td>
                <span className={regionClass(o.region)}>{o.region}</span>
              </td>
              <td>
                <span className={`status-tag ${o.status}`}>{o.status}</span>
              </td>
              <td className="right">€{Number(o.total).toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
