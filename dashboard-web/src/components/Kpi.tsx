import React from "react";

interface Props {
  label: string;
  value: string | number;
  hint?: string;
}

export function Kpi({ label, value, hint }: Props) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {hint ? <div className="hint">{hint}</div> : null}
    </div>
  );
}
