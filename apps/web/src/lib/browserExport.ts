import type { ChartSpec } from "../types";

type ChartPoint = {
  label: string;
  value: number;
};

const CHART_COLORS = ["#c85c3c", "#1d5f85", "#ef9f34", "#2f8f6b", "#7a4a8b", "#7c8d3d"];

function quoteCsvCell(value: unknown): string {
  if (value === null || value === undefined) {
    return "\"\"";
  }
  const normalized = typeof value === "object" ? JSON.stringify(value) : String(value);
  return `"${normalized.replace(/"/g, "\"\"")}"`;
}

export function serializeCsv(columns: string[], rows: unknown[][]): string {
  const headerRow = columns.map((column) => quoteCsvCell(column)).join(",");
  const bodyRows = rows.map((row) => row.map((cell) => quoteCsvCell(cell)).join(","));
  return [headerRow, ...bodyRows].join("\n");
}

export async function copyText(text: string): Promise<void> {
  const normalized = text.trim();
  if (!normalized) {
    throw new Error("Cannot copy empty text");
  }

  if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(normalized);
    return;
  }

  if (typeof document === "undefined") {
    throw new Error("Clipboard API unavailable");
  }

  const textarea = document.createElement("textarea");
  textarea.value = normalized;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.append(textarea);
  textarea.focus();
  textarea.select();

  try {
    const copied = document.execCommand?.("copy");
    if (!copied) {
      throw new Error("Clipboard copy failed");
    }
  } finally {
    textarea.remove();
  }
}

function downloadBlob(filename: string, blob: Blob): void {
  if (typeof document === "undefined" || typeof URL === "undefined") {
    throw new Error("File download API unavailable");
  }

  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = filename;
  anchor.style.display = "none";
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(objectUrl);
}

function toChartPoints(spec: ChartSpec, data: Record<string, unknown>[]): ChartPoint[] {
  return data
    .map((item) => ({
      label: String(item[spec.x] ?? ""),
      value: Number(item[spec.y] ?? 0),
    }))
    .filter((item) => Number.isFinite(item.value));
}

function valueRange(points: ChartPoint[]): { min: number; max: number } {
  const values = points.map((point) => point.value);
  const min = Math.min(0, ...values);
  const max = Math.max(0, ...values);
  if (Math.abs(max - min) < 1e-9) {
    return { min, max: min + 1 };
  }
  return { min, max };
}

function toY(value: number, min: number, max: number, plotY: number, plotHeight: number): number {
  return plotY + ((max - value) / (max - min)) * plotHeight;
}

function truncateLabel(label: string, maxLength = 14): string {
  return label.length > maxLength ? `${label.slice(0, maxLength - 1)}…` : label;
}

function drawLineChart(ctx: CanvasRenderingContext2D, points: ChartPoint[]): void {
  const plotX = 80;
  const plotY = 110;
  const plotWidth = 1040;
  const plotHeight = 430;
  const { min, max } = valueRange(points);
  const baselineY = toY(0, min, max, plotY, plotHeight);
  const xGap = points.length === 1 ? 0 : plotWidth / Math.max(points.length - 1, 1);

  ctx.strokeStyle = "rgba(23, 50, 84, 0.25)";
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(plotX, baselineY);
  ctx.lineTo(plotX + plotWidth, baselineY);
  ctx.stroke();

  ctx.strokeStyle = "#c85c3c";
  ctx.lineWidth = 4;
  ctx.lineJoin = "round";
  ctx.lineCap = "round";
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = points.length === 1 ? plotX + plotWidth / 2 : plotX + xGap * index;
    const y = toY(point.value, min, max, plotY, plotHeight);
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  ctx.fillStyle = "#1d5f85";
  points.forEach((point, index) => {
    const x = points.length === 1 ? plotX + plotWidth / 2 : plotX + xGap * index;
    const y = toY(point.value, min, max, plotY, plotHeight);
    ctx.beginPath();
    ctx.arc(x, y, 5, 0, Math.PI * 2);
    ctx.fill();
  });

  ctx.fillStyle = "#173254";
  ctx.font = "500 18px 'IBM Plex Sans', sans-serif";
  ctx.textAlign = "center";
  points.forEach((point, index) => {
    const x = points.length === 1 ? plotX + plotWidth / 2 : plotX + xGap * index;
    ctx.fillText(truncateLabel(point.label, 16), x, plotY + plotHeight + 32);
  });
}

function drawBarChart(ctx: CanvasRenderingContext2D, points: ChartPoint[]): void {
  const plotX = 80;
  const plotY = 110;
  const plotWidth = 1040;
  const plotHeight = 430;
  const { min, max } = valueRange(points);
  const baselineY = toY(0, min, max, plotY, plotHeight);
  const barWidth = Math.max(16, Math.min(72, (plotWidth / points.length) * 0.62));
  const totalBarsWidth = barWidth * points.length;
  const gap = points.length > 1 ? (plotWidth - totalBarsWidth) / (points.length - 1) : 0;

  ctx.strokeStyle = "rgba(23, 50, 84, 0.25)";
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(plotX, baselineY);
  ctx.lineTo(plotX + plotWidth, baselineY);
  ctx.stroke();

  ctx.textAlign = "center";
  points.forEach((point, index) => {
    const x = plotX + index * (barWidth + gap);
    const top = toY(Math.max(point.value, 0), min, max, plotY, plotHeight);
    const bottom = toY(Math.min(point.value, 0), min, max, plotY, plotHeight);
    const y = Math.min(top, bottom);
    const height = Math.max(3, Math.abs(top - bottom));

    ctx.fillStyle = CHART_COLORS[index % CHART_COLORS.length];
    ctx.fillRect(x, y, barWidth, height);

    ctx.fillStyle = "#173254";
    ctx.font = "500 16px 'IBM Plex Sans', sans-serif";
    ctx.fillText(truncateLabel(point.label, 14), x + barWidth / 2, plotY + plotHeight + 32);

    ctx.fillStyle = "rgba(23, 50, 84, 0.72)";
    ctx.font = "600 15px 'IBM Plex Sans', sans-serif";
    const valueY = point.value >= 0 ? y - 8 : y + height + 18;
    ctx.fillText(point.value.toLocaleString(), x + barWidth / 2, valueY);
  });
}

function drawPieChart(ctx: CanvasRenderingContext2D, points: ChartPoint[]): void {
  const positivePoints = points.map((point) => ({ ...point, value: Math.max(0, point.value) }));
  const total = positivePoints.reduce((sum, point) => sum + point.value, 0);
  if (total <= 0) {
    throw new Error("No chart data to export");
  }

  const cx = 300;
  const cy = 330;
  const radius = 170;
  let startAngle = -Math.PI / 2;

  positivePoints.forEach((point, index) => {
    const angle = (point.value / total) * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, radius, startAngle, startAngle + angle);
    ctx.closePath();
    ctx.fillStyle = CHART_COLORS[index % CHART_COLORS.length];
    ctx.fill();
    startAngle += angle;
  });

  ctx.fillStyle = "#fff";
  ctx.beginPath();
  ctx.arc(cx, cy, 70, 0, Math.PI * 2);
  ctx.fill();

  ctx.fillStyle = "#173254";
  ctx.font = "600 20px 'IBM Plex Sans', sans-serif";
  ctx.textAlign = "left";
  positivePoints.forEach((point, index) => {
    const y = 190 + index * 40;
    const color = CHART_COLORS[index % CHART_COLORS.length];
    const share = ((point.value / total) * 100).toFixed(1);
    ctx.fillStyle = color;
    ctx.fillRect(620, y - 13, 16, 16);
    ctx.fillStyle = "#173254";
    ctx.fillText(`${truncateLabel(point.label, 26)}  ${share}%`, 646, y);
  });
}

async function canvasToPngBlob(canvas: HTMLCanvasElement): Promise<Blob> {
  return new Promise<Blob>((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (!blob) {
        reject(new Error("Failed to generate chart image"));
        return;
      }
      resolve(blob);
    }, "image/png");
  });
}

export function downloadCsv(filename: string, columns: string[], rows: unknown[][]): void {
  const csv = serializeCsv(columns, rows);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  downloadBlob(filename, blob);
}

export async function downloadChartPng(
  filename: string,
  spec: ChartSpec,
  data: Record<string, unknown>[],
): Promise<void> {
  if (typeof document === "undefined") {
    throw new Error("File download API unavailable");
  }

  const points = toChartPoints(spec, data);
  if (!points.length) {
    throw new Error("No chart data to export");
  }

  const canvas = document.createElement("canvas");
  canvas.width = 1200;
  canvas.height = 720;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("Canvas context unavailable");
  }

  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#173254";
  ctx.font = "700 30px 'IBM Plex Sans', sans-serif";
  ctx.textAlign = "left";
  ctx.fillText(spec.title?.trim() || "Talk2Sheet Chart", 64, 66);
  ctx.font = "500 16px 'IBM Plex Sans', sans-serif";
  ctx.fillStyle = "rgba(23, 50, 84, 0.66)";
  ctx.fillText(`X: ${spec.x}  ·  Y: ${spec.y}`, 64, 92);

  if (spec.type === "line") {
    drawLineChart(ctx, points);
  } else if (spec.type === "bar") {
    drawBarChart(ctx, points);
  } else {
    drawPieChart(ctx, points);
  }

  const blob = await canvasToPngBlob(canvas);
  downloadBlob(filename, blob);
}
