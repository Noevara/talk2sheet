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

export function downloadCsv(filename: string, columns: string[], rows: unknown[][]): void {
  if (typeof document === "undefined" || typeof URL === "undefined") {
    throw new Error("File download API unavailable");
  }

  const csv = serializeCsv(columns, rows);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
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
