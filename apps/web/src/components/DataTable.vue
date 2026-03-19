<script setup lang="ts">
const props = defineProps<{
  columns: string[];
  rows: unknown[][];
  emptyText: string;
  compact?: boolean;
}>();

function formatCell(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}
</script>

<template>
  <div class="data-table-shell" :class="{ 'data-table-shell-compact': compact }">
    <div v-if="!rows.length" class="data-table-empty">{{ emptyText }}</div>
    <div v-else class="data-table-scroll">
      <table
        class="data-table"
        :class="{
          'data-table-compact': compact,
          'data-table-compact-two-col': compact && props.columns.length === 2,
        }"
      >
        <thead>
          <tr>
            <th v-for="column in columns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, rowIndex) in rows" :key="rowIndex">
            <td v-for="(cell, cellIndex) in row" :key="`${rowIndex}-${cellIndex}`">
              <div class="cell-content" :title="formatCell(cell)">{{ formatCell(cell) }}</div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.data-table-shell {
  min-height: 180px;
  min-width: 0;
}

.data-table-scroll {
  max-height: 420px;
  overflow: auto;
  max-width: 100%;
  min-width: 0;
  border: 1px solid rgba(18, 41, 74, 0.1);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.92);
}

.data-table {
  width: max-content;
  border-collapse: collapse;
  min-width: 100%;
}

.data-table-compact {
  width: 100%;
  min-width: 0;
  table-layout: fixed;
}

.data-table th,
.data-table td {
  padding: 0.82rem 0.92rem;
  text-align: left;
  border-bottom: 1px solid rgba(18, 41, 74, 0.08);
  font-size: 0.94rem;
  vertical-align: top;
  overflow-wrap: anywhere;
}

.data-table-compact th,
.data-table-compact td {
  padding: 0.62rem 0.7rem;
  font-size: 0.9rem;
}

.data-table-compact th:first-child,
.data-table-compact td:first-child {
  width: auto;
}

.data-table-compact th:last-child,
.data-table-compact td:last-child {
  width: 8.5rem;
  text-align: right;
  white-space: nowrap;
}

.data-table-compact-two-col th:first-child,
.data-table-compact-two-col td:first-child {
  width: calc(100% - 8.5rem);
}

.cell-content {
  min-width: 0;
}

.data-table-compact .cell-content {
  display: block;
  overflow-wrap: anywhere;
  word-break: break-word;
}

.data-table-compact-two-col td:first-child .cell-content,
.data-table-compact-two-col th:first-child {
  max-width: 16rem;
}

.data-table-compact-two-col td:first-child .cell-content {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
  overflow: hidden;
}

.data-table th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: #f8f2e9;
  color: #173254;
  font-weight: 700;
}

.data-table tr:nth-child(even) td {
  background: rgba(247, 241, 232, 0.6);
}

.data-table-empty {
  display: grid;
  place-items: center;
  min-height: 180px;
  border: 1px dashed rgba(18, 41, 74, 0.2);
  border-radius: 18px;
  color: rgba(23, 50, 84, 0.72);
  background: rgba(255, 255, 255, 0.72);
}
</style>
