<script setup lang="ts">
import { computed } from "vue";

import type { ChartSpec } from "../types";

const props = defineProps<{
  spec: ChartSpec;
  data: Record<string, unknown>[];
  noDataText: string;
}>();

type ChartPoint = {
  label: string;
  value: number;
};

const points = computed<ChartPoint[]>(() => {
  return props.data
    .map((item) => ({
      label: String(item[props.spec.x] ?? ""),
      value: Number(item[props.spec.y] ?? 0),
    }))
    .filter((item) => Number.isFinite(item.value));
});

const maxValue = computed(() => Math.max(...points.value.map((item) => item.value), 0));

const barHeights = computed(() => {
  const denominator = maxValue.value || 1;
  return points.value.map((item) => Math.max(8, (item.value / denominator) * 170));
});

const linePoints = computed(() => {
  if (!points.value.length) {
    return "";
  }
  const width = 520;
  const height = 220;
  const xGap = points.value.length === 1 ? width / 2 : width / Math.max(points.value.length - 1, 1);
  const denominator = maxValue.value || 1;
  return points.value
    .map((item, index) => {
      const x = index * xGap;
      const y = height - (item.value / denominator) * 180 - 20;
      return `${x},${y}`;
    })
    .join(" ");
});

const pieStyle = computed(() => {
  if (!points.value.length) {
    return {};
  }
  const total = points.value.reduce((sum, item) => sum + Math.max(0, item.value), 0) || 1;
  let current = 0;
  const segments = points.value.map((item, index) => {
    const slice = (Math.max(0, item.value) / total) * 100;
    const color = PIE_COLORS[index % PIE_COLORS.length];
    const start = current;
    current += slice;
    return `${color} ${start}% ${current}%`;
  });
  return {
    background: `conic-gradient(${segments.join(", ")})`,
  };
});

const pieLegend = computed(() => {
  const total = points.value.reduce((sum, item) => sum + Math.max(0, item.value), 0) || 1;
  return points.value.map((item, index) => ({
    ...item,
    share: ((Math.max(0, item.value) / total) * 100).toFixed(1),
    color: PIE_COLORS[index % PIE_COLORS.length],
  }));
});

const PIE_COLORS = ["#c85c3c", "#1d5f85", "#ef9f34", "#2f8f6b", "#7a4a8b", "#7c8d3d"];
</script>

<template>
  <div class="chart-shell">
    <div v-if="!points.length" class="chart-empty">{{ noDataText }}</div>

    <div v-else-if="spec.type === 'bar'" class="bar-chart">
      <div class="bar-chart-grid">
        <div v-for="(point, index) in points" :key="point.label" class="bar-slot">
          <div class="bar-value">{{ point.value.toLocaleString() }}</div>
          <div class="bar-track">
            <div class="bar-fill" :style="{ height: `${barHeights[index]}px` }"></div>
          </div>
          <div class="bar-label">{{ point.label }}</div>
        </div>
      </div>
    </div>

    <div v-else-if="spec.type === 'line'" class="line-chart">
      <svg viewBox="0 0 520 220" class="line-chart-svg" preserveAspectRatio="none">
        <line x1="0" y1="200" x2="520" y2="200" class="axis-line" />
        <polyline :points="linePoints" class="line-stroke" />
        <template v-for="(point, index) in points" :key="`${point.label}-${index}`">
          <circle
            :cx="points.length === 1 ? 260 : index * (520 / Math.max(points.length - 1, 1))"
            :cy="220 - (point.value / (maxValue || 1)) * 180 - 20"
            r="5"
            class="line-dot"
          />
        </template>
      </svg>
      <div class="line-labels">
        <span v-for="point in points" :key="point.label">{{ point.label }}</span>
      </div>
    </div>

    <div v-else class="pie-chart">
      <div class="pie-chart-graphic" :style="pieStyle"></div>
      <div class="pie-chart-legend">
        <div v-for="entry in pieLegend" :key="entry.label" class="legend-row">
          <span class="legend-swatch" :style="{ background: entry.color }"></span>
          <span class="legend-label">{{ entry.label }}</span>
          <span class="legend-share">{{ entry.share }}%</span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.chart-shell {
  border-radius: 20px;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(247, 241, 232, 0.9));
  border: 1px solid rgba(18, 41, 74, 0.12);
  padding: 1rem;
}

.chart-empty {
  min-height: 180px;
  display: grid;
  place-items: center;
  color: rgba(23, 50, 84, 0.68);
}

.bar-chart-grid {
  min-height: 250px;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(82px, 1fr));
  gap: 0.85rem;
  align-items: end;
}

.bar-slot {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.5rem;
}

.bar-value {
  font-size: 0.8rem;
  color: rgba(23, 50, 84, 0.8);
}

.bar-track {
  width: 100%;
  min-height: 190px;
  border-radius: 16px;
  background: linear-gradient(180deg, rgba(29, 95, 133, 0.08), rgba(29, 95, 133, 0.16));
  display: flex;
  align-items: flex-end;
  justify-content: center;
  padding: 0.35rem;
}

.bar-fill {
  width: 100%;
  border-radius: 12px 12px 8px 8px;
  background: linear-gradient(180deg, #ef9f34, #c85c3c);
}

.bar-label {
  text-align: center;
  font-size: 0.84rem;
  color: #173254;
  word-break: break-word;
}

.line-chart {
  display: grid;
  gap: 0.75rem;
}

.line-chart-svg {
  width: 100%;
  height: 240px;
}

.axis-line {
  stroke: rgba(23, 50, 84, 0.24);
  stroke-width: 2;
}

.line-stroke {
  fill: none;
  stroke: #c85c3c;
  stroke-width: 4;
  stroke-linejoin: round;
  stroke-linecap: round;
}

.line-dot {
  fill: #1d5f85;
}

.line-labels {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(72px, 1fr));
  gap: 0.5rem;
  font-size: 0.82rem;
  color: rgba(23, 50, 84, 0.72);
}

.line-labels span {
  text-align: center;
  word-break: break-word;
}

.pie-chart {
  display: flex;
  flex-wrap: wrap;
  gap: 1.25rem;
  align-items: center;
}

.pie-chart-graphic {
  width: 210px;
  height: 210px;
  border-radius: 999px;
  box-shadow: inset 0 0 0 16px rgba(255, 255, 255, 0.65);
}

.pie-chart-legend {
  flex: 1;
  min-width: 220px;
  display: grid;
  gap: 0.7rem;
}

.legend-row {
  display: grid;
  grid-template-columns: 12px 1fr auto;
  gap: 0.7rem;
  align-items: center;
}

.legend-swatch {
  width: 12px;
  height: 12px;
  border-radius: 999px;
}

.legend-label,
.legend-share {
  color: #173254;
  font-size: 0.92rem;
}
</style>
