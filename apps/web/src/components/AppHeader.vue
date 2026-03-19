<script setup lang="ts">
import type { UiMessages } from "../i18n/messages";
import type { UploadedFileResponse } from "../types";

defineProps<{
  ui: UiMessages;
  workbook: UploadedFileResponse | null;
}>();
</script>

<template>
  <header class="hero">
    <div class="hero-copy">
      <div class="hero-title-row">
        <div>
          <p class="eyebrow">{{ ui.eyebrow }}</p>
          <h1>{{ ui.brand }}</h1>
        </div>

        <div v-if="workbook" class="hero-dataset">
          <span class="hero-dataset-type">{{ workbook.file_type }}</span>
          <strong>{{ workbook.file_name }}</strong>
        </div>
      </div>
      <p class="hero-text">{{ ui.tagline }}</p>
    </div>

    <div class="hero-controls">
      <slot name="controls"></slot>
    </div>
  </header>
</template>
