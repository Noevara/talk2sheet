<script setup lang="ts">
import type { UiMessages } from "../../../i18n/messages";
import type { Locale } from "../../../types";

defineProps<{
  ui: UiMessages;
  locale: Locale;
  localeOptions: Array<{ value: Locale; label: string }>;
}>();

const emit = defineEmits<{
  "update:locale": [value: Locale];
}>();
</script>

<template>
  <label class="locale-switcher">
    <span>{{ ui.languageLabel }}</span>
    <select :value="locale" @change="emit('update:locale', ($event.target as HTMLSelectElement).value as Locale)">
      <option v-for="option in localeOptions" :key="option.value" :value="option.value">
        {{ option.label }}
      </option>
    </select>
  </label>
</template>
