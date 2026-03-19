import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const SRC_ROOT = path.resolve(__dirname, "../src");

const APP_ROOT = path.join(SRC_ROOT, "app");
const FEATURES_ROOT = path.join(SRC_ROOT, "features");
const WORKBOOK_ROOT = path.join(FEATURES_ROOT, "workbook");
const CONVERSATION_ROOT = path.join(FEATURES_ROOT, "conversation");
const SETTINGS_ROOT = path.join(FEATURES_ROOT, "settings");
const LEGACY_COMPOSABLES_ROOT = path.join(SRC_ROOT, "composables");
const APP_ENTRY = path.join(SRC_ROOT, "App.vue");

function isCodeFile(filePath) {
  return [".ts", ".tsx", ".vue"].includes(path.extname(filePath));
}

function collectFiles(root) {
  if (!fs.existsSync(root)) {
    return [];
  }

  const entries = fs.readdirSync(root, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectFiles(fullPath));
      continue;
    }
    if (entry.isFile() && isCodeFile(fullPath)) {
      files.push(fullPath);
    }
  }

  return files;
}

function toPosix(filePath) {
  return filePath.split(path.sep).join("/");
}

function isInside(targetPath, rootPath) {
  const relative = path.relative(rootPath, targetPath);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function extractSpecifiers(sourceText) {
  const specifiers = new Set();
  const pattern = /(?:import|export)\s+[\s\S]*?\sfrom\s+["']([^"']+)["']/g;
  for (const match of sourceText.matchAll(pattern)) {
    specifiers.add(match[1]);
  }
  return [...specifiers];
}

function resolveSpecifier(fromFile, specifier) {
  if (!specifier.startsWith(".")) {
    return null;
  }

  const base = path.resolve(path.dirname(fromFile), specifier);
  const candidates = [
    base,
    `${base}.ts`,
    `${base}.tsx`,
    `${base}.vue`,
    path.join(base, "index.ts"),
    path.join(base, "index.vue"),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return base;
}

function checkFeatureIsolation(rootPath, forbiddenRoots) {
  const violations = [];

  for (const filePath of collectFiles(rootPath)) {
    const sourceText = fs.readFileSync(filePath, "utf-8");
    for (const specifier of extractSpecifiers(sourceText)) {
      const resolved = resolveSpecifier(filePath, specifier);
      if (!resolved) {
        continue;
      }
      for (const forbidden of forbiddenRoots) {
        if (isInside(resolved, forbidden.root)) {
          violations.push(
            `${toPosix(path.relative(SRC_ROOT, filePath))} imports ${specifier} -> ${toPosix(path.relative(SRC_ROOT, resolved))}; ${forbidden.reason}`,
          );
        }
      }
    }
  }

  return violations;
}

function checkLegacyComposableAdapters() {
  const violations = [];

  for (const filePath of collectFiles(LEGACY_COMPOSABLES_ROOT)) {
    const sourceText = fs.readFileSync(filePath, "utf-8").trim();
    const lines = sourceText
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
    const exportOnly = lines.every((line) => line.startsWith("export "));
    if (!exportOnly) {
      violations.push(`${toPosix(path.relative(SRC_ROOT, filePath))} must stay as a feature re-export adapter`);
      continue;
    }

    for (const specifier of extractSpecifiers(sourceText)) {
      const resolved = resolveSpecifier(filePath, specifier);
      if (!resolved || !isInside(resolved, FEATURES_ROOT)) {
        violations.push(
          `${toPosix(path.relative(SRC_ROOT, filePath))} must re-export from src/features only: ${specifier}`,
        );
      }
    }
  }

  return violations;
}

function checkAppEntry() {
  const sourceText = fs.readFileSync(APP_ENTRY, "utf-8");
  const specifiers = extractSpecifiers(sourceText);
  if (specifiers.length === 1 && specifiers[0] === "./app/AppShell.vue") {
    return [];
  }
  return [
    `App.vue must remain a thin entrypoint that only imports ./app/AppShell.vue; found [${specifiers.join(", ")}]`,
  ];
}

const violations = [
  ...checkAppEntry(),
  ...checkLegacyComposableAdapters(),
  ...checkFeatureIsolation(WORKBOOK_ROOT, [
    { root: APP_ROOT, reason: "workbook feature must not depend on app shell" },
    { root: CONVERSATION_ROOT, reason: "workbook feature must not depend on conversation feature" },
  ]),
  ...checkFeatureIsolation(CONVERSATION_ROOT, [
    { root: APP_ROOT, reason: "conversation feature must not depend on app shell" },
    { root: WORKBOOK_ROOT, reason: "conversation feature must not depend on workbook feature" },
  ]),
  ...checkFeatureIsolation(SETTINGS_ROOT, [
    { root: APP_ROOT, reason: "settings feature must not depend on app shell" },
    { root: WORKBOOK_ROOT, reason: "settings feature must not depend on workbook feature" },
    { root: CONVERSATION_ROOT, reason: "settings feature must not depend on conversation feature" },
  ]),
];

if (violations.length > 0) {
  console.error("Feature boundary violations found:");
  for (const violation of violations) {
    console.error(`- ${violation}`);
  }
  process.exit(1);
}

console.log("Feature boundaries look good.");
