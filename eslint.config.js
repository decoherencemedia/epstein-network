// Flat config (ESLint 9+). Scoped narrowly:
//   - site/js/**  = browser-global vanilla JS (IIFEs, no modules, no bundler)
//   - cloudflare/workers/**  = Cloudflare Workers runtime (module workers, fetch handler)
// `dist/`, `viz_data/`, and the two vendored libs in site/js are ignored outright.

import js from "@eslint/js";
import globals from "globals";

export default [
  {
    ignores: [
      "dist/",
      "viz_data/",
      "node_modules/",
      "site/js/imagesloaded.pkgd.min.js",
      "site/js/latinize.js",
    ],
  },
  js.configs.recommended,
  {
    // Repo-wide rule tweaks. "_"-prefixed names are the convention for
    // intentionally-unused params/vars (same rule as ruff's F841/ARG).
    rules: {
      "no-unused-vars": ["warn", { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }],
    },
  },
  {
    files: ["site/js/**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "script",
      globals: {
        ...globals.browser,
        // Shared helpers exposed as window globals by site/js/shared.js and nav.js.
        latinize: "readonly",
        imagesLoaded: "readonly",
      },
    },
  },
  {
    files: ["cloudflare/**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: { ...globals.worker, ...globals.serviceworker },
    },
  },
];
