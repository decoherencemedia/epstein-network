import js from "@eslint/js";
import html from "eslint-plugin-html";
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
    rules: {
      "no-unused-vars": [
        "warn",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" },
      ],

      // `null: "ignore"` keeps the `x == null` idiom (null + undefined).
      eqeqeq: ["error", "always", { null: "ignore" }],
      "no-throw-literal": "error",
      "no-return-assign": ["error", "always"],
      "no-self-compare": "error",
      "no-unmodified-loop-condition": "error",
      "no-unreachable-loop": "error",
      "no-constructor-return": "error",
      "no-promise-executor-return": "error",
      // variables: false — module-state `const`s are declared below the helpers that
      // reference them; those helpers only fire after init, so the forward refs are safe.
      "no-use-before-define": ["error", { functions: false, classes: true, variables: false }],
      radix: "error",
      // allowEmptyCatch — `catch (_) {}` is used intentionally around JSON/URL-decode
      // fallbacks where swallowing the error is the behavior we want.
      "no-empty": ["error", { allowEmptyCatch: true }],

      "no-useless-concat": "warn",
      "no-useless-return": "warn",
      "no-useless-catch": "warn",
      "no-useless-rename": "warn",
      "no-unneeded-ternary": "warn",
      "no-lonely-if": "warn",
      "default-case-last": "warn",
      "no-duplicate-imports": "error",

      "no-shadow": ["warn", { builtinGlobals: false, hoist: "functions" }],
      // allow "+" — the `+x` numeric coercion is used throughout with `!= null` guards;
      // `Number(x)` isn't clearer. `!!x` / `"" + x` are still flagged.
      "no-implicit-coercion": ["warn", { boolean: false, allow: ["+"] }],
      "no-new-wrappers": "error",
      "no-new-native-nonconstructor": "error",
      "no-sequences": "error",
    },
  },
  {
    files: ["site/js/**/*.js", "site/pages/**/*.html", "site/partials/nav.html", "site/partials/footer.html"],
    plugins: { html },
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "script",
      globals: {
        ...globals.browser,
        latinize: "readonly",
        imagesLoaded: "readonly",
        d3: "readonly",
        SiteShared: "readonly",
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
