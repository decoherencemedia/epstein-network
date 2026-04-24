/**
 * Cloudflare Worker for /search/people/* and /search/document/*:
 *
 * People paths (/search/people/*):
 *   - Static pre-generated pages exist for many paths. If the origin returns 404, fall back to
 *     serving /search/index.html so the client-side shell runs at the original URL.
 *   - If ?photo= is present, inject og:image / twitter:image.
 *
 * Document paths (/search/document/*):
 *   - No static pages exist; always serves /search/index.html.
 *   - If ?photo= is present, use it as og:image. Otherwise fetch the first result from the
 *     photos API for the document prefix and use that image. Fails silently on any API error.
 *
 * Deploy: cd epstein-web/cloudflare/workers/og-photo-inject && npx wrangler deploy
 * Routes: epstein.photos/search/people/*
 *         epstein.photos/search/document/*
 */

const DEFAULT_SPACES_CDN = "https://epstein.sfo3.cdn.digitaloceanspaces.com";
const API_BASE = "https://api.epstein.photos";

/** @param {string} s */
function escapeHtmlAttr(s) {
  return String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

/**
 * @param {string} photoRaw
 * @param {string} cdnBase
 */
function cdnImagesAbsoluteUrl(photoRaw, cdnBase) {
  const trimmed = String(photoRaw || "").trim();
  if (!trimmed) return null;
  const base = trimmed.split("/").pop() || trimmed;
  const webpName = base.replace(/\.jpg$/i, ".webp");
  const enc = encodeURIComponent(webpName);
  return `${cdnBase.replace(/\/$/, "")}/images/${enc}`;
}

const PEOPLE_SEARCH_PATH = /^\/search\/people\/[^/]+\/?/;
const DOCUMENT_SEARCH_PATH = /^\/search\/document\/([^/]+)\/?/;

/**
 * @param {string} html
 * @param {string} imageUrl
 */
function injectOgImageMeta(html, imageUrl) {
  const c = escapeHtmlAttr(imageUrl);
  const tags = [
    ["property", "og:image", c],
    ["property", "og:image:secure_url", c],
    ["name", "twitter:image", c],
  ];
  let out = html;
  for (const [attr, key, val] of tags) {
    const prop = attr;
    const re = new RegExp(`<meta\\s+${prop}="${key}"\\s+content="[^"]*"\\s*/?>`, "i");
    const replacement = `<meta ${prop}="${key}" content="${val}" />`;
    if (re.test(out)) {
      out = out.replace(re, replacement);
    } else {
      out = out.replace(/<\/head>/i, `  <meta ${prop}="${key}" content="${val}" />\n</head>`);
    }
  }
  const cardRe = /<meta\s+name="twitter:card"\s+content="[^"]*"\s*\/?>/i;
  const cardRepl = '<meta name="twitter:card" content="summary_large_image" />';
  if (cardRe.test(out)) {
    out = out.replace(cardRe, cardRepl);
  } else {
    out = out.replace(/<\/head>/i, `  ${cardRepl}\n</head>`);
  }
  return out;
}

/**
 * @param {Request} request
 */
function searchShellUrl(request) {
  const u = new URL(request.url);
  u.pathname = "/search/index.html";
  u.search = "";
  return u.toString();
}

/**
 * Fetch the first photo filename for a document prefix from the API.
 * Returns null on any error or empty results.
 * @param {string} docId  — already URL-decoded path segment
 */
async function fetchFirstDocumentPhoto(docId) {
  const docPrefix = String(docId || "")
    .trim()
    .toUpperCase();
  if (!docPrefix) return null;
  try {
    const apiUrl =
      API_BASE +
      "/photos?" +
      new URLSearchParams({ document_prefix: docPrefix, offset: "0", limit: "1" }).toString();
    const res = await fetch(apiUrl, { headers: { Accept: "application/json" } });
    if (!res.ok) return null;
    const json = await res.json();
    const first = Array.isArray(json.data) && json.data[0];
    if (!first) return null;
    const image = String(first.image || first.image_name || first.filename || "").trim();
    return image || null;
  } catch (_) {
    return null;
  }
}

/**
 * @param {Response} res
 * @param {string} html
 */
function htmlResponse(res, html) {
  const headers = new Headers(res.headers);
  headers.set("content-type", "text/html; charset=utf-8");
  headers.delete("content-length");
  return new Response(html, {
    status: res.status,
    statusText: res.statusText,
    headers,
  });
}

export default {
  /**
   * @param {Request} request
   * @param {{ SPACES_CDN_BASE?: string }} env
   * @param {ExecutionContext} _ctx
   */
  async fetch(request, env, _ctx) {
    if (request.method !== "GET") {
      return fetch(request);
    }

    const url = new URL(request.url);
    const cdnBase = env.SPACES_CDN_BASE || DEFAULT_SPACES_CDN;

    // ── Document search ──────────────────────────────────────────────────────
    // No static pages exist; always serve the shell. Look up the first API
    // result for og:image unless ?photo= is explicitly provided.
    const docMatch = url.pathname.match(DOCUMENT_SEARCH_PATH);
    if (docMatch) {
      const explicitPhoto = url.searchParams.get("photo");
      let docId = docMatch[1];
      try {
        docId = decodeURIComponent(docId);
      } catch (_) {}

      // Fetch shell and API result in parallel.
      const shellPromise = fetch(searchShellUrl(request), {
        method: "GET",
        headers: request.headers,
        redirect: "follow",
      });
      const photoPromise = explicitPhoto ? Promise.resolve(explicitPhoto) : fetchFirstDocumentPhoto(docId);

      const [shellRes, photoFilename] = await Promise.all([shellPromise, photoPromise]);
      if (!shellRes.ok) return shellRes;

      let html = await shellRes.text();
      const imageUrl = photoFilename ? cdnImagesAbsoluteUrl(photoFilename, cdnBase) : null;
      if (imageUrl) html = injectOgImageMeta(html, imageUrl);
      return new Response(html, {
        status: 200,
        statusText: "OK",
        headers: (() => {
          const h = new Headers(shellRes.headers);
          h.set("content-type", "text/html; charset=utf-8");
          h.delete("content-length");
          return h;
        })(),
      });
    }

    // ── People search ────────────────────────────────────────────────────────
    // Static pages exist for many paths; fall back to shell on 404.
    // Inject og:image when ?photo= is present.
    if (!PEOPLE_SEARCH_PATH.test(url.pathname)) {
      return fetch(request);
    }

    const photo = url.searchParams.get("photo");
    const imageUrl = photo ? cdnImagesAbsoluteUrl(photo, cdnBase) : null;

    const upstreamUrl = new URL(request.url);
    if (photo) upstreamUrl.search = "";
    const upstreamReq = new Request(upstreamUrl.toString(), {
      method: request.method,
      headers: request.headers,
      redirect: "follow",
    });

    const res = await fetch(upstreamReq);

    if (res.status === 404) {
      const shellRes = await fetch(searchShellUrl(request), {
        method: "GET",
        headers: request.headers,
        redirect: "follow",
      });
      if (!shellRes.ok) return shellRes;
      let html = await shellRes.text();
      if (photo && imageUrl) html = injectOgImageMeta(html, imageUrl);
      return new Response(html, {
        status: 200,
        statusText: "OK",
        headers: (() => {
          const h = new Headers(shellRes.headers);
          h.set("content-type", "text/html; charset=utf-8");
          h.delete("content-length");
          return h;
        })(),
      });
    }

    if (photo && imageUrl) {
      if (res.status !== 200) return res;
      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("text/html")) return res;
      const html = await res.text();
      return htmlResponse(res, injectOgImageMeta(html, imageUrl));
    }

    return res;
  },
};
