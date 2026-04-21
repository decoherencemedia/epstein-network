/**
 * Cloudflare Worker for /search/people/*:
 *
 * 1) If the origin has no static page for the path (404), respond with /search/index.html
 *    so the client-side search shell + API run (same URL in the address bar).
 *
 * 2) If ?photo= is present, set og:image / twitter:image on the HTML (static page or shell),
 *    matching site/js/shared.js Spaces URL rules.
 *
 * Subrequests for (2) strip the query string so the origin returns normal HTML; crawlers see
 * injected tags on the first response.
 *
 * Deploy: cd epstein-web/cloudflare/workers/og-photo-inject && npx wrangler deploy
 * Route: epstein.photos/search/people/*
 */

const DEFAULT_SPACES_CDN = "https://epstein.sfo3.cdn.digitaloceanspaces.com";

/** @param {string} s */
function escapeHtmlAttr(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;");
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
    const re = new RegExp(
      `<meta\\s+${prop}="${key}"\\s+content="[^"]*"\\s*/?>`,
      "i"
    );
    const replacement = `<meta ${prop}="${key}" content="${val}" />`;
    if (re.test(out)) {
      out = out.replace(re, replacement);
    } else {
      out = out.replace(
        /<\/head>/i,
        `  <meta ${prop}="${key}" content="${val}" />\n</head>`
      );
    }
  }
  const cardRe =
    /<meta\s+name="twitter:card"\s+content="[^"]*"\s*\/?>/i;
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
 * @param {Request} request
 */
async function fetchSearchShell(request) {
  return fetch(searchShellUrl(request), {
    method: "GET",
    headers: request.headers,
    redirect: "follow",
  });
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
    if (!PEOPLE_SEARCH_PATH.test(url.pathname)) {
      return fetch(request);
    }

    const photo = url.searchParams.get("photo");
    const cdnBase = env.SPACES_CDN_BASE || DEFAULT_SPACES_CDN;
    const imageUrl = photo ? cdnImagesAbsoluteUrl(photo, cdnBase) : null;

    const upstreamUrl = new URL(request.url);
    if (photo) {
      upstreamUrl.search = "";
    }
    const upstreamReq = new Request(upstreamUrl.toString(), {
      method: request.method,
      headers: request.headers,
      redirect: "follow",
    });

    const res = await fetch(upstreamReq);

    if (res.status === 404) {
      const shellRes = await fetchSearchShell(request);
      if (!shellRes.ok) {
        return shellRes;
      }
      let html = await shellRes.text();
      if (photo && imageUrl) {
        html = injectOgImageMeta(html, imageUrl);
      }
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
      if (res.status !== 200) {
        return res;
      }
      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("text/html")) {
        return res;
      }
      const html = await res.text();
      const outHtml = injectOgImageMeta(html, imageUrl);
      return htmlResponse(res, outHtml);
    }

    return res;
  },
};
