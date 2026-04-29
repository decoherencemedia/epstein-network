(function () {
  "use strict";

  /** DigitalOcean Spaces CDN origin (no trailing slash). */
  const SPACES_CDN_BASE = "https://epstein.sfo3.cdn.digitaloceanspaces.com";

  /** Epstein API origin (no trailing slash). Change here for local dev (e.g. http://127.0.0.1:5000). */
  const API_BASE = "https://api.epstein.photos";
  //   const API_BASE = "http://127.0.0.1:5000";

  /**
   * Canonical uppercase stems before numeric suffix (document / image id search).
   * Comparison is case-insensitive via `.toUpperCase()`.
   */
  const DOCUMENT_ID_PREFIXES = Object.freeze(["EFTA", "HOUSE_OVERSIGHT_"]);

  /**
   * Uppercase query with file extensions and punctuation removed for search/API:
   * strips trailing ``.pdf`` / ``.webp`` / ``.jpg`` / ``.jpeg`` (any case), optional
   * ``.`` + letters while typing an extension, hyphens between image indices, and stray hyphens/periods.
   */
  function stripDocumentSearchTerm(s) {
    let t = String(s || "").trim();
    if (!t) return "";
    let u = t.toUpperCase();
    for (let i = 0; i < DOCUMENT_ID_PREFIXES.length; i++) {
      const P = DOCUMENT_ID_PREFIXES[i];
      if (u.startsWith(P)) {
        let rest = u.slice(P.length);
        rest = rest.replace(/\.[A-Z]*$/i, "");
        rest = rest.replace(/[-.]/g, "");
        return P + rest;
      }
    }
    u = u.replace(/\.(PDF|WEBP|JPEG|JPG)$/i, "");
    u = u.replace(/[-.]/g, "");
    return u;
  }

  function isValidPartialDocumentQueryNormalized(u) {
    const U = String(u || "").trim();
    if (!U) return true;
    for (let i = 0; i < DOCUMENT_ID_PREFIXES.length; i++) {
      const P = DOCUMENT_ID_PREFIXES[i];
      if (P.startsWith(U)) return true;
      if (U.startsWith(P)) {
        const rest = U.slice(P.length);
        if (/^\d*$/.test(rest)) return true;
        return false;
      }
    }
    return false;
  }

  /**
   * True if the trimmed query is empty, or could still become a valid id
   * ``PREFIX`` + digits for one of DOCUMENT_ID_PREFIXES (case-insensitive).
   * Allows pasted filenames (``EFTA….pdf``, ``….webp``) and image indices with hyphens before stripping.
   */
  function isValidPartialDocumentQuery(s) {
    const t = String(s || "").trim();
    if (!t) return true;
    const stripped = stripDocumentSearchTerm(t);
    if (isValidPartialDocumentQueryNormalized(stripped)) return true;
    const u = t.toUpperCase();
    for (let i = 0; i < DOCUMENT_ID_PREFIXES.length; i++) {
      const P = DOCUMENT_ID_PREFIXES[i];
      if (P.startsWith(u)) return true;
      if (u.startsWith(P)) {
        const rest = u.slice(P.length);
        if (/^\d*$/.test(rest)) return true;
        if (/^(\d+)(-\d+)*(\.[A-Z]{0,4})?$/.test(rest)) return true;
        return false;
      }
    }
    return false;
  }

  function openInNewTab(url) {
    const s = String(url || "").trim();
    if (!s) return;
    window.open(s, "_blank", "noopener,noreferrer");
  }

  /** True when served from ``file://``, ``localhost``, or ``0.0.0.0`` — used to prefer local assets. */
  function isLocalDev() {
    if (typeof window === "undefined") return false;
    const loc = window.location;
    return loc.protocol === "file:" || loc.hostname === "localhost" || loc.hostname === "0.0.0.0";
  }

  /**
   * Canonical people search URL: ``/search/people/<sorted-person_id>-…/`` (matches static generator).
   */
  function peopleUrlForPersonIds(personIds) {
    const ids = Array.isArray(personIds)
      ? personIds
          .map(function (x) {
            return String(x || "").trim();
          })
          .filter(Boolean)
      : [];
    if (!ids.length) return null;
    const sorted = ids.slice().sort();
    const slug = sorted.join("-");
    return "/search/people/" + slug + "/";
  }

  /** Same order as Search page chips: first selected = index 0, … */
  const personColorPalette = Object.freeze([
    "#00bd00",
    "#ff42cc",
    "#ff6300",
    "#00abff",
    "#fcee00",
    "#00e4ca",
    "#a989ff",
    "#ff8795",
    "#7bff83",
    "#f8a5ff",
  ]);

  function hexToRgb(hex) {
    const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(String(hex).trim());
    if (!m) return null;
    return { r: parseInt(m[1], 16), g: parseInt(m[2], 16), b: parseInt(m[3], 16) };
  }

  function rgbaWithAlpha(hex, alpha) {
    const rgb = hexToRgb(hex);
    if (!rgb) return "rgba(0, 0, 0, " + alpha + ")";
    return "rgba(" + rgb.r + "," + rgb.g + "," + rgb.b + "," + alpha + ")";
  }

  function lastPathSegment(path) {
    const s = String(path || "").trim();
    if (!s) return "";
    const i = s.lastIndexOf("/");
    return i >= 0 ? s.slice(i + 1) : s;
  }

  /**
   * CDN URL for a path under Spaces: relative path or absolute http(s) passthrough.
   * Matches prior browse ``faceImageUrl`` behavior.
   */
  function cdnAssetUrl(path) {
    if (!path) return null;
    const s = String(path).trim();
    if (/^https?:\/\//i.test(s)) return s;
    const normalized = s.replace(/^\/+/, "");
    return SPACES_CDN_BASE.replace(/\/$/, "") + "/" + normalized;
  }

  function cdnFacesUrl(pathOrBasename) {
    const base = lastPathSegment(pathOrBasename);
    if (!base) return null;
    return SPACES_CDN_BASE.replace(/\/$/, "") + "/faces/" + encodeURIComponent(base);
  }

  function cdnThumbnailWebpUrl(pathOrFilename) {
    const base = lastPathSegment(pathOrFilename);
    if (!base) return null;
    const stem = base.replace(/\.[^.]+$/, "");
    return SPACES_CDN_BASE.replace(/\/$/, "") + "/thumbnails/" + encodeURIComponent(stem) + ".webp";
  }

  function cdnImagesUrl(pathOrBasename) {
    const base = lastPathSegment(pathOrBasename);
    if (!base) return null;
    return SPACES_CDN_BASE.replace(/\/$/, "") + "/images/" + encodeURIComponent(base);
  }

  function cdnAtlasWebpUrl() {
    return SPACES_CDN_BASE.replace(/\/$/, "") + "/atlas/atlas.webp";
  }

  /** If ``person_id`` matches ``person_<digits>``, return the numeric part; else ``null``. */
  function personStubNumber(personId) {
    const m = /^person_(\d+)$/i.exec(String(personId || "").trim());
    return m ? Number(m[1]) : null;
  }

  /**
   * Display name for a person: trimmed ``name`` if present, else ``stubLabel`` for
   * ``person_<digits>`` ids (string or ``(n) => string``), else the raw ``personId``.
   * Callers own the source lookup (sheets / API / etc.); this only owns the fallback chain.
   */
  function personDisplayName(personId, name, stubLabel) {
    const s = name != null ? String(name).trim() : "";
    if (s) return s;
    const n = personStubNumber(personId);
    if (n != null) return typeof stubLabel === "function" ? stubLabel(n) : stubLabel;
    return String(personId || "");
  }

  /**
   * Lowercase + latinize for people-name search (accent-insensitive, ASCII-ish keys).
   * Normalize curly apostrophes to ASCII first. Load ``js/latinize.js`` before this file
   * (see ``site/build.sh``); if ``latinize`` is missing, falls back to lowercase only.
   */
  function normalizePersonSearchKey(s) {
    let t = String(s || "").trim();
    if (!t) return "";
    t = t.replace(/’/g, "'").replace(/‘/g, "'");
    t = t.toLowerCase();
    if (typeof window !== "undefined" && typeof window.latinize === "function") {
      t = window.latinize(t);
    }
    return t;
  }

  window.SiteShared = Object.freeze({
    openInNewTab: openInNewTab,
    isLocalDev: isLocalDev,
    peopleUrlForPersonIds: peopleUrlForPersonIds,
    personColorPalette: personColorPalette,
    hexToRgb: hexToRgb,
    rgbaWithAlpha: rgbaWithAlpha,
    SPACES_CDN_BASE: SPACES_CDN_BASE,
    API_BASE: API_BASE,
    DOCUMENT_ID_PREFIXES: DOCUMENT_ID_PREFIXES,
    stripDocumentSearchTerm: stripDocumentSearchTerm,
    isValidPartialDocumentQueryNormalized: isValidPartialDocumentQueryNormalized,
    isValidPartialDocumentQuery: isValidPartialDocumentQuery,
    lastPathSegment: lastPathSegment,
    cdnAssetUrl: cdnAssetUrl,
    cdnFacesUrl: cdnFacesUrl,
    cdnThumbnailWebpUrl: cdnThumbnailWebpUrl,
    cdnImagesUrl: cdnImagesUrl,
    cdnAtlasWebpUrl: cdnAtlasWebpUrl,
    personStubNumber: personStubNumber,
    personDisplayName: personDisplayName,
    normalizePersonSearchKey: normalizePersonSearchKey,
  });
})();
