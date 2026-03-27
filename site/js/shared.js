(function () {
  "use strict";

  function openInNewTab(url) {
    const s = String(url || "").trim();
    if (!s) return;
    window.open(s, "_blank", "noopener,noreferrer");
  }

  function peopleUrlForPersonIds(personIds) {
    const ids = Array.isArray(personIds)
      ? personIds.map(function (x) { return String(x || "").trim(); }).filter(Boolean)
      : [];
    if (!ids.length) return null;
    const params = new URLSearchParams();
    params.set("tab", "people");
    params.set("person_ids", ids.join(","));
    return "/people/?" + params.toString();
  }

  /** Same order as Search page chips: first selected = index 0, … */
  var personColorPalette = Object.freeze([
    "#00bd00", "#ff42cc", "#ff6300", "#00abff", "#fcee00", "#00e4ca", "#a989ff", "#ff8795", "#7bff83", "#f8a5ff",
  ]);

  function hexToRgb(hex) {
    var m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(String(hex).trim());
    if (!m) return null;
    return { r: parseInt(m[1], 16), g: parseInt(m[2], 16), b: parseInt(m[3], 16) };
  }

  function rgbaWithAlpha(hex, alpha) {
    var rgb = hexToRgb(hex);
    if (!rgb) return "rgba(0, 0, 0, " + alpha + ")";
    return "rgba(" + rgb.r + "," + rgb.g + "," + rgb.b + "," + alpha + ")";
  }

  window.SiteShared = Object.freeze({
    openInNewTab: openInNewTab,
    peopleUrlForPersonIds: peopleUrlForPersonIds,
    personColorPalette: personColorPalette,
    hexToRgb: hexToRgb,
    rgbaWithAlpha: rgbaWithAlpha,
  });
})();
