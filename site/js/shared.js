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
    params.set("person_ids", ids.join(","));
    return "/people/?" + params.toString();
  }

  window.SiteShared = Object.freeze({
    openInNewTab: openInNewTab,
    peopleUrlForPersonIds: peopleUrlForPersonIds,
  });
})();
