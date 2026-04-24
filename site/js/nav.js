(function () {
  "use strict";

  var mq = window.matchMedia("(max-width: 768px)");

  /**
   * Pick the longest-prefix `.site-nav-link` whose pathname matches
   * location.pathname and mark it active. "/" only activates on an exact match
   * so that e.g. "/search/people/<id>/" activates the "/search/" link, not "/".
   */
  function markActiveNavLink(menu) {
    var links = menu.querySelectorAll("a.site-nav-link");
    var here = window.location.pathname || "/";
    var best = null;
    var bestLen = -1;
    for (var i = 0; i < links.length; i++) {
      var a = links[i];
      var p = a.pathname || "/";
      var matches = p === "/" ? here === "/" : here === p || here.indexOf(p) === 0;
      if (matches && p.length > bestLen) {
        best = a;
        bestLen = p.length;
      }
    }
    if (best) {
      best.classList.add("site-nav-active");
      best.setAttribute("aria-current", "page");
    }
  }

  function init() {
    var header = document.querySelector(".site-header");
    if (!header) return;
    var toggle = header.querySelector(".site-nav-toggle");
    var menu = header.querySelector("#site-nav-menu");
    var backdrop = header.querySelector(".site-nav-backdrop");
    if (!toggle || !menu) return;

    markActiveNavLink(menu);

    function isMobile() {
      return mq.matches;
    }

    function close() {
      header.classList.remove("site-nav-open");
      toggle.setAttribute("aria-expanded", "false");
      toggle.setAttribute("aria-label", "Open menu");
      document.body.classList.remove("site-nav-open");
    }

    function open() {
      if (!isMobile()) return;
      header.classList.add("site-nav-open");
      toggle.setAttribute("aria-expanded", "true");
      toggle.setAttribute("aria-label", "Close menu");
      document.body.classList.add("site-nav-open");
    }

    function toggleMenu() {
      if (!isMobile()) return;
      if (header.classList.contains("site-nav-open")) close();
      else open();
    }

    toggle.addEventListener("click", function (e) {
      e.preventDefault();
      toggleMenu();
    });

    if (backdrop) {
      backdrop.addEventListener("click", function () {
        close();
      });
    }

    menu.querySelectorAll("a").forEach(function (a) {
      a.addEventListener("click", function () {
        close();
      });
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && header.classList.contains("site-nav-open")) {
        e.preventDefault();
        close();
        toggle.focus();
      }
    });

    function onViewportChange() {
      if (!isMobile()) close();
    }

    mq.addEventListener("change", onViewportChange);
    window.addEventListener("resize", onViewportChange);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
