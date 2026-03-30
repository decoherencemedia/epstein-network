(function () {
  "use strict";

  var mq = window.matchMedia("(max-width: 768px)");

  function init() {
    var header = document.querySelector(".site-header");
    if (!header) return;
    var toggle = header.querySelector(".site-nav-toggle");
    var menu = header.querySelector("#site-nav-menu");
    var backdrop = header.querySelector(".site-nav-backdrop");
    if (!toggle || !menu) return;

    function isMobile() {
      return mq.matches;
    }

    function close() {
      header.classList.remove("site-nav-open");
      toggle.setAttribute("aria-expanded", "false");
      document.body.classList.remove("site-nav-open");
    }

    function open() {
      if (!isMobile()) return;
      header.classList.add("site-nav-open");
      toggle.setAttribute("aria-expanded", "true");
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

    if (typeof mq.addEventListener === "function") {
      mq.addEventListener("change", onViewportChange);
    } else if (typeof mq.addListener === "function") {
      mq.addListener(onViewportChange);
    }
    window.addEventListener("resize", onViewportChange);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
