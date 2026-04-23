(function () {
  "use strict";

  // Bump the version suffix when the disclaimer copy materially changes so
  // previously-accepted visitors are re-prompted with the new wording. The
  // markup in site/partials/disclaimer.html is the source of truth for copy.
  var STORAGE_KEY = "agreed_to_disclaimer_v1";
  var STORAGE_VALUE = "true";

  function hasAccepted() {
    try {
      return window.localStorage.getItem(STORAGE_KEY) === STORAGE_VALUE;
    } catch (_) {
      // Safari private mode / cookies-blocked / storage-disabled environments throw on access.
      return false;
    }
  }

  function persistAcceptance() {
    try {
      window.localStorage.setItem(STORAGE_KEY, STORAGE_VALUE);
    } catch (_) {
      // If persistence fails, accept for the current session anyway — user will re-see the
      // modal on the next page load, which is an acceptable fallback for locked-down storage.
    }
  }

  // Toggle `inert` + `aria-hidden` on every top-level sibling of the modal so
  // keyboard focus and assistive-tech cursors can't escape into the page behind
  // while the gate is up. `inert` is supported in all evergreen browsers (Chrome
  // 102+, Safari 15.5+, Firefox 112+); `aria-hidden` covers the rest.
  function setSiblingsInert(modal, on) {
    var siblings = document.body.children;
    for (var i = 0; i < siblings.length; i++) {
      var el = siblings[i];
      if (el === modal) continue;
      if (on) {
        el.setAttribute("aria-hidden", "true");
        el.setAttribute("inert", "");
      } else {
        el.removeAttribute("aria-hidden");
        el.removeAttribute("inert");
      }
    }
  }

  function init() {
    if (hasAccepted()) return;
    var modal = document.getElementById("disclaimer-modal");
    var acceptBtn = document.getElementById("disclaimer-accept");
    if (!modal || !acceptBtn) return;

    var previouslyFocused = document.activeElement;
    modal.hidden = false;
    document.body.classList.add("disclaimer-open");
    setSiblingsInert(modal, true);
    acceptBtn.focus();

    // The accept button is the only focusable element inside the modal, so both
    // Tab and Shift+Tab must cycle back to it — otherwise focus escapes into the
    // inert background and the user has nowhere visible to land.
    modal.addEventListener("keydown", function (e) {
      if (e.key === "Tab") {
        e.preventDefault();
        acceptBtn.focus();
      }
    });

    acceptBtn.addEventListener("click", function () {
      persistAcceptance();
      close();
    });

    // Cross-tab sync: if the user accepts in another tab, close the modal here
    // too. Same-origin `storage` events only fire in *other* tabs, so no loop.
    window.addEventListener("storage", function (e) {
      if (e.key === STORAGE_KEY && e.newValue === STORAGE_VALUE && !modal.hidden) {
        close();
      }
    });

    function close() {
      modal.hidden = true;
      document.body.classList.remove("disclaimer-open");
      setSiblingsInert(modal, false);
      if (previouslyFocused && typeof previouslyFocused.focus === "function") {
        try {
          previouslyFocused.focus();
        } catch (_) {
          // Previously-focused element may have been removed from the DOM; fine to drop focus.
        }
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
