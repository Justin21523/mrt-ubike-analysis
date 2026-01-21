/* Deprecated compatibility shim.
   New code should import from `./lib/mba_core.js` and `./lib/mba_cards.js`.

   This file intentionally avoids static `import` statements so it can be loaded
   either as a module or as a classic script (to tolerate stale cached HTML). */

(function initPageCommon() {
  const setGlobal = (core, cards) => {
    try {
      const createCard = cards?.createCard;
      const renderCard = cards?.renderCard;
      window.MBA = { ...(core || {}), createCard, renderCard };
    } catch {
      // ignore
    }
  };

  // `import()` works in modern browsers even from classic scripts.
  Promise.all([import("./lib/mba_core.js"), import("./lib/mba_cards.js")])
    .then(([core, cards]) => setGlobal(core, cards))
    .catch((err) => {
      console.error("Failed to init page_common.js", err);
      setGlobal(null, null);
    });
})();
