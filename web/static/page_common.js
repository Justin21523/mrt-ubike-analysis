/* Deprecated compatibility module.
   New code should import from `./lib/mba_core.js` and `./lib/mba_cards.js`. */

import * as Core from "./lib/mba_core.js";
import { createCard, renderCard } from "./lib/mba_cards.js";

export * from "./lib/mba_core.js";
export { createCard, renderCard } from "./lib/mba_cards.js";

// Keep a global for quick debugging in the browser console.
window.MBA = { ...Core, createCard, renderCard };

