import * as Core from "./lib/mba_core.js";

async function main() {
  try {
    const [status, meta] = await Promise.all([Core.fetchJson("/status"), Core.fetchJson("/meta")]);
    Core.setHeaderBadges(status, meta);
    Core.setStatusText("Ready");
  } catch (e) {
    console.error(e);
    Core.setStatusText(`Error: ${e.message}`);
  }
}

main();

