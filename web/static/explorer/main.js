import { runExplorer } from "../app.js";

runExplorer().catch((err) => {
  console.error(err);
  alert(`Failed to load app: ${err.message}`);
});

