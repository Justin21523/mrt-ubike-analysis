# `data/`

This repository uses a simple local “lakehouse” layout (Bronze/Silver/Gold) to keep ingestion reproducible and transformations traceable:

- `data/bronze/`: raw extracts (no field/value changes; store request metadata)
- `data/silver/`: cleaned & standardized tables (types, timezone, stable keys)
- `data/gold/`: analysis-ready feature sets

`data/` is gitignored by default to avoid committing large files or sensitive artifacts, while keeping folder structure via `.gitkeep`.
