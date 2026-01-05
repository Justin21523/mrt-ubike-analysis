# Stage 1: Repository Layout

## Why a fixed layout?

Urban mobility projects quickly become messy (multiple sources, long pipelines, frequent iteration). A fixed layout:

- separates code, docs, and data artifacts
- makes outputs traceable (Bronze/Silver/Gold)
- reduces future friction for CI, data quality checks, and features/analytics

## Tree (high level)

```
.
├── config/                # App config (JSON)
│   └── default.json
├── data/                  # Local lakehouse (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│   └── cache/
├── docs/                  # Design notes and assumptions
├── notebooks/             # Optional exploration only
├── scripts/               # Runnable entrypoints (call into `src/`)
├── src/
│   └── metrobikeatlas/     # Python package (modular, testable)
│       ├── ingestion/
│       ├── preprocessing/
│       ├── features/
│       ├── analytics/
│       ├── api/
│       ├── schemas/
│       ├── utils/
│       └── demo/
├── web/                   # Static frontend (served by FastAPI)
│   └── static/
└── tests/
```

## Boundaries

- `src/`: reusable modules only (no hard-coded paths; notebook-independent)
- `scripts/`: runnable entrypoints that compose `src/` components
- `docs/`: design decisions and assumptions (so they are reviewable)
