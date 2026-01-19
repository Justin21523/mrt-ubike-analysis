# Urban Mobility Flow Analysis: MRT & YouBike

## 1. Project Overview

This project aims to analyze the relationship between **urban mobility flows** (Taiwan MRT systems and YouBike bike-sharing usage) and **urban factors**, including:

- District characteristics
- Land use and surrounding environment
- Accessibility and convenience
- Temporal patterns (commuting vs leisure)

The project integrates **transportation open data**, **geospatial data**, and **urban indicators** to uncover how different urban contexts influence mobility behavior.

This is a data-driven urban analytics project suitable for:
- Smart city research
- Urban planning analysis
- Transportation policy evaluation
- Data science and spatial analysis portfolios

---

## 2. Core Research Questions

1. How do MRT station passenger flows vary across different urban district types?
2. What is the relationship between MRT usage and nearby YouBike usage?
3. Which urban factors most strongly influence YouBike adoption?
4. How do mobility patterns differ between weekdays, weekends, and peak hours?
5. Can MRT stations be categorized into functional types based on mobility and environment features?

---

## 3. Data Sources

### 3.1 Transportation Data (TDX Platform)

- MRT station metadata (station ID, coordinates, lines)
- MRT entry/exit passenger counts (time-series)
- YouBike station locations
- YouBike borrow/return counts
- Time-based availability data

### 3.2 Urban & Spatial Data

- Administrative district boundaries (GIS)
- Land use categories (residential, commercial, mixed-use)
- Points of Interest (POI): schools, offices, shopping areas, parks
- Road network and walkability indicators
- Optional: parking facilities, bus stop density

### 3.3 Environmental & Contextual Data (Optional)

- Weather data (rainfall, temperature)
- Green space proximity
- Terrain or elevation (if applicable)

---

## 4. Analytical Framework

### 4.1 Spatial Integration

- Buffer-based analysis (e.g., 300m / 500m radius around MRT stations)
- Distance-based association between MRT stations and YouBike stations
- Aggregation of urban features within station influence zones

### 4.2 Temporal Analysis

- Hourly / daily / weekly aggregation
- Peak vs off-peak comparison
- Weekday vs weekend behavior

### 4.3 Modeling & Analysis

- Descriptive statistics
- Correlation analysis
- Regression models (linear, regularized, tree-based)
- Clustering (e.g., KMeans) for station typology
- Optional: time-series decomposition

---

## 5. Project Architecture

```

urban-mobility-analysis/
│
├─ data/
│  ├─ raw/                # Original downloaded data
│  ├─ processed/          # Cleaned & merged datasets
│  └─ external/           # GIS, POI, land-use data
│
├─ src/
│  ├─ ingestion/          # API clients and data fetching
│  ├─ preprocessing/     # Cleaning, transformation, feature engineering
│  ├─ analysis/           # Statistical and ML analysis
│  ├─ spatial/            # GIS and spatial computation utilities
│  └─ visualization/     # Charts, maps, dashboards
│
├─ notebooks/             # Exploratory analysis (Jupyter)
│
├─ configs/
│  └─ config.yaml         # API keys, parameters
│
├─ outputs/
│  ├─ figures/
│  ├─ tables/
│  └─ reports/
│
├─ PROJECT_DESCRIPTION.md
├─ README.md
└─ requirements.txt

```

---

## 6. Technology Stack

- Python 3.10+
- pandas, numpy
- geopandas, shapely
- matplotlib / seaborn / plotly
- scikit-learn
- requests / httpx
- Jupyter Notebook

---

## 7. Expected Deliverables

- Clean, reproducible data pipeline
- Station-level integrated dataset
- Analytical results with visualizations
- MRT station typology classification
- Insightful conclusions about urban mobility patterns

---

## 8. Design Principles

- Reproducibility first
- Clear separation of ingestion, processing, and analysis
- No hard-coded values
- Config-driven architecture
- Code readability over premature optimization

---

## 9. Future Extensions

- Policy simulation (e.g., adding YouBike stations)
- Comparison across multiple cities
- Integration with census or socioeconomic data
- Interactive web dashboard

```

