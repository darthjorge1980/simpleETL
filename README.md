# SimpleETL ⚡

A visual, node-based ETL (Extract–Transform–Load) tool with a drag-and-drop interface. Build data pipelines visually, execute them against large datasets using **Polars** (streaming/lazy), and export standalone Python scripts for production.

![Architecture](https://img.shields.io/badge/Backend-Python_FastAPI-green) ![Frontend](https://img.shields.io/badge/Frontend-React_TypeScript-blue) ![Engine](https://img.shields.io/badge/Engine-Polars-orange)

## Features

- **Visual Node Editor** – Drag-and-drop pipeline builder (React Flow / xyflow)
- **Plugin Architecture** – Every connector is a plugin. Add your own with a simple Python class.
- **Large Data Support** – Uses Polars LazyFrames with streaming execution
- **Code Generation** – Export pipelines as standalone Python scripts
- **Save / Load** – Persist pipelines as JSON and reload them
- **Data Preview** – Inspect data at any point in the pipeline with the Preview Data node
- **Switch / Router** – Route rows to different outputs based on conditions
- **Change Capture (CDC)** – Detect inserts, updates, and deletes between datasets
- **Dark Theme** – Sleek dark interface

## Built-in Plugins (38)

### Sources (7)
| Plugin | Description |
|--------|-------------|
| Read CSV | Scan CSV files (streaming, 24 options) |
| Read JSON | Read JSON files |
| Read Excel | Read .xlsx files |
| Read Parquet | Scan Parquet files (streaming) |
| Read SQL | Query any SQL database (SQLAlchemy) |
| Read API | Fetch data from REST APIs |
| Row Generator | Generate test data with configurable columns |

### Transforms (27)
| Plugin | Description |
|--------|-------------|
| Filter Rows | Filter with Polars expressions |
| Select Columns | Keep specific columns |
| Rename Columns | Rename columns with a mapping |
| Join | Join two dataframes on key columns |
| Union (Stack) | Stack two datasets vertically (concat rows) |
| Lookup | Enrich rows by looking up values from a reference table |
| Aggregate | Group by and aggregate |
| Sort | Sort by columns |
| Add Column | Compute a new column with expressions |
| Drop Duplicates | Remove duplicate rows |
| Fill Null | Replace nulls (forward fill, backward, mean, value, etc.) |
| Cast Type | Change data types of columns |
| String Operations | 20+ string functions (upper, lower, trim, replace, extract, etc.) |
| Date Operations | Extract year/month/day, format, truncate, parse dates |
| Surrogate Key | Generate auto-increment key column |
| Checksum / Hash | Generate hash from one or more columns |
| Window / Rank | row_number, rank, cumsum, lag, lead, rolling functions |
| Pivot | Rotate rows into columns |
| Unpivot (Melt) | Rotate columns into rows |
| Column Split | Split a column by delimiter into multiple columns |
| Column Merge | Concatenate multiple columns into one |
| Explode (Normalize) | Expand list values into separate rows |
| If / Then / Else | Conditional column (CASE/WHEN) |
| Switch / Router | Route rows to matched/rejected outputs by condition |
| Change Capture (CDC) | Compare datasets to detect inserts, updates, deletes |
| Sample / Head / Tail | Take first N, last N, or random sample |
| Preview Data | Inspect data inline at any pipeline point |

### Destinations (4)
| Plugin | Description |
|--------|-------------|
| Write CSV | Write to CSV (16 options) |
| Write JSON | Write to JSON |
| Write Parquet | Write to Parquet (compressed) |
| Write Excel | Write to .xlsx |

## Quick Start

### 1. Backend

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### 2. Frontend

```bash
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173** in your browser.

### Production Build

```bash
cd frontend
npm run build      # Builds into backend/frontend_dist/

cd ../backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

Now the backend serves both the API and the frontend at `http://localhost:8000`.

## Creating a Plugin

Create a new `.py` file under `backend/plugins/sources/`, `backend/plugins/transforms/`, or `backend/plugins/destinations/`:

```python
from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class MyCustomReader(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="my_custom_reader",
            name="My Reader",
            category=PluginCategory.SOURCE,
            description="Reads data from my custom source",
            icon="Database",
            color="#22c55e",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(
                    name="connection",
                    label="Connection",
                    param_type="string",
                    required=True,
                ),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        # Your logic here
        df = pl.DataFrame({"col": [1, 2, 3]})
        return {"output": df.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        var = "__OUT__"
        code = f'{var} = pl.DataFrame({{"col": [1, 2, 3]}}).lazy()'
        return code, {"output": var}
```

The plugin will be **auto-discovered** on server restart.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                 Frontend (React + xyflow)                │
│   Sidebar │ Node Canvas │ Properties Panel │ Toolbar     │
└────────────────────────┬────────────────────────────────┘
                         │ REST API
┌────────────────────────▼────────────────────────────────┐
│                Backend (FastAPI + Polars)                │
│  Plugin Manager → Engine (topo-sort) → Code Generator   │
│        ▲                                                │
│  ┌─────┴──────────────────────────────────────┐         │
│  │ Plugins: sources/ transforms/ destinations/ │         │
│  └────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, TypeScript, xyflow (React Flow), Zustand, Tailwind CSS, Lucide icons |
| Backend | Python 3.11+, FastAPI, Polars (lazy/streaming), Pydantic v2 |
| Build | Vite 6, uvicorn |

## License

MIT
