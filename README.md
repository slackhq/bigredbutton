# Big Red Button

> An Airflow plugin for clearing recently failed task instances with only a few clicks

The Big Red Button plugin provides a web interface and REST API for viewing and clearing recently failed task instances across your Airflow DAGs. Perfect for those moments when you need to quickly recover from cascading failures or retry multiple tasks at once.

## Features

- **Bulk Clearing** — Clear all recently failed and upstream-failed task instances across multiple DAGs with a single click
- **Tag-Based Filtering** — Filter DAGs by tags to selectively clear failures for specific groups of workflows
- **Time Window Selection** — Choose from 1 hour, 12 hours, 1 day, or 7 days
- **Two-Step Confirmation** — Every clearing operation requires explicit confirmation
- **Audit Logging** — All clearing operations are logged to Airflow's audit log
- **REST API** — Programmatic access for automation and integrations
- **RBAC Integration** — Separate user and admin views controlled by Airflow's role-based access

## Requirements

- Apache Airflow 3.1+
- Python 3.9+
- Node.js 18+ (for building the UI)

## Installation

### Airflow 3.1+

1. **Download the latest release:**

```bash
# Download from GitHub Releases
curl -L https://github.com/slackhq/bigredbutton/releases/latest/download/big_red_button-<version>.tar.gz -o big_red_button.tar.gz
```

Or visit the [Releases page](https://github.com/slackhq/bigredbutton/releases) and download the latest `.tar.gz`.

2. **Extract to your Airflow plugins directory:**

```bash
tar -xzf big_red_button.tar.gz -C $AIRFLOW_HOME/plugins/
```

3. **Restart your Airflow webserver:**

```bash
airflow webserver
```

4. **Access the plugin:**

Navigate to your Airflow UI and look for:
- **"Big Red Button"** in the Admin menu (tag-filtered view)
- **"Big Red Button: Admin"** in the Admin menu (unrestricted view)

### Airflow 2 (legacy)

For Airflow 2.x installations, use the `2.10.2` tag:

```bash
curl -sL "https://github.com/slackhq/bigredbutton/archive/refs/tags/2.10.2.tar.gz" \
  | tar -xz --strip-components=2 -C $AIRFLOW_HOME/plugins "bigredbutton-2.10.2/plugins/big_red_button"
```

## Usage

### User View (Tag-Filtered)

1. Navigate to **"Big Red Button"** in the Airflow UI
2. Select one or more tags (required)
3. Choose a time window
4. View failure counts grouped by DAG
5. Click **"Clear"** on a specific DAG or **"Clear All Failed DAGs"**
6. Confirm the operation

**Route:** `/big-red-button`

### Admin View

1. Navigate to **"Big Red Button: Admin"** in the Airflow UI
2. Choose a time window
3. View all failures across all DAGs (tags are optional filters)
4. Clear individual DAGs or all failures at once

**Route:** `/big-red-button-admin`

Access to the admin view should be restricted via Airflow's RBAC configuration.

## REST API

All endpoints are mounted under `/big-red-button`.

### User Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/failures?clear_window=1_hour&tags=my_tag` | Get failures (tags required) |
| GET | `/api/tags` | List all DAG tags |
| POST | `/api/clear` | Clear failures (tags or dag_id required) |

### Admin Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/admin/failures?clear_window=1_hour` | Get all failures (tags optional) |
| POST | `/api/admin/clear` | Clear failures (no restrictions) |

### Request/Response Examples

**Get failures:**
```bash
curl "http://localhost:8080/big-red-button/api/admin/failures?clear_window=1_hour"
```

**Clear failures:**
```bash
curl -X POST "http://localhost:8080/big-red-button/api/clear" \
  -H "Content-Type: application/json" \
  -d '{"clear_window": "1_hour", "tags_filter": ["my_team"]}'
```

## Configuration

The plugin uses the following default settings (defined in `big_red_button.py`):

```python
clear_windows = {
    "1_hour": timedelta(hours=1),
    "12_hours": timedelta(hours=12),
    "1_day": timedelta(days=1),
    "7_days": timedelta(days=7),
}

PAGE_SIZE = 200  # Tasks cleared per batch
```

## Development Setup

### Prerequisites
- Python 3.9+
- Node.js 18+

### Quick Start

```bash
# Python setup
make setup

# UI setup and build
make ui-setup
make ui-build

# Run tests
make test

# Start UI dev server (hot reload, proxies to Airflow)
make ui-dev
```

### Available Make Targets

| Target | Description |
|--------|-------------|
| `make setup` | Create venv and install Python dependencies |
| `make test` | Run tests |
| `make test-verbose` | Run tests with verbose output |
| `make test-coverage` | Run tests with coverage report |
| `make lint` | Run ruff linter |
| `make lint-fix` | Auto-fix lint issues |
| `make format` | Format code with ruff |
| `make ui-setup` | Install UI dependencies |
| `make ui-build` | Build UI bundle for production |
| `make ui-dev` | Start UI dev server with hot reload |
| `make clean` | Remove venv, node_modules, and build artifacts |

## Project Structure

```
bigredbutton/
├── plugins/
│   └── big_red_button/
│       ├── big_red_button.py    # Core backend logic and plugin registration
│       ├── api.py               # FastAPI REST API
│       ├── static/              # Built UI bundle (generated by ui-build)
│       └── ui/                  # React frontend source
│           ├── src/
│           │   ├── main.tsx
│           │   ├── App.tsx
│           │   ├── api.ts
│           │   └── styles.css
│           ├── package.json
│           └── vite.config.ts
├── tests/
│   ├── conftest.py
│   └── test_big_red_button.py
├── requirements.txt
├── requirements-dev.txt
└── Makefile
```

## How It Works

1. **Query:** Finds all failed and upstream-failed task instances within the specified time window using `TaskInstance.last_heartbeat_at`
2. **Filter:** Optionally filters by DAG tags or specific DAG ID
3. **Group:** Groups failures by DAG for visualization
4. **Clear:** Uses Airflow's built-in `clear_task_instances()` in batches
5. **Log:** Records the operation to Airflow's audit log
