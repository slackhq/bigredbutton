# 🔴 Big Red Button

> A powerful Airflow plugin for clearing recently failed task instances with only a few clicks

The Big Red Button plugin provides a convenient web interface for viewing and clearing recently failed task instances across your Airflow DAGs. Perfect for those moments when you need to quickly recover from cascading failures or retry multiple tasks at once.

## ✨ Features

### 🎯 **Bulk Clearing**
Clear all recently failed and upstream-failed task instances across multiple DAGs with a single click. No more manually clicking through individual task instances.

### 🏷️ **Tag-Based Filtering**
Filter DAGs by tags to selectively clear failures for specific groups of workflows. Great for managing different environments or teams.

### ⏰ **Time Window Selection**
Choose from multiple time windows to clear failures:
- **1 hour** - Recent failures only
- **12 hours** - Half-day failures
- **1 day** - Daily failures
- **7 days** - Weekly failures

### 📊 **Failure Overview**
View failure counts grouped by DAG before clearing. Know exactly what you're about to clear with a confirmation page showing all affected tasks.

### 🔐 **Two-Step Confirmation**
Safety first! Every clearing operation requires explicit confirmation, preventing accidental mass deletions.

### 📝 **Audit Logging**
All clearing operations are logged to Airflow's audit log with details about who cleared what and when.

## 📦 Installation

### Requirements
- Apache Airflow 2.0+ (not currently tested for Airflow 3.0+)
- Python 3.8-3.11

### Steps

1. **Copy the plugin to your Airflow plugins directory:**

```bash
# Copy the entire big_red_button folder to your Airflow plugins directory
cp -r plugins/big_red_button $AIRFLOW_HOME/plugins/
```

2. **Restart your Airflow webserver:**

```bash
# Restart the webserver to load the plugin
airflow webserver
```

3. **Access the plugin:**

Navigate to your Airflow UI and look for:
- **"Big Red Button"** in the main menu (tag-filtered view)
- **"Big Red Button Admin"** in the main menu (admin view for all DAGs)

## 🚀 Usage

### Tag-Filtered View

1. Navigate to **"Big Red Button"** in the Airflow UI
2. Select tags to filter DAGs
3. Choose a time window (1 hour, 12 hours, 1 day, or 7 days)
4. Click **"Clear Failed DAGs"**
5. Review the confirmation page showing all affected tasks
6. Click **"Confirm Clear"** to execute the clearing operation

### Admin View (All DAGs)

1. Navigate to **"Big Red Button Admin"** in the Airflow UI
2. Choose a time window (1 hour, 12 hours, 1 day, or 7 days)
3. View failure counts for **all** DAGs
4. Click **"Clear All Failed DAGs"** to proceed
5. Review and confirm the clearing operation

### Clear by Individual DAG

From either view:
1. Find the DAG with failures in the failure count table
2. Click **"Clear"** next to the specific DAG
3. Review the task-level details
4. Confirm to clear only that DAG's failures

## 🎨 Views

### Big Red Button (Tag-Filtered)
Perfect for teams managing multiple DAG groups. Filter by tags to see only the failures relevant to your team or environment.

**Route:** `/bigredbuttonbaseview`

### Big Red Button Admin
Full administrative view showing failures across all DAGs without filtering. Ideal for platform administrators who need visibility into the entire Airflow instance.

**Route:** `/bigredbuttonadminbaseview`

## 🔧 Configuration

The plugin uses the following default settings (defined in `big_red_button.py`):

```python
# Time windows for clearing failures
clear_windows = {
    "1_hour": timedelta(hours=1),
    "12_hours": timedelta(hours=12),
    "1_day": timedelta(days=1),
    "7_days": timedelta(days=7),
}

# Number of tasks to clear per database query
PAGE_SIZE = 200
```

These can be modified by editing the source file if needed.

## 🛡️ Safety Features

- **Two-step confirmation:** Always shows what will be cleared before executing
- **Audit logging:** Every clearing operation is logged with user info
- **Paginated clearing:** Large batches are cleared in chunks to avoid database timeouts
- **Read-only preview:** Confirmation page doesn't execute any changes

## 🧪 Development Setup

### Prerequisites
- Python 3.8-3.11 (Airflow 2.x is not compatible with Python 3.12+)

### Quick Start

```bash
# Setup virtual environment and install dependencies
make setup

# Run tests
make test

# Run tests with verbose output
make test-verbose

# Run tests with coverage
make test-coverage

# Clean up
make clean
```

### Manual Setup

1. Create a virtual environment:
```bash
python3 -m venv venv
```

2. Activate the virtual environment:
```bash
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

3. Install dependencies:
```bash
pip install -r requirements-dev.txt
```

### Running Tests

With the virtual environment activated:
```bash
pytest tests/ -v
```

Or run all tests:
```bash
pytest
```

To run tests with coverage:
```bash
pytest tests/ --cov=plugins/big_red_button --cov-report=term-missing
```

## 📁 Project Structure

```
bigredbutton/
├── plugins/
│   └── big_red_button/
│       ├── big_red_button.py    # Main plugin code
│       └── templates/           # Flask templates
│           ├── big_red_button.html
│           ├── big_red_button_admin.html
│           └── clear_failed.html
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # Pytest configuration
│   └── test_big_red_button.py   # Unit tests
├── requirements.txt             # Runtime dependencies
├── requirements-dev.txt         # Development dependencies
├── Makefile                     # Development commands
└── README.md
```

## 🧩 How It Works

The plugin integrates with Airflow's task clearing mechanism:

1. **Query:** Finds all failed and upstream-failed task instances within the specified time window
2. **Filter:** Optionally filters by DAG tags or specific DAG ID
3. **Group:** Groups failures by DAG for easy visualization
4. **Clear:** Uses Airflow's built-in `clear_task_instances()` function in batches
5. **Log:** Records the operation to Airflow's audit log


## 🙏 Acknowledgments

Originally developed for managing task failures at scale in production Airflow environments.

---

**Need to clear those failed tasks?** The Big Red Button is here to help! 🚀
