#
# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional

from airflow.models import DagTag, DagModel, TaskInstance, clear_task_instances, Log
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import create_session

from sqlalchemy import tuple_
from sqlalchemy.orm import Session

ONE_HOUR = "1_hour"
PAGE_SIZE = 200

clear_windows = {
    ONE_HOUR: timedelta(hours=1),
    "12_hours": timedelta(hours=12),
    "1_day": timedelta(days=1),
    "7_days": timedelta(days=7),
}


def handle_clearing(session: Session, recent_failures: List[TaskInstance]) -> None:
    """Clear failed task instances in batches and log the operation.

    Args:
        session: SQLAlchemy database session
        recent_failures: List of TaskInstance objects to clear
    """
    log_clearing(len(recent_failures))

    index = 0
    while index < len(recent_failures):
        page_to_clear = recent_failures[index : index + PAGE_SIZE]
        clear_task_instances(page_to_clear, session)
        index = index + PAGE_SIZE


def log_clearing(
    rows_cleared: int,
    clear_window: Optional[str] = None,
    dag_id: Optional[str] = None,
    tags_filter: Optional[List[str]] = None,
    user: str = "anonymous",
    user_display: str = "",
) -> None:
    """Log a big red button clearing event to the Airflow logs table.

    Args:
        rows_cleared: Number of task instances that were cleared
        clear_window: Time window used for filtering (e.g., '1_hour')
        dag_id: Optional DAG ID that was cleared
        tags_filter: Optional list of tags used for filtering
        user: Username performing the clearing
        user_display: Display name of the user
    """
    message = [f"rows_cleared: {rows_cleared}"]
    if clear_window:
        message.append(f"clear_window: {clear_window}")
    if dag_id:
        message.append(f"dag_id: {dag_id}")
    elif tags_filter:
        message.append(f"tags_filter: {tags_filter}")

    with create_session() as session:
        log_entry = Log(
            event="big_red_button",
            extra=",".join(message),
            owner=user,
            owner_display_name=user_display,
        )
        session.add(log_entry)
        session.commit()


def get_recent_failures_paged(
    session: Session, time_cutoff: datetime, dag_ids: Optional[List[str]] = None
) -> Generator[List[TaskInstance], None, None]:
    """Query for failed and upstream_failed task instances since a given time.

    Yields results in pages to avoid loading too much data at once.
    Uses TaskInstance.last_heartbeat_at to identify recent failures, then
    collects all failed/upstream_failed tasks from the same DAG runs.

    Args:
        session: SQLAlchemy database session
        time_cutoff: Datetime threshold - only return failures after this time
        dag_ids: Optional list of DAG IDs to filter by

    Yields:
        Lists of TaskInstance objects in batches of PAGE_SIZE
    """
    failed_runs_query = (
        session.query(TaskInstance.dag_id, TaskInstance.run_id)
        .distinct()
        .filter(TaskInstance.state == "failed")
        .filter(TaskInstance.last_heartbeat_at > time_cutoff)
    )

    if dag_ids:
        failed_runs_query = failed_runs_query.filter(TaskInstance.dag_id.in_(dag_ids))

    failed_runs = failed_runs_query.all()

    if not failed_runs:
        return

    for i in range(0, len(failed_runs), PAGE_SIZE):
        batch_runs = failed_runs[i : i + PAGE_SIZE]

        batch_failures = (
            session.query(TaskInstance)
            .filter(TaskInstance.state.in_(["failed", "upstream_failed"]))
            .filter(tuple_(TaskInstance.dag_id, TaskInstance.run_id).in_(batch_runs))
            .all()
        )

        if batch_failures:
            yield batch_failures


def get_recent_failures(
    session: Session, time_cutoff: datetime, dag_ids: Optional[List[str]] = None
) -> List[TaskInstance]:
    """Query for failed and upstream_failed task instances since a given time.

    Optimized to use IN clause with tuple comparison instead of subquery join.
    Returns all results by paging through them in batches to avoid loading
    too much data at once.

    Args:
        session: SQLAlchemy database session
        time_cutoff: Datetime threshold - only return failures after this time
        dag_ids: Optional list of DAG IDs to filter by

    Returns:
        List of TaskInstance objects matching the failure criteria (both failed and upstream_failed),
        sorted by dag_id, run_id, and task_id
    """
    all_failures = []
    for batch in get_recent_failures_paged(session, time_cutoff, dag_ids):
        all_failures.extend(batch)

    # Sort the final combined list to ensure consistent ordering
    all_failures.sort(
        key=lambda ti: (ti.dag_id, ti.run_id, ti.task_id, ti.map_index or -1)
    )
    return all_failures


def group_failures_by_dag(
    failures: List[TaskInstance],
) -> Dict[str, List[TaskInstance]]:
    """Group task instances by DAG ID.

    Args:
        failures: List of TaskInstance objects

    Returns:
        Dict mapping dag_id to list of TaskInstance objects
    """
    failures_by_dag = {}
    for failure in failures:
        dag_id = failure.dag_id
        if dag_id in failures_by_dag:
            failures_by_dag[dag_id].append(failure)
        else:
            failures_by_dag[dag_id] = [failure]
    return failures_by_dag


def get_dag_ids(session: Session, tags_filter: List[str]) -> List[str]:
    """Get DAG IDs for active, non-paused DAGs matching the specified tags.

    Args:
        session: SQLAlchemy database session
        tags_filter: List of tag names to filter DAGs by

    Returns:
        List of DAG IDs that match the tag filter criteria
    """
    dag_ids = (
        session.query(DagModel.dag_id)
        .filter(~DagModel.is_stale, ~DagModel.is_paused)
        .filter(DagModel.tags.any(DagTag.name.in_(tags_filter)))
        .order_by(DagModel.dag_id)
        .all()
    )

    return [dag_id for (dag_id,) in dag_ids]


def get_dag_tags(
    session: Session, tags_filter: Optional[List[str]]
) -> List[Dict[str, Any]]:
    """Get all distinct DAG tags with selection status.

    Args:
        session: SQLAlchemy database session
        tags_filter: List of currently selected tag names

    Returns:
        List of dicts containing tag name and whether it's currently selected
    """
    dag_tags = session.query(DagTag.name).distinct(DagTag.name).all()
    tags = [
        {
            "name": name,
            "selected": bool(tags_filter and name in tags_filter),
        }
        for (name,) in dag_tags
    ]
    return tags


def _get_fastapi_app():
    from plugins.big_red_button.api import app
    return app


class BigRedButtonPlugin(AirflowPlugin):
    """Airflow plugin that registers Big Red Button backend functionality."""

    name = "big_red_button"

    fastapi_apps = [
        {
            "app": _get_fastapi_app(),
            "name": "big_red_button",
            "url_prefix": "/big-red-button",
        }
    ]

    react_apps = [
        {
            "name": "Big Red Button",
            "url_route": "big-red-button",
            "bundle_url": "/big-red-button/static/big-red-button.js",
            "category": "Admin",
        },
        {
            "name": "Big Red Button: Admin",
            "url_route": "big-red-button-admin",
            "bundle_url": "/big-red-button/static/big-red-button.js",
            "category": "Admin",
        },
    ]
