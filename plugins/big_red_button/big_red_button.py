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
from ast import literal_eval
from datetime import datetime, timedelta, timezone

from airflow.jobs.job import Job
from airflow.models import DagTag, DagModel, TaskInstance, clear_task_instances, Log
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import create_session
from airflow.www.auth import has_access_view
from airflow.www.extensions.init_auth_manager import get_auth_manager

from flask import Blueprint
from flask_appbuilder import BaseView, expose
from flask import (
    redirect,
    request,
    session as flask_session,
    url_for,
)
from sqlalchemy import func, tuple_

ONE_HOUR = "1_hour"
FILTER_TAGS_COOKIE = "tags_filter"
# How many tasks to clear per query
PAGE_SIZE = 200

clear_windows = {
    ONE_HOUR: timedelta(hours=1),
    "12_hours": timedelta(hours=12),
    "1_day": timedelta(days=1),
    "7_days": timedelta(days=7),
}

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "big_red_button",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/big_red_button",
)


def handle_clearing(session, recent_failures):
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


def log_clearing(rows_cleared):
    """Log a big red button clearing event to the Airflow logs table.

    Args:
        rows_cleared: Number of task instances that were cleared
    """
    clear_window = request.form.get("clear_window")
    dag_id = request.form.get("dag_id")
    tags_filter = request.form.get("tags_filter")
    message = [f"rows_cleared: {rows_cleared}"]
    if clear_window:
        message.append(f"clear_window: {clear_window}")
    if dag_id:
        message.append(f"dag_id: {dag_id}")
    elif tags_filter:
        message.append(f"tags_filter: {tags_filter}")

    with create_session() as session:
        if not get_auth_manager().is_logged_in():
            user = "anonymous"
            user_display = ""
        else:
            user = get_auth_manager().get_user_name()
            user_display = get_auth_manager().get_user_display_name()

        log_entry = Log(
            event="big_red_button",
            extra=",".join(message),
            owner=user,
            owner_display_name=user_display,
        )
        session.add(log_entry)
        session.commit()


def get_recent_failures_paged(session, time_cutoff, dag_ids=None):
    """Query for failed and upstream_failed task instances since a given time.

    Yields results in pages to avoid loading too much data at once.
    Optimized to materialize the failed run pairs first, avoiding repeated
    execution of the expensive Job join for each page.

    Args:
        session: SQLAlchemy database session
        time_cutoff: Datetime threshold - only return failures after this time
        dag_ids: Optional list of DAG IDs to filter by

    Yields:
        Lists of TaskInstance objects in batches of PAGE_SIZE
    """
    # First, materialize all (dag_id, run_id) pairs with recent failures
    # We need to get the failed tasks first, since upstream_failures won't have a Job to query
    failed_runs_query = (
        session.query(TaskInstance.dag_id, TaskInstance.run_id)
        .distinct()
        .filter(TaskInstance.state == "failed")
        .filter(Job.latest_heartbeat > time_cutoff)
        .filter(Job.job_type == "LocalTaskJob")
        .join(Job, Job.id == TaskInstance.job_id)
    )

    if dag_ids:
        failed_runs_query = failed_runs_query.filter(TaskInstance.dag_id.in_(dag_ids))

    # Execute the query once to get all failed run pairs
    # This is much more efficient than re-running the Job join for each page
    failed_runs = failed_runs_query.all()

    if not failed_runs:
        return

    # Now batch the failed runs and query TaskInstances in chunks
    # This avoids having a huge IN clause and allows us to page results
    for i in range(0, len(failed_runs), PAGE_SIZE):
        batch_runs = failed_runs[i : i + PAGE_SIZE]

        # Query TaskInstances for this batch of (dag_id, run_id) pairs
        batch_failures = (
            session.query(TaskInstance)
            .filter(TaskInstance.state.in_(["failed", "upstream_failed"]))
            .filter(tuple_(TaskInstance.dag_id, TaskInstance.run_id).in_(batch_runs))
            .all()
        )

        if batch_failures:
            yield batch_failures


def get_recent_failures(session, time_cutoff, dag_ids=None):
    """Query for failed and upstream_failed task instances since a given time.

    Optimized to use IN clause with tuple comparison instead of subquery join.
    Returns all results by paging through them in batches to avoid loading
    too much data at once.

    Args:
        session: SQLAlchemy database session
        time_cutoff: Datetime threshold - only return failures after this time
        dag_ids: Optional list of DAG IDs to filter by

    Returns:
        List of TaskInstance objects matching the failure criteria (both failed and upstream_failed)
    """
    all_failures = []
    for batch in get_recent_failures_paged(session, time_cutoff, dag_ids):
        all_failures.extend(batch)
    return all_failures


def get_recent_failure_counts(session, time_cutoff, dag_ids=None):
    """Query for recent failure counts grouped by DAG ID.

    Optimized to materialize failed run pairs first, avoiding expensive
    subquery evaluation in the aggregate query.

    Args:
        session: SQLAlchemy database session
        time_cutoff: Datetime threshold - only count failures after this time
        dag_ids: Optional list of DAG IDs to filter by

    Returns:
        List of tuples (dag_id, failure_count) with counts of failed and upstream_failed
        tasks per DAG, or empty list if no failures found
    """
    # First, materialize all (dag_id, run_id) pairs with recent failures
    # We need to get the failed tasks first, since upstream_failures won't have a Job to query
    failed_runs_query = (
        session.query(TaskInstance.dag_id, TaskInstance.run_id)
        .distinct()
        .filter(TaskInstance.state == "failed")
        .filter(Job.latest_heartbeat > time_cutoff)
        .filter(Job.job_type == "LocalTaskJob")
        .join(Job, Job.id == TaskInstance.job_id)
    )

    if dag_ids:
        failed_runs_query = failed_runs_query.filter(TaskInstance.dag_id.in_(dag_ids))

    # Execute the query once to get all failed run pairs
    failed_runs = failed_runs_query.all()

    if not failed_runs:
        return []

    # Use the materialized pairs for the count query
    recent_failures_query = (
        session.query(
            TaskInstance.dag_id, func.count(TaskInstance.task_id).label("failure_count")
        )
        .filter(TaskInstance.state.in_(["failed", "upstream_failed"]))
        .filter(tuple_(TaskInstance.dag_id, TaskInstance.run_id).in_(failed_runs))
        .group_by(TaskInstance.dag_id)
    )

    recent_failures_counts = recent_failures_query.all()

    # Handle SqlAlchemy returning empty list when no rows match
    if len(recent_failures_counts) == 0:
        return []

    if len(recent_failures_counts) == 1 and not recent_failures_counts[0].dag_id:
        return []

    return recent_failures_counts


def group_failures_by_dag(failures):
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


def get_dag_ids(session, tags_filter):
    """Get DAG IDs for active, non-paused DAGs matching the specified tags.

    Args:
        session: SQLAlchemy database session
        tags_filter: List of tag names to filter DAGs by

    Returns:
        List of DAG IDs that match the tag filter criteria
    """
    dag_ids = (
        session.query(DagModel.dag_id)
        .filter(~DagModel.is_subdag, DagModel.is_active, ~DagModel.is_paused)
        .filter(DagModel.tags.any(DagTag.name.in_(tags_filter)))
        .order_by(DagModel.dag_id)
        .all()
    )

    return [dag_id for (dag_id,) in dag_ids]


def get_dag_tags(session, tags_filter):
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


class BigRedButtonViewMixin:
    """Mixin class providing common clearing functionality for Big Red Button views."""

    def _get_redirect_url(self):
        """Get the URL to redirect to after clearing. Override in subclasses."""
        raise NotImplementedError

    def _render_clear_failed_dags(self, clear_window, dag_ids):
        """Render the confirmation page for clearing failed DAGs.

        Args:
            clear_window: Time window string (e.g., '1_hour')
            dag_ids: Optional list of DAG IDs to filter by, or None for all DAGs

        Returns:
            Rendered template showing failures grouped by DAG
        """
        time_cutoff = datetime.now(timezone.utc) - clear_windows[clear_window]
        with create_session() as session:
            recent_failures = get_recent_failures(
                session, time_cutoff=time_cutoff, dag_ids=dag_ids
            )
            recent_failures_by_dag = group_failures_by_dag(recent_failures)
            return self.render_template(
                "clear_failed_dags.html",
                recent_failures=recent_failures_by_dag,
                failure_count=len(recent_failures),
                endpoint="clear_failed_dags/confirm",
            )

    def _execute_clear_failed_dags(self, clear_window, dag_ids):
        """Execute the clearing of failed DAGs after confirmation.

        Args:
            clear_window: Time window string (e.g., '1_hour')
            dag_ids: Optional list of DAG IDs to filter by, or None for all DAGs

        Returns:
            Redirect to the main view
        """
        time_cutoff = datetime.now(timezone.utc) - clear_windows[clear_window]
        with create_session() as session:
            recent_failures = get_recent_failures(
                session, time_cutoff=time_cutoff, dag_ids=dag_ids
            )
            handle_clearing(session, recent_failures)
            return redirect(self._get_redirect_url())

    def _render_clear_failed_tasks(self, clear_window, dag_id):
        """Render the confirmation page for clearing failed tasks of a specific DAG.

        Args:
            clear_window: Time window string (e.g., '1_hour')
            dag_id: The DAG ID to clear tasks for

        Returns:
            Rendered template showing failures for the specified DAG
        """
        time_cutoff = datetime.now(timezone.utc) - clear_windows[clear_window]
        with create_session() as session:
            recent_failures = get_recent_failures(
                session, time_cutoff=time_cutoff, dag_ids=[dag_id]
            )
            return self.render_template(
                "clear_failed_tasks.html",
                recent_failures=recent_failures,
                endpoint="clear_failed_tasks/confirm",
            )

    def _execute_clear_failed_tasks(self, clear_window, dag_id):
        """Execute the clearing of failed tasks for a specific DAG after confirmation.

        Args:
            clear_window: Time window string (e.g., '1_hour')
            dag_id: The DAG ID to clear tasks for

        Returns:
            Redirect to the main view
        """
        time_cutoff = datetime.now(timezone.utc) - clear_windows[clear_window]
        with create_session() as session:
            recent_failures = get_recent_failures(
                session, time_cutoff=time_cutoff, dag_ids=[dag_id]
            )
            handle_clearing(session, recent_failures)
            return redirect(self._get_redirect_url())


class BigRedButtonBaseView(BigRedButtonViewMixin, BaseView):
    """Flask view for the Big Red Button interface (tag-filtered version)."""

    default_view = "big_red_button"

    def _get_redirect_url(self):
        """Get the URL to redirect to after clearing."""
        return "/bigredbuttonbaseview"

    @expose("/")
    @has_access_view()
    def big_red_button(self):
        """Render the main Big Red Button page with tag filtering and failure counts.

        Handles tag filtering via URL params and session cookies, allowing users to
        view failure counts for DAGs matching selected tags.
        """
        clear_window = request.args.get("clear_window")
        tags_filter = request.args.getlist("tags")

        if not clear_window:
            clear_window = ONE_HOUR

        if request.args.get("reset_tags") is not None:
            flask_session[FILTER_TAGS_COOKIE] = None
            # Remove the reset_tags=reset from the URL
            return redirect(url_for("BigRedButtonBaseView.big_red_button"))

        cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
        if tags_filter:
            flask_session[FILTER_TAGS_COOKIE] = ",".join(tags_filter)
        elif cookie_val:
            # If tags exist in cookie, but not URL, add them to the URL
            return redirect(
                url_for(
                    "BigRedButtonBaseView.big_red_button", tags=cookie_val.split(",")
                )
            )

        with create_session() as session:
            tags = get_dag_tags(session, tags_filter)

            recent_failure_counts = []
            if tags_filter:
                dag_ids = get_dag_ids(session, tags_filter)
                if dag_ids:
                    time_cutoff = (
                            datetime.now(timezone.utc) - clear_windows[clear_window]
                    )
                    recent_failure_counts = get_recent_failure_counts(
                        session, time_cutoff=time_cutoff, dag_ids=dag_ids
                    )

            return self.render_template(
                "big_red_button.html",
                clear_window=clear_window,
                recent_failures=recent_failure_counts,
                tags=tags,
                tags_filter=tags_filter,
            )

    @expose("/clear_failed_dags", methods=["POST"])
    @has_access_view()
    def clear_failed_dags(self):
        """Display confirmation page showing all failed and upstream_failed tasks grouped by DAG ID.

        This is the first step of the two-step clearing process for DAGs.
        """
        clear_window = request.form.get("clear_window")
        tags_filter = request.form.get("tags_filter")

        if tags_filter:
            # tags_filter is a string that is structured like a list
            # literal_eval parses it out
            tags = literal_eval(tags_filter)
            with create_session() as session:
                dag_ids = get_dag_ids(session, tags)
                if dag_ids:
                    return self._render_clear_failed_dags(clear_window, dag_ids)

    @expose("/clear_failed_dags/confirm", methods=["POST"])
    @has_access_view()
    def _clear_failed_dags(self):
        """Execute the actual clearing of failed and upstream_failed tasks after confirmation.

        This is the second step that performs the clearing operation.
        """
        clear_window = request.form.get("clear_window")
        tags_filter = request.form.get("tags_filter")

        if tags_filter:
            # tags_filter is a string that is structured like a list
            # literal_eval parses it out
            tags = literal_eval(tags_filter)
            with create_session() as session:
                dag_ids = get_dag_ids(session, tags)
                if dag_ids:
                    return self._execute_clear_failed_dags(clear_window, dag_ids)

    @expose("/clear_failed_tasks", methods=["POST"])
    @has_access_view()
    def clear_failed_tasks(self):
        """Display confirmation page showing failed and upstream_failed tasks for a specific DAG.

        This is the first step of the two-step clearing process for individual tasks.
        """
        clear_window = request.form.get("clear_window")
        dag_id = request.form.get("dag_id")
        if dag_id:
            return self._render_clear_failed_tasks(clear_window, dag_id)

    @expose("/clear_failed_tasks/confirm", methods=["POST"])
    @has_access_view()
    def _clear_failed_tasks(self):
        """Execute the actual clearing of failed and upstream_failed tasks for a specific DAG after confirmation.

        This is the second step that performs the clearing operation.
        """
        clear_window = request.form.get("clear_window")
        dag_id = request.form.get("dag_id")
        if dag_id:
            return self._execute_clear_failed_tasks(clear_window, dag_id)


class BigRedButtonAdminBaseView(BigRedButtonViewMixin, BaseView):
    """Flask view for the Big Red Button Admin interface (shows all DAGs without tag filtering)."""

    default_view = "big_red_button_admin"

    def _get_redirect_url(self):
        """Get the URL to redirect to after clearing."""
        return "/bigredbuttonadminbaseview"

    @expose("/")
    @has_access_view()
    def big_red_button_admin(self):
        """Render the admin Big Red Button page showing all DAG failures without tag filtering."""
        clear_window = request.args.get("clear_window")
        if not clear_window:
            clear_window = ONE_HOUR
        time_cutoff = datetime.now(timezone.utc) - clear_windows[clear_window]
        with create_session() as session:
            recent_failure_counts = get_recent_failure_counts(
                session, time_cutoff=time_cutoff
            )

            return self.render_template(
                "big_red_button_admin.html",
                clear_window=clear_window,
                recent_failures=recent_failure_counts,
            )

    @expose("/clear_failed_dags", methods=["POST"])
    @has_access_view()
    def clear_failed_dags(self):
        """Display confirmation page showing all failed and upstream_failed tasks (admin version without tag filtering).

        This is the first step of the two-step clearing process for all DAGs.
        """
        clear_window = request.form.get("clear_window")
        return self._render_clear_failed_dags(clear_window, dag_ids=None)

    @expose("/clear_failed_dags/confirm", methods=["POST"])
    @has_access_view()
    def _clear_failed_dags(self):
        """Execute the actual clearing of all failed and upstream_failed tasks after confirmation (admin version).

        This is the second step that performs the clearing operation.
        """
        clear_window = request.form.get("clear_window")
        return self._execute_clear_failed_dags(clear_window, dag_ids=None)

    @expose("/clear_failed_tasks", methods=["POST"])
    @has_access_view()
    def clear_failed_tasks(self):
        """Display confirmation page showing failed and upstream_failed tasks for a specific DAG (admin version).

        This is the first step of the two-step clearing process for individual tasks.
        """
        clear_window = request.form.get("clear_window")
        dag_id = request.form.get("dag_id")
        if dag_id:
            return self._render_clear_failed_tasks(clear_window, dag_id)

    @expose("/clear_failed_tasks/confirm", methods=["POST"])
    @has_access_view()
    def _clear_failed_tasks(self):
        """Execute the actual clearing of failed and upstream_failed tasks for a specific DAG after confirmation (admin version).

        This is the second step that performs the clearing operation.
        """
        clear_window = request.form.get("clear_window")
        dag_id = request.form.get("dag_id")
        if dag_id:
            return self._execute_clear_failed_tasks(clear_window, dag_id)


big_red_button_base_view = BigRedButtonBaseView()
big_red_button_package = {
    "name": "Big Red Button",
    "category": "Admin",
    "view": big_red_button_base_view,
}

big_red_button_admin_base_view = BigRedButtonAdminBaseView()
big_red_button_admin_package = {
    "name": "Big Red Button: Admin",
    "category": "Admin",
    "view": big_red_button_admin_base_view,
}


# Defining the plugin class
class BigRedButtonPlugin(AirflowPlugin):
    """Airflow plugin that registers the Big Red Button views and blueprints.

    Provides two interfaces:
    - Big Red Button: Tag-filtered view for selective DAG clearing
    - Big Red Button: Admin: Unrestricted view for clearing all DAGs
    """

    name = "big_red_button"
    flask_blueprints = [bp]
    appbuilder_views = [big_red_button_package, big_red_button_admin_package]
