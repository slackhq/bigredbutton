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
"""Tests for the Big Red Button Airflow plugin.

NOTE: This test suite covers utility functions and business logic that can be tested with unit tests.
Complex SQL query functions (get_recent_failures_paged, get_recent_failure_counts) and Flask view methods
would benefit from integration tests with a real database and Flask app context.
"""

import sys
import pytest
from datetime import datetime, timedelta, timezone
from typing import Callable
from unittest.mock import Mock, MagicMock, patch

# Mock airflow modules before importing big_red_button
# This handles cases where airflow.www isn't available or Airflow models have changed
sys.modules['airflow.www'] = MagicMock()
sys.modules['airflow.www.extensions'] = MagicMock()
sys.modules['airflow.www.extensions.init_auth_manager'] = MagicMock()
sys.modules['airflow.www.auth'] = MagicMock()

from plugins.big_red_button.big_red_button import (
    handle_clearing,
    group_failures_by_dag,
    get_dag_ids,
    get_dag_tags,
    PAGE_SIZE,
    clear_windows,
    get_recent_failures,
)


@pytest.fixture
def mock_session() -> Mock:
    """Create a mock SQLAlchemy session."""
    session = Mock()
    return session


@pytest.fixture
def mock_task_instance() -> Callable:
    """Create a mock TaskInstance."""
    def _create_task_instance(
        dag_id: str = "test_dag",
        run_id: str = "test_run",
        task_id: str = "test_task",
        state: str = "failed"
    ) -> Mock:
        task = Mock()
        task.dag_id = dag_id
        task.run_id = run_id
        task.task_id = task_id
        task.state = state
        task.job_id = 1
        return task
    return _create_task_instance


class TestGroupFailuresByDag:
    """Tests for group_failures_by_dag function."""

    def test_empty_list(self):
        """Test grouping an empty list of failures."""
        result = group_failures_by_dag([])
        assert result == {}

    def test_single_dag(self, mock_task_instance):
        """Test grouping failures from a single DAG."""
        task1 = mock_task_instance(dag_id="dag1", task_id="task1")
        task2 = mock_task_instance(dag_id="dag1", task_id="task2")

        result = group_failures_by_dag([task1, task2])

        assert "dag1" in result
        assert len(result["dag1"]) == 2
        assert task1 in result["dag1"]
        assert task2 in result["dag1"]

    def test_multiple_dags(self, mock_task_instance):
        """Test grouping failures from multiple DAGs."""
        task1 = mock_task_instance(dag_id="dag1", task_id="task1")
        task2 = mock_task_instance(dag_id="dag2", task_id="task2")
        task3 = mock_task_instance(dag_id="dag1", task_id="task3")

        result = group_failures_by_dag([task1, task2, task3])

        assert len(result) == 2
        assert len(result["dag1"]) == 2
        assert len(result["dag2"]) == 1

    def test_preserves_task_order(self, mock_task_instance):
        """Test that grouping preserves the order of tasks within each DAG."""
        task1 = mock_task_instance(dag_id="dag1", task_id="task1")
        task2 = mock_task_instance(dag_id="dag1", task_id="task2")
        task3 = mock_task_instance(dag_id="dag1", task_id="task3")

        result = group_failures_by_dag([task1, task2, task3])

        assert result["dag1"][0] == task1
        assert result["dag1"][1] == task2
        assert result["dag1"][2] == task3


class TestGetDagIds:
    """Tests for get_dag_ids function."""

    def test_get_dag_ids_with_tags(self, mock_session):
        """Test getting DAG IDs filtered by tags."""
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [("dag1",), ("dag2",), ("dag3",)]
        mock_session.query.return_value = mock_query

        result = get_dag_ids(mock_session, ["tag1", "tag2"])

        assert result == ["dag1", "dag2", "dag3"]
        mock_session.query.assert_called_once()

    def test_get_dag_ids_empty_result(self, mock_session):
        """Test getting DAG IDs when no DAGs match the tags."""
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []
        mock_session.query.return_value = mock_query

        result = get_dag_ids(mock_session, ["nonexistent_tag"])

        assert result == []

    def test_get_dag_ids_single_tag(self, mock_session):
        """Test getting DAG IDs with a single tag filter."""
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [("dag1",)]
        mock_session.query.return_value = mock_query

        result = get_dag_ids(mock_session, ["single_tag"])

        assert result == ["dag1"]


class TestGetDagTags:
    """Tests for get_dag_tags function."""

    def test_get_dag_tags_no_filter(self, mock_session):
        """Test getting all DAG tags without a filter."""
        mock_query = Mock()
        mock_query.distinct.return_value = mock_query
        mock_query.all.return_value = [("tag1",), ("tag2",), ("tag3",)]
        mock_session.query.return_value = mock_query

        result = get_dag_tags(mock_session, None)

        assert len(result) == 3
        assert all(not tag["selected"] for tag in result)
        assert result[0]["name"] == "tag1"
        assert result[1]["name"] == "tag2"
        assert result[2]["name"] == "tag3"

    def test_get_dag_tags_with_filter(self, mock_session):
        """Test getting DAG tags with a selection filter."""
        mock_query = Mock()
        mock_query.distinct.return_value = mock_query
        mock_query.all.return_value = [("tag1",), ("tag2",), ("tag3",)]
        mock_session.query.return_value = mock_query

        result = get_dag_tags(mock_session, ["tag1", "tag3"])

        assert len(result) == 3
        assert result[0]["name"] == "tag1"
        assert result[0]["selected"] is True
        assert result[1]["name"] == "tag2"
        assert result[1]["selected"] is False
        assert result[2]["name"] == "tag3"
        assert result[2]["selected"] is True

    def test_get_dag_tags_empty_db(self, mock_session):
        """Test getting DAG tags when database has no tags."""
        mock_query = Mock()
        mock_query.distinct.return_value = mock_query
        mock_query.all.return_value = []
        mock_session.query.return_value = mock_query

        result = get_dag_tags(mock_session, None)

        assert result == []


class TestGetRecentFailures:
    """Tests for get_recent_failures function."""

    def test_get_recent_failures_combines_pages(self, mock_session, mock_task_instance):
        """Test that get_recent_failures correctly combines paged results."""
        time_cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        tasks_page1 = [mock_task_instance(task_id=f"task{i}") for i in range(100)]
        tasks_page2 = [mock_task_instance(task_id=f"task{i}") for i in range(100, 150)]

        with patch('plugins.big_red_button.big_red_button.get_recent_failures_paged') as mock_paged:
            mock_paged.return_value = [tasks_page1, tasks_page2]

            result = get_recent_failures(mock_session, time_cutoff)

            assert len(result) == 150
            mock_paged.assert_called_once_with(mock_session, time_cutoff, None)

    def test_get_recent_failures_with_dag_ids(self, mock_session, mock_task_instance):
        """Test that get_recent_failures passes dag_ids filter correctly."""
        time_cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        tasks = [mock_task_instance(dag_id="dag1", task_id=f"task{i}") for i in range(10)]

        with patch('plugins.big_red_button.big_red_button.get_recent_failures_paged') as mock_paged:
            mock_paged.return_value = [tasks]

            result = get_recent_failures(mock_session, time_cutoff, dag_ids=["dag1"])

            assert len(result) == 10
            mock_paged.assert_called_once_with(mock_session, time_cutoff, ["dag1"])

    def test_get_recent_failures_empty(self, mock_session):
        """Test get_recent_failures when no failures exist."""
        time_cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

        with patch('plugins.big_red_button.big_red_button.get_recent_failures_paged') as mock_paged:
            mock_paged.return_value = []

            result = get_recent_failures(mock_session, time_cutoff)

            assert len(result) == 0


class TestHandleClearing:
    """Tests for handle_clearing function."""

    @patch('plugins.big_red_button.big_red_button.log_clearing')
    @patch('plugins.big_red_button.big_red_button.clear_task_instances')
    def test_clear_small_batch(self, mock_clear, mock_log, mock_session, mock_task_instance):
        """Test clearing a small batch of tasks (less than PAGE_SIZE)."""
        tasks = [mock_task_instance(task_id=f"task{i}") for i in range(50)]

        handle_clearing(mock_session, tasks)

        mock_log.assert_called_once_with(50)
        mock_clear.assert_called_once_with(tasks, mock_session)

    @patch('plugins.big_red_button.big_red_button.log_clearing')
    @patch('plugins.big_red_button.big_red_button.clear_task_instances')
    def test_clear_exact_page_size(self, mock_clear, mock_log, mock_session, mock_task_instance):
        """Test clearing exactly PAGE_SIZE tasks."""
        tasks = [mock_task_instance(task_id=f"task{i}") for i in range(PAGE_SIZE)]

        handle_clearing(mock_session, tasks)

        mock_log.assert_called_once_with(PAGE_SIZE)
        mock_clear.assert_called_once_with(tasks, mock_session)

    @patch('plugins.big_red_button.big_red_button.log_clearing')
    @patch('plugins.big_red_button.big_red_button.clear_task_instances')
    def test_clear_large_batch(self, mock_clear, mock_log, mock_session, mock_task_instance):
        """Test clearing a large batch of tasks that requires pagination."""
        tasks = [mock_task_instance(task_id=f"task{i}") for i in range(450)]

        handle_clearing(mock_session, tasks)

        mock_log.assert_called_once_with(450)
        # Should be called 3 times: 200 + 200 + 50
        assert mock_clear.call_count == 3

        # Verify the batch sizes
        call_args = [call[0][0] for call in mock_clear.call_args_list]
        assert len(call_args[0]) == PAGE_SIZE  # First batch: 200
        assert len(call_args[1]) == PAGE_SIZE  # Second batch: 200
        assert len(call_args[2]) == 50          # Third batch: 50

    @patch('plugins.big_red_button.big_red_button.log_clearing')
    @patch('plugins.big_red_button.big_red_button.clear_task_instances')
    def test_clear_empty_list(self, mock_clear, mock_log, mock_session):
        """Test clearing an empty list of tasks."""
        handle_clearing(mock_session, [])

        mock_log.assert_called_once_with(0)
        mock_clear.assert_not_called()

    @patch('plugins.big_red_button.big_red_button.log_clearing')
    @patch('plugins.big_red_button.big_red_button.clear_task_instances')
    def test_clear_respects_page_boundaries(self, mock_clear, mock_log, mock_session, mock_task_instance):
        """Test that clearing properly handles page boundaries."""
        tasks = [mock_task_instance(task_id=f"task{i}") for i in range(401)]

        handle_clearing(mock_session, tasks)

        assert mock_clear.call_count == 3

        # Verify each batch contains the correct tasks
        first_batch = mock_clear.call_args_list[0][0][0]
        second_batch = mock_clear.call_args_list[1][0][0]
        third_batch = mock_clear.call_args_list[2][0][0]

        assert first_batch[0].task_id == "task0"
        assert first_batch[-1].task_id == "task199"
        assert second_batch[0].task_id == "task200"
        assert second_batch[-1].task_id == "task399"
        assert third_batch[0].task_id == "task400"


class TestLogClearing:
    """Tests for log_clearing function.

    Note: log_clearing requires a Flask request context and interacts with
    the Airflow authentication system. These tests would be better suited
    as integration tests with a proper Flask app context.
    """

    def test_log_clearing_integration_needed(self):
        """Placeholder test documenting that log_clearing needs integration tests."""
        # log_clearing accesses Flask's request object and Airflow's auth system
        # It should be tested in integration tests with:
        # - A Flask test client and app context
        # - Mock or test Airflow auth manager
        # - Test database session
        assert True, "log_clearing requires integration tests with Flask context"


class TestClearWindows:
    """Tests for clear_windows constant."""

    def test_clear_windows_defined(self):
        """Test that all expected clear windows are defined."""
        assert "1_hour" in clear_windows
        assert "12_hours" in clear_windows
        assert "1_day" in clear_windows
        assert "7_days" in clear_windows

    def test_clear_window_values(self):
        """Test that clear windows have correct timedelta values."""
        assert clear_windows["1_hour"] == timedelta(hours=1)
        assert clear_windows["12_hours"] == timedelta(hours=12)
        assert clear_windows["1_day"] == timedelta(days=1)
        assert clear_windows["7_days"] == timedelta(days=7)

    def test_clear_window_order(self):
        """Test that clear windows are in ascending order of time."""
        windows_list = [
            clear_windows["1_hour"],
            clear_windows["12_hours"],
            clear_windows["1_day"],
            clear_windows["7_days"],
        ]
        assert windows_list == sorted(windows_list)


class TestPageSize:
    """Tests for PAGE_SIZE constant."""

    def test_page_size_value(self):
        """Test that PAGE_SIZE has the expected value."""
        assert PAGE_SIZE == 200

    def test_page_size_positive(self):
        """Test that PAGE_SIZE is a positive integer."""
        assert isinstance(PAGE_SIZE, int)
        assert PAGE_SIZE > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
