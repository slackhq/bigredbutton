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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from airflow.utils.session import create_session

app = FastAPI(title="Big Red Button API")

STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Imported after `app` is defined to avoid a circular import: the plugin module
# (big_red_button.py) imports `app` from here at class-definition time, and
# Airflow loads these modules alphabetically (api.py first).
from plugins.big_red_button.auth import (  # noqa: E402
    GetUserDep,
    RequiresAdminDep,
    RequiresDagEditDep,
    is_admin,
)
from plugins.big_red_button.big_red_button import (  # noqa: E402
    clear_windows,
    get_dag_ids,
    get_dag_tags,
    get_recent_failures,
    group_failures_by_dag,
    handle_clearing,
    log_clearing,
)


class FailureInfo(BaseModel):
    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    state: str


class DagFailureSummary(BaseModel):
    dag_id: str
    failure_count: int
    failures: list[FailureInfo]


class FailuresResponse(BaseModel):
    total_failures: int
    dags: list[DagFailureSummary]


class TagInfo(BaseModel):
    name: str
    selected: bool


class ClearRequest(BaseModel):
    clear_window: str
    dag_id: Optional[str] = None
    tags_filter: Optional[list[str]] = None


class ClearResponse(BaseModel):
    cleared_count: int


class MeResponse(BaseModel):
    is_admin: bool


def _build_failures_response(failures_by_dag: dict) -> FailuresResponse:
    total = 0
    dags = []
    for did, failures in failures_by_dag.items():
        total += len(failures)
        dags.append(
            DagFailureSummary(
                dag_id=did,
                failure_count=len(failures),
                failures=[
                    FailureInfo(
                        dag_id=f.dag_id,
                        task_id=f.task_id,
                        run_id=f.run_id,
                        map_index=f.map_index if f.map_index is not None else -1,
                        state=f.state,
                    )
                    for f in failures
                ],
            )
        )
    return FailuresResponse(total_failures=total, dags=dags)


@app.get("/api/me", response_model=MeResponse)
async def get_me(user: GetUserDep):
    """Tell the UI whether to offer admin mode. Not a security boundary."""
    return MeResponse(is_admin=is_admin(user))


# ---------------------------------------------------------------------------
# User endpoints — tag-scoped, requires tags to be specified.
# Reads require authentication; the clear mutation requires DAG-edit permission.
# ---------------------------------------------------------------------------


@app.get("/api/failures", response_model=FailuresResponse)
async def get_failures(
    user: GetUserDep,
    clear_window: str = Query(default="1_hour", enum=list(clear_windows.keys())),
    tags: list[str] = Query(..., min_length=1),
    dag_id: Optional[str] = Query(default=None),
):
    if clear_window not in clear_windows:
        raise HTTPException(
            status_code=400, detail=f"Invalid clear_window: {clear_window}"
        )

    time_cutoff = datetime.now(timezone.utc) - clear_windows[clear_window]

    with create_session() as session:
        if dag_id:
            dag_ids = [dag_id]
        else:
            dag_ids = get_dag_ids(session, tags)
            if not dag_ids:
                return FailuresResponse(total_failures=0, dags=[])

        recent_failures = get_recent_failures(
            session, time_cutoff=time_cutoff, dag_ids=dag_ids
        )
        failures_by_dag = group_failures_by_dag(recent_failures)
        return _build_failures_response(failures_by_dag)


@app.get("/api/tags", response_model=list[TagInfo])
async def get_tags(
    user: GetUserDep, selected: Optional[list[str]] = Query(default=None)
):
    with create_session() as session:
        tags = get_dag_tags(session, selected)
        return [TagInfo(**t) for t in tags]


@app.post("/api/clear", response_model=ClearResponse)
async def clear_failures(req: ClearRequest, user: RequiresDagEditDep):
    if req.clear_window not in clear_windows:
        raise HTTPException(
            status_code=400, detail=f"Invalid clear_window: {req.clear_window}"
        )

    if not req.tags_filter and not req.dag_id:
        raise HTTPException(
            status_code=400,
            detail="tags_filter or dag_id is required. Use the admin endpoint to clear all.",
        )

    time_cutoff = datetime.now(timezone.utc) - clear_windows[req.clear_window]

    with create_session() as session:
        if req.dag_id:
            dag_ids = [req.dag_id]
        else:
            dag_ids = get_dag_ids(session, req.tags_filter)
            if not dag_ids:
                return ClearResponse(cleared_count=0)

        recent_failures = get_recent_failures(
            session, time_cutoff=time_cutoff, dag_ids=dag_ids
        )
        handle_clearing(session, recent_failures)

        log_clearing(
            rows_cleared=len(recent_failures),
            clear_window=req.clear_window,
            dag_id=req.dag_id,
            tags_filter=req.tags_filter,
            user=getattr(user, "username", "unknown"),
            user_display=getattr(user, "name", ""),
        )

        return ClearResponse(cleared_count=len(recent_failures))


# ---------------------------------------------------------------------------
# Admin endpoints — unrestricted, can operate on all DAGs.
# Restricted to the ADMIN role via RequiresAdminDep. Airflow can't hide a
# react_app nav entry per role, so the single page only offers admin mode when
# /api/me reports is_admin. These endpoints are the real security boundary:
# non-admins receive a 403.
# ---------------------------------------------------------------------------


@app.get("/api/admin/failures", response_model=FailuresResponse)
async def get_failures_admin(
    user: RequiresAdminDep,
    clear_window: str = Query(default="1_hour", enum=list(clear_windows.keys())),
    tags: Optional[list[str]] = Query(default=None),
    dag_id: Optional[str] = Query(default=None),
):
    if clear_window not in clear_windows:
        raise HTTPException(
            status_code=400, detail=f"Invalid clear_window: {clear_window}"
        )

    time_cutoff = datetime.now(timezone.utc) - clear_windows[clear_window]

    with create_session() as session:
        dag_ids = None
        if dag_id:
            dag_ids = [dag_id]
        elif tags:
            dag_ids = get_dag_ids(session, tags)
            if not dag_ids:
                return FailuresResponse(total_failures=0, dags=[])

        recent_failures = get_recent_failures(
            session, time_cutoff=time_cutoff, dag_ids=dag_ids
        )
        failures_by_dag = group_failures_by_dag(recent_failures)
        return _build_failures_response(failures_by_dag)


@app.post("/api/admin/clear", response_model=ClearResponse)
async def clear_failures_admin(req: ClearRequest, user: RequiresAdminDep):
    if req.clear_window not in clear_windows:
        raise HTTPException(
            status_code=400, detail=f"Invalid clear_window: {req.clear_window}"
        )

    time_cutoff = datetime.now(timezone.utc) - clear_windows[req.clear_window]

    with create_session() as session:
        dag_ids = None
        if req.dag_id:
            dag_ids = [req.dag_id]
        elif req.tags_filter:
            dag_ids = get_dag_ids(session, req.tags_filter)
            if not dag_ids:
                return ClearResponse(cleared_count=0)

        recent_failures = get_recent_failures(
            session, time_cutoff=time_cutoff, dag_ids=dag_ids
        )
        handle_clearing(session, recent_failures)

        log_clearing(
            rows_cleared=len(recent_failures),
            clear_window=req.clear_window,
            dag_id=req.dag_id,
            tags_filter=req.tags_filter,
            user=getattr(user, "username", "unknown"),
            user_display=getattr(user, "name", ""),
        )

        return ClearResponse(cleared_count=len(recent_failures))
