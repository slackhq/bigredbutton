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
import os
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from jwt import ExpiredSignatureError, InvalidTokenError

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.auth.managers.models.resource_details import (
    DagAccessEntity,
    DagDetails,
)

AUTH_ENABLED = os.environ.get("BRB_AUTH_ENABLED", "true").lower() in (
    "true",
    "1",
    "yes",
)


class _AnonymousUser:
    username = "anonymous"
    name = "Anonymous"


async def get_user(request: Request) -> BaseUser:
    """Resolve the authenticated user from JWT token (cookie or Authorization header)."""
    if not AUTH_ENABLED:
        return _AnonymousUser()

    user: BaseUser | None = getattr(request.state, "user", None)
    if user:
        return user

    token_str: str | None = None
    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        token_str = auth_header[7:]
    else:
        token_str = request.cookies.get(COOKIE_NAME_JWT_TOKEN)

    if not token_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated"
        )

    try:
        return await get_auth_manager().get_user_from_token(token_str)
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
        )
    except InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token"
        )


GetUserDep = Annotated[BaseUser, Depends(get_user)]


def requires_dag_edit(user: GetUserDep) -> BaseUser:
    """Require permission to edit DAG task instances (admin-level clear operations)."""
    if not AUTH_ENABLED:
        return user

    auth_manager = get_auth_manager()
    if not auth_manager.is_authorized_dag(
        method="PUT",
        access_entity=DagAccessEntity.TASK_INSTANCE,
        details=DagDetails(id=None),
        user=user,
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    return user


RequiresDagEditDep = Annotated[BaseUser, Depends(requires_dag_edit)]
