from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LoginForm(BaseModel):
    username: str = Field(min_length=1, max_length=40)
    password: str = Field(min_length=1, max_length=128)


class CaseForm(BaseModel):
    title: str = Field(min_length=1, max_length=120)
    payload: dict[str, Any]


class ReviewForm(BaseModel):
    note: str = Field(min_length=1, max_length=500)


class MigrationForm(BaseModel):
    """Permissive initiation body.

    The same intent can be expressed with several field names because callers
    describe the selection as 盘点编号 / 档案编号 and the idempotent order in
    different ways. Every field is optional; an empty body migrates nothing only
    when no archive exists.
    """

    model_config = ConfigDict(extra='allow')

    snapshot_id: str | list[str] | None = None
    snapshot_ids: list[str] | None = None
    ids: list[str] | list[int] | None = None
    case_id: int | None = None
    case_ids: list[int] | None = None

    order_no: str | None = None
    order_id: str | int | None = None
    idempotency_key: str | None = None
    key: str | None = None
    request_id: str | None = None
    client_order_id: str | None = None

    source: str | None = None
    source_db: str | None = None
    source_database: str | None = None
    from_db: str | None = None
    from_database: str | None = None
    target: str | None = None
    target_db: str | None = None
    target_database: str | None = None
    to_db: str | None = None
    to_database: str | None = None
    new_database: str | None = None

    actor: str | None = None
    note: str | None = None
    run: bool | None = True
    execute: bool | None = True
    auto_switch: bool | None = True
    wait: bool | None = True
