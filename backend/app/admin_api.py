import calendar

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import LIFF_URL
from app.db import (
    get_conn,
    get_group_by_token,
    get_group_members,
    get_shift_board,
    get_shift_requirements,
    get_shift_targets,
    get_submission_status,
    save_bot_log,
    save_shift_requirements,
    save_shift_targets,
    update_group_name,
    upsert_group_member_with_name,
)
from app.line_api import (
    get_group_member_ids,
    get_group_member_profile,
    get_group_summary,
    push_group_text_message,
)

router = APIRouter(prefix="/api/admin", tags=["admin"])


class TargetsSaveRequest(BaseModel):
    target_month: str
    line_user_ids: list[str]


class RequirementItem(BaseModel):
    shift_date: str
    is_closed: bool = False
    lunch_required: int = 0
    dinner_required: int = 0
    note: str = ""


class RequirementsSaveRequest(BaseModel):
    target_month: str
    requirements: list[RequirementItem]


class BotRunRequest(BaseModel):
    job_type: str
    target_month: str


def require_group(group_token: str):
    group = get_group_by_token(group_token)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    return group


def get_days_in_month(target_month: str) -> int:
    year, month = map(int, target_month.split("-"))
    return calendar.monthrange(year, month)[1]


def build_member_name_map(group_id: int):
    members = get_group_members(group_id, active_only=True)
    return {
        row["line_user_id"]: row["display_name"] or row["line_user_id"]
        for row in members
    }


def build_daily_status(group_id: int, month: str):
    targets = get_shift_targets(group_id, month)
    target_ids = [row["line_user_id"] for row in targets]

    requirements = get_shift_requirements(group_id, month)
    board = get_shift_board(group_id, month)

    requirement_map = {
        row["shift_date"]: {
            "is_closed": bool(row["is_closed"]),
            "lunch_required": row["lunch_required"],
            "dinner_required": row["dinner_required"],
            "note": row["note"] or "",
        }
        for row in requirements
    }

    days_in_month = get_days_in_month(month)
    lunch_daily = {day: 0 for day in range(1, days_in_month + 1)}
    dinner_daily = {day: 0 for day in range(1, days_in_month + 1)}

    for user_id, days in board["lunch"].items():
        if user_id in target_ids:
            for day in days:
                lunch_daily[day] += 1

    for user_id, days in board["dinner"].items():
        if user_id in target_ids:
            for day in days:
                dinner_daily[day] += 1

    daily_status = []
    for day in range(1, days_in_month + 1):
        shift_date = f"{month}-{day:02d}"
        req = requirement_map.get(
            shift_date,
            {
                "is_closed": False,
                "lunch_required": 0,
                "dinner_required": 0,
                "note": "",
            },
        )

        daily_status.append(
            {
                "shift_date": shift_date,
                "is_closed": req["is_closed"],
                "lunch_required": req["lunch_required"],
                "dinner_required": req["dinner_required"],
                "lunch_submitted": lunch_daily[day],
                "dinner_submitted": dinner_daily[day],
                "lunch_diff": lunch_daily[day] - req["lunch_required"],
                "dinner_diff": dinner_daily[day] - req["dinner_required"],
                "note": req["note"],
            }
        )

    return daily_status


def build_recruit_message(group_name: str, month: str):
    lines = [
        f"【{group_name} シフト募集】",
        f"対象月: {month}",
        "",
        "シフト提出をお願いします。",
    ]

    if LIFF_URL:
        lines += [
            "",
            "提出はこちら:",
            LIFF_URL,
        ]

    return "\n".join(lines)


def build_missing_message(group_name: str, month: str, missing_names: list[str]):
    lines = [
        f"【{group_name} 未提出通知】",
        f"対象月: {month}",
        "",
    ]

    if missing_names:
        lines.append("未提出:")
        lines.extend([f"- {name}" for name in missing_names])
    else:
        lines.append("未提出者はいません。")

    if LIFF_URL:
        lines += [
            "",
            "提出はこちら:",
            LIFF_URL,
        ]

    return "\n".join(lines)


def build_status_message(group_name: str, month: str, target_count: int, submitted_count: int, missing_count: int):
    return "\n".join([
        f"【{group_name} 提出状況】",
        f"対象月: {month}",
        f"対象人数: {target_count}",
        f"提出済み: {submitted_count}",
        f"未提出: {missing_count}",
    ])


def build_shortage_message(group_name: str, month: str, daily_status: list[dict]):
    shortage_lines = []

    for row in daily_status:
        if row["is_closed"]:
            continue

        lunch_short = row["lunch_diff"] < 0
        dinner_short = row["dinner_diff"] < 0

        if lunch_short or dinner_short:
            parts = [row["shift_date"]]

            if lunch_short:
                parts.append(f"LUNCH {abs(row['lunch_diff'])}人不足")

            if dinner_short:
                parts.append(f"DINNER {abs(row['dinner_diff'])}人不足")

            shortage_lines.append(" / ".join(parts))

    if not shortage_lines:
        return "\n".join([
            f"【{group_name} 不足日通知】",
            f"対象月: {month}",
            "不足日はありません。",
        ])

    return "\n".join([
        f"【{group_name} 不足日通知】",
        f"対象月: {month}",
        "",
        *shortage_lines,
    ])


@router.get("/groups")
def list_groups():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT g.id, g.line_group_id, g.group_name, g.group_token,
               COUNT(m.id) as member_count
        FROM groups g
        LEFT JOIN group_members m
          ON g.id = m.group_id AND m.is_active = 1
        GROUP BY g.id, g.line_group_id, g.group_name, g.group_token
        ORDER BY COALESCE(g.group_name, g.line_group_id)
        """
    )
    rows = cur.fetchall()
    conn.close()

    return {
        "groups": [
            {
                "group_id": row["id"],
                "line_group_id": row["line_group_id"],
                "group_name": row["group_name"] or row["line_group_id"],
                "group_token": row["group_token"],
                "member_count": row["member_count"],
            }
            for row in rows
        ]
    }


@router.post("/groups/{group_token}/sync-members")
def sync_members(group_token: str):
    group = require_group(group_token)

    summary = get_group_summary(group["line_group_id"])
    if summary and summary.get("groupName"):
        update_group_name(group["line_group_id"], summary["groupName"])

    member_ids = get_group_member_ids(group["line_group_id"])
    if member_ids is None:
        raise HTTPException(status_code=500, detail="Failed to fetch group members")

    synced_count = 0

    for user_id in member_ids:
        profile = get_group_member_profile(group["line_group_id"], user_id)
        display_name = None
        if profile:
            display_name = profile.get("displayName")

        upsert_group_member_with_name(
            group_id=group["id"],
            line_user_id=user_id,
            display_name=display_name,
            active=True,
        )
        synced_count += 1

    return {
        "ok": True,
        "group_id": group["id"],
        "group_name": summary.get("groupName") if summary else group["group_name"],
        "synced_count": synced_count,
    }


@router.get("/groups/{group_token}/members")
def get_members(group_token: str):
    group = require_group(group_token)
    members = get_group_members(group["id"], active_only=True)

    return {
        "group_id": group["id"],
        "group_name": group["group_name"],
        "group_token": group["group_token"],
        "members": [
            {
                "line_user_id": row["line_user_id"],
                "display_name": row["display_name"] or row["line_user_id"],
                "is_active": row["is_active"],
            }
            for row in members
        ],
    }


@router.get("/groups/{group_token}/targets")
def get_targets(group_token: str, month: str):
    group = require_group(group_token)
    targets = get_shift_targets(group["id"], month)
    return {
        "group_id": group["id"],
        "target_month": month,
        "line_user_ids": [row["line_user_id"] for row in targets],
    }


@router.post("/groups/{group_token}/targets")
def save_targets(group_token: str, payload: TargetsSaveRequest):
    group = require_group(group_token)
    save_shift_targets(group["id"], payload.target_month, payload.line_user_ids)
    return {
        "ok": True,
        "group_id": group["id"],
        "target_month": payload.target_month,
        "target_count": len(set(payload.line_user_ids)),
    }


@router.get("/groups/{group_token}/requirements")
def get_requirements(group_token: str, month: str):
    group = require_group(group_token)
    requirements = get_shift_requirements(group["id"], month)
    return {
        "group_id": group["id"],
        "target_month": month,
        "requirements": [
            {
                "shift_date": row["shift_date"],
                "is_closed": bool(row["is_closed"]),
                "lunch_required": row["lunch_required"],
                "dinner_required": row["dinner_required"],
                "note": row["note"] or "",
            }
            for row in requirements
        ],
    }


@router.post("/groups/{group_token}/requirements")
def save_requirements(group_token: str, payload: RequirementsSaveRequest):
    group = require_group(group_token)
    save_shift_requirements(
        group["id"],
        payload.target_month,
        [item.model_dump() for item in payload.requirements],
    )
    return {
        "ok": True,
        "group_id": group["id"],
        "target_month": payload.target_month,
        "requirement_count": len(payload.requirements),
    }


@router.get("/groups/{group_token}/status")
def get_status(group_token: str, month: str):
    group = require_group(group_token)

    members = get_group_members(group["id"], active_only=True)
    member_name_map = {
        row["line_user_id"]: row["display_name"] or row["line_user_id"]
        for row in members
    }

    targets = get_shift_targets(group["id"], month)
    target_ids = [row["line_user_id"] for row in targets]

    status = get_submission_status(group["id"], month)
    submitted_ids = sorted(status["submitted_ids"])
    missing_ids = sorted(status["missing_ids"])

    daily_status = build_daily_status(group["id"], month)

    return {
        "group_id": group["id"],
        "group_name": group["group_name"] or group["line_group_id"],
        "group_token": group["group_token"],
        "target_month": month,
        "target_count": len(target_ids),
        "submitted_count": len(submitted_ids),
        "missing_count": len(missing_ids),
        "submitted_members": [
            {
                "line_user_id": user_id,
                "display_name": member_name_map.get(user_id, user_id),
            }
            for user_id in submitted_ids
        ],
        "missing_members": [
            {
                "line_user_id": user_id,
                "display_name": member_name_map.get(user_id, user_id),
            }
            for user_id in missing_ids
        ],
        "daily_status": daily_status,
    }


@router.post("/groups/{group_token}/bot/run")
def run_bot(group_token: str, payload: BotRunRequest):
    group = require_group(group_token)
    month = payload.target_month
    job_type = payload.job_type

    member_name_map = build_member_name_map(group["id"])
    status = get_submission_status(group["id"], month)

    target_ids = sorted(status["target_ids"])
    submitted_ids = sorted(status["submitted_ids"])
    missing_ids = sorted(status["missing_ids"])

    group_name = group["group_name"] or group["line_group_id"]
    message_text = ""

    if job_type == "recruit":
        message_text = build_recruit_message(group_name, month)

    elif job_type == "missing_reminder":
        missing_names = [member_name_map.get(user_id, user_id) for user_id in missing_ids]
        message_text = build_missing_message(group_name, month, missing_names)

    elif job_type == "status_summary":
        message_text = build_status_message(
            group_name=group_name,
            month=month,
            target_count=len(target_ids),
            submitted_count=len(submitted_ids),
            missing_count=len(missing_ids),
        )

    elif job_type == "shortage_summary":
        daily_status = build_daily_status(group["id"], month)
        message_text = build_shortage_message(group_name, month, daily_status)

    else:
        raise HTTPException(status_code=400, detail="Unknown job_type")

    result = push_group_text_message(group["line_group_id"], message_text)

    if not result["ok"]:
        raise HTTPException(
            status_code=500,
            detail=f"LINE push failed: {result['status_code']} {result['body']}",
        )

    save_bot_log(group["id"], job_type, message_text)

    return {
        "ok": True,
        "group_id": group["id"],
        "group_name": group_name,
        "job_type": job_type,
        "message_text": message_text,
    }