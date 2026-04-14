from datetime import datetime
import calendar

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.db import (
    get_group_by_token,
    get_group_members,
    get_shift_board,
    get_shift_submission,
    save_shift_submission,
)

router = APIRouter(prefix="/api/liff", tags=["liff"])


class ShiftSaveRequest(BaseModel):
    line_user_id: str
    target_month: str
    lunch_days: list[int] = []
    dinner_days: list[int] = []
    note: str = ""


def get_days_in_month(target_month: str) -> int:
    year, month = map(int, target_month.split("-"))
    return calendar.monthrange(year, month)[1]


@router.get("/groups/{group_token}/shift-board")
def get_shift_board_api(group_token: str, month: str):
    group = get_group_by_token(group_token)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    members = get_group_members(group["id"], active_only=True)
    board = get_shift_board(group["id"], month)

    return {
        "group": {
            "id": group["id"],
            "line_group_id": group["line_group_id"],
            "group_token": group["group_token"],
            "group_name": group["group_name"],
        },
        "target_month": month,
        "days_in_month": get_days_in_month(month),
        "members": [
            {
                "line_user_id": row["line_user_id"],
                "display_name": row["display_name"] or row["line_user_id"],
                "is_active": row["is_active"],
            }
            for row in members
        ],
        "lunch": board["lunch"],
        "dinner": board["dinner"],
    }


@router.get("/groups/{group_token}/my-shift")
def get_my_shift_api(group_token: str, month: str, line_user_id: str):
    group = get_group_by_token(group_token)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    submission = get_shift_submission(group["id"], line_user_id, month)
    if not submission:
        return {
            "line_user_id": line_user_id,
            "target_month": month,
            "lunch_days": [],
            "dinner_days": [],
            "note": "",
            "submitted_at": None,
        }

    return {
        "line_user_id": line_user_id,
        "target_month": month,
        "lunch_days": submission["lunch_days"],
        "dinner_days": submission["dinner_days"],
        "note": submission["note"],
        "submitted_at": submission["submitted_at"],
    }


@router.post("/groups/{group_token}/shift-board")
def save_shift_board_api(group_token: str, payload: ShiftSaveRequest):
    group = get_group_by_token(group_token)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    save_shift_submission(
        group_id=group["id"],
        line_user_id=payload.line_user_id,
        target_month=payload.target_month,
        lunch_days=payload.lunch_days,
        dinner_days=payload.dinner_days,
        note=payload.note,
    )

    saved = get_shift_submission(group["id"], payload.line_user_id, payload.target_month)

    return {
        "ok": True,
        "group_token": group_token,
        "line_user_id": payload.line_user_id,
        "target_month": payload.target_month,
        "lunch_days": saved["lunch_days"],
        "dinner_days": saved["dinner_days"],
        "note": saved["note"],
        "submitted_at": saved["submitted_at"],
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }