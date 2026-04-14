import base64
import hashlib
import hmac
import json

import requests
from fastapi import APIRouter, Header, HTTPException, Request

from app.config import LINE_CHANNEL_ACCESS_TOKEN, LINE_CHANNEL_SECRET
from app.db import (
    mark_group_member_left,
    save_group,
    save_webhook_event,
    update_group_name,
    upsert_group_member,
)

router = APIRouter()


def verify_signature(body: bytes, signature: str) -> bool:
    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected_signature, signature)


def fetch_group_summary(group_id: str):
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return None

    url = f"https://api.line.me/v2/bot/group/{group_id}/summary"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if not response.ok:
            print("group summary fetch failed:", response.status_code, response.text)
            return None
        return response.json()
    except Exception as e:
        print("group summary fetch exception:", e)
        return None


@router.post("/webhook/line")
async def line_webhook(
    request: Request,
    x_line_signature: str = Header(default=""),
):
    body = await request.body()

    if not verify_signature(body, x_line_signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body.decode("utf-8"))
    events = payload.get("events", [])

    for event in events:
        event_type = event.get("type")
        source = event.get("source", {})
        source_type = source.get("type")
        source_group_id = source.get("groupId")
        source_user_id = source.get("userId")

        save_webhook_event(
            event_type=event_type or "",
            source_type=source_type,
            source_group_id=source_group_id,
            source_user_id=source_user_id,
            raw_json=json.dumps(event, ensure_ascii=False),
        )

        if source_group_id:
            save_group(source_group_id)

            summary = fetch_group_summary(source_group_id)
            if summary and summary.get("groupName"):
                update_group_name(
                    line_group_id=source_group_id,
                    group_name=summary["groupName"],
                )

        if source_group_id and source_user_id:
            upsert_group_member(
                line_group_id=source_group_id,
                line_user_id=source_user_id,
                display_name=None,
                active=True,
            )

        if event_type == "memberJoined" and source_group_id:
            joined = event.get("joined", {}).get("members", [])
            for member in joined:
                user_id = member.get("userId")
                if user_id:
                    upsert_group_member(
                        line_group_id=source_group_id,
                        line_user_id=user_id,
                        display_name=None,
                        active=True,
                    )

        if event_type == "memberLeft" and source_group_id:
            left = event.get("left", {}).get("members", [])
            for member in left:
                user_id = member.get("userId")
                if user_id:
                    mark_group_member_left(
                        line_group_id=source_group_id,
                        line_user_id=user_id,
                    )

    return {"ok": True}
