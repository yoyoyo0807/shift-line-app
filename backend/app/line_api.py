import requests

from app.config import LINE_CHANNEL_ACCESS_TOKEN


def _auth_headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }


def _auth_json_headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def get_group_summary(group_id: str):
    url = f"https://api.line.me/v2/bot/group/{group_id}/summary"
    response = requests.get(url, headers=_auth_headers(), timeout=10)

    if not response.ok:
        print("get_group_summary failed:", response.status_code, response.text)
        return None

    return response.json()


def get_group_member_ids(group_id: str):
    member_ids = []
    start = None

    while True:
        url = f"https://api.line.me/v2/bot/group/{group_id}/members/ids"
        params = {}
        if start:
            params["start"] = start

        response = requests.get(url, headers=_auth_headers(), params=params, timeout=10)

        if not response.ok:
            print("get_group_member_ids failed:", response.status_code, response.text)
            return None

        data = response.json()
        member_ids.extend(data.get("memberIds", []))

        start = data.get("next")
        if not start:
            break

    return member_ids


def get_group_member_profile(group_id: str, user_id: str):
    url = f"https://api.line.me/v2/bot/group/{group_id}/member/{user_id}"
    response = requests.get(url, headers=_auth_headers(), timeout=10)

    if not response.ok:
        print("get_group_member_profile failed:", response.status_code, response.text)
        return None

    return response.json()


def push_group_text_message(group_id: str, text: str):
    url = "https://api.line.me/v2/bot/message/push"
    payload = {
        "to": group_id,
        "messages": [
            {
                "type": "text",
                "text": text,
            }
        ],
    }

    response = requests.post(url, headers=_auth_json_headers(), json=payload, timeout=10)

    if not response.ok:
        print("push_group_text_message failed:", response.status_code, response.text)
        return {
            "ok": False,
            "status_code": response.status_code,
            "body": response.text,
        }

    return {
        "ok": True,
        "status_code": response.status_code,
    }