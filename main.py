import requests
import pandas as pd
import time
import os
from typing import Dict, List, Any, Optional

# ==============================
# 설정값 (환경변수로 관리)
# ==============================
BRAZE_REST_ENDPOINT = "https://rest.iad-07.braze.com"
BRAZE_API_KEY = os.environ.get("BRAZE_API_KEY")  # 환경변수에서 읽어옴

HEADERS = {
    "Authorization": f"Bearer {BRAZE_API_KEY}",
    "Content-Type": "application/json",
}


# ==============================
# 공통 요청 함수
# ==============================
def safe_get(url: str, headers=None, params=None) -> Dict[str, Any]:
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if not r.ok:
        print("REQUEST URL:", r.url)
        print("STATUS:", r.status_code)
        print("BODY:", r.text[:2000])
        raise RuntimeError(f"Request failed: {r.status_code} | {r.text}")
    return r.json()


# ==============================
# 캠페인 리스트 조회
# ==============================
def get_campaigns_page(page: int):
    url = f"{BRAZE_REST_ENDPOINT}/campaigns/list"
    params = {
        "page": page,
        "include_archived": "true",
        "sort_direction": "asc",
    }
    data = safe_get(url, headers=HEADERS, params=params)
    return data.get("campaigns", [])


def get_all_campaigns():
    all_campaigns = []
    page = 0
    while True:
        campaigns = get_campaigns_page(page)
        if not campaigns:
            break
        all_campaigns.extend(campaigns)
        if len(campaigns) < 100:
            break
        page += 1
    return all_campaigns


# ==============================
# 캠페인 상세 조회
# ==============================
def get_campaign_details(campaign_id: str):
    url = f"{BRAZE_REST_ENDPOINT}/campaigns/details"
    params = {"campaign_id": campaign_id}
    return safe_get(url, headers=HEADERS, params=params)


# ==============================
# 포맷/버튼 가공
# ==============================
def extract_format_from_variant_name(variant_name: str) -> Optional[str]:
    if not variant_name:
        return None
    parts = variant_name.split("_")
    if len(parts) >= 2:
        return parts[1].strip().upper()
    return None


def get_valid_button_id(fmt: Optional[str]):
    if not fmt:
        return None
    fmt = fmt.upper()
    if fmt == "SLIDEUP":
        return None
    elif fmt == "CUSTOM":
        return "0"
    elif fmt == "MODAL":
        return "1"
    else:
        return None


# ==============================
# STEP 1: 캠페인 데이터 수집 및 가공
# ==============================
def flatten_campaign_variants():
    campaigns = get_all_campaigns()
    rows = []

    for c in campaigns:
        campaign_id = c.get("id")
        campaign_name = c.get("name")
        details = get_campaign_details(campaign_id)

        if details.get("draft") or details.get("archived"):
            continue

        messages = details.get("messages", {}) or {}

        for variant_api_id, variant_info in messages.items():
            variant_name = variant_info.get("name") or ""
            channel = variant_info.get("channel")

            if "control" in variant_name.lower():
                continue
            if channel != "trigger_in_app_message":
                continue

            fmt = extract_format_from_variant_name(variant_name)
            valid_button_id = get_valid_button_id(fmt)

            rows.append({
                "id": variant_api_id,
                "CAMPAIGN_API_ID": campaign_id,
                "CAMPAIGN_NAME": campaign_name,
                "CAMPAIGN_VARIANT_NAME": variant_name,
                "FORMAT": fmt,
                "VALID_BUTTON_ID": valid_button_id,
            })

    return rows


# ==============================
# STEP 2: Braze 카탈로그 업데이트
# ==============================
def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]


def update_catalog(df: pd.DataFrame):
    url = f"{BRAZE_REST_ENDPOINT}/catalogs/IAM_INFO/items"

    df = df.where(pd.notnull(df), None)
    items = df.to_dict(orient="records")

    print("\n=== 전체 전송 시작 ===")

    for idx, chunk in enumerate(chunked(items, 50), start=1):
        payload = {"items": chunk}
        response = requests.post(url, headers=HEADERS, json=payload)

        print(f"\n[Batch {idx}]")
        print("sent:", len(chunk))
        print("status:", response.status_code)
        print("body:", response.text)

        if response.status_code not in [200, 201, 202]:
            print("❌ 에러 발생")
            break

        time.sleep(0.3)

    print("\n=== 전송 완료 ===")


# ==============================
# 실행
# ==============================
if __name__ == "__main__":
    # STEP 1: 데이터 수집
    print("=== STEP 1: 캠페인 데이터 수집 ===")
    rows = flatten_campaign_variants()

    df = pd.DataFrame(rows)
    df = df[
        ["id", "CAMPAIGN_API_ID", "CAMPAIGN_NAME",
         "CAMPAIGN_VARIANT_NAME", "FORMAT", "VALID_BUTTON_ID"]
    ].sort_values(
        ["CAMPAIGN_NAME", "CAMPAIGN_VARIANT_NAME"]
    ).reset_index(drop=True)

    print(f"수집 완료: {len(df)} rows")

    # STEP 2: 카탈로그 업데이트
    print("\n=== STEP 2: 카탈로그 업데이트 ===")
    update_catalog(df)
