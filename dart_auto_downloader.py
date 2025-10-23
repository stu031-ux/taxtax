#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART 자동 다운로더 v2 (검색 개선: 정확일치 + 부분일치 동시 표출)
----------------------------------------------------------------
- "삼성"처럼 짧은 키워드도 계열사가 여러 개 나오도록 수정
- 정확일치가 있으면 맨 위에, 그 외 부분일치도 함께 나열
- 최대 200건 표시, 상장사(주식코드 있음) 우선 정렬

설치:
    pip install requests pandas openpyxl
"""

import io
import re
import time
import zipfile
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

API_HOST = "https://opendart.fss.or.kr"
CORPCODE_API = f"{API_HOST}/api/corpCode.xml"
LIST_API    = f"{API_HOST}/api/list.json"
DOC_API     = f"{API_HOST}/api/document.xml"

CACHE_DIR = Path("_dart_cache")
CACHE_DIR.mkdir(exist_ok=True)

S = requests.Session()
S.headers.update({"User-Agent": "dart-auto-downloader/1.2"})

def sanitize_filename(name: str) -> str:
    if not name:
        name = "unknown_report"
    bad = r'\\/:*?"<>|'
    for ch in bad:
        name = name.replace(ch, "_")
    name = "_".join(name.split())
    return name[:120]

def input_nonempty(prompt: str) -> str:
    while True:
        s = input(prompt).strip()
        if s:
            return s

# ─────────────────────────────────────────────────────────────────────
def fetch_corp_master(api_key: str) -> pd.DataFrame:
    cache_zip = CACHE_DIR / "corpCode.zip"
    cache_xml = CACHE_DIR / "CORPCODE.xml"
    need = True
    if cache_xml.exists():
        if (time.time() - cache_xml.stat().st_mtime) < 30*24*3600:
            need = False
    if need:
        print("📥 법인코드 마스터 다운로드 중…")
        r = S.get(CORPCODE_API, params={"crtfc_key": api_key}, timeout=60)
        r.raise_for_status()
        cache_zip.write_bytes(r.content)
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(CACHE_DIR)

    import xml.etree.ElementTree as ET
    root = ET.parse(cache_xml).getroot()
    rows = []
    for el in root.findall(".//list"):
        rows.append({
            "corp_code": el.findtext("corp_code") or "",
            "corp_name": el.findtext("corp_name") or "",
            "stock_code": el.findtext("stock_code") or "",
        })
    return pd.DataFrame(rows)

def search_companies(master: pd.DataFrame, query: str) -> pd.DataFrame:
    """회사명 검색 (정확일치 + 부분일치 동시 표출)"""
    q = (query or "").strip()
    if not q:
        return master.head(0)

    m = master.copy()
    m["__norm"] = m["corp_name"].fillna("").str.replace(r"\s+", "", regex=True)
    qn = re.sub(r"\s+", "", q)

    # 정확일치(공백/대소문자 무시)
    mask_exact = m["__norm"].str.casefold() == qn.casefold()
    exact = m[mask_exact].copy()
    exact["__rank"] = 0  # 최우선

    # 부분일치
    mask_part = m["__norm"].str.contains(re.escape(qn), case=False, regex=True)
    part = m[mask_part & (~mask_exact)].copy()
    part["__rank"] = 1

    # 합치기: 정확일치 먼저, 그 다음 부분일치
    res = pd.concat([exact, part], ignore_index=True)

    # 정렬: rank → 상장사 우선(stock_code 비어있지 않음) → 회사명
    res["__listed"] = res["stock_code"].fillna("").ne("")
    res = res.sort_values(by=["__rank", "__listed", "corp_name"], ascending=[True, False, True])

    return res.head(200).drop(columns=["__norm","__rank","__listed"], errors="ignore")

# ─────────────────────────────────────────────────────────────────────
def fetch_list(api_key: str, corp_code: str, year: str) -> List[Dict]:
    out = []
    page_no = 1
    bgn_de = f"{year}0101"
    end_de = f"{year}1231"
    print(f"📡 공시 목록 조회: {corp_code} / {bgn_de} ~ {end_de}")
    while True:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": 100
        }
        r = S.get(LIST_API, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "000":
            print("❌ list.json 오류:", data.get("message"))
            break
        items = data.get("list") or []
        out.extend(items)
        total = int(data.get("total_count", 0))
        if len(out) >= total or not items:
            break
        page_no += 1
        time.sleep(0.15)
    print(f"✅ 목록 {len(out)}건 수집")
    return out

def is_zip(content: bytes) -> bool:
    return len(content) > 1000 and content[:4] == b"PK\x03\x04"

def download_zip(api_key: str, rcept_no: str, out_dir: Path, rcept_dt: str, report_nm: str) -> Optional[Path]:
    """ZIP 다운로드 (파일명에 제출일+보고서명 포함)"""
    params = {"crtfc_key": api_key, "rcept_no": rcept_no}
    r = S.get(DOC_API, params=params, timeout=60)
    content = r.content or b""
    if r.status_code == 200 and is_zip(content):
        # 파일명 구성: 20241021_사업보고서_20241021001234.zip
        safe_name = sanitize_filename(f"{rcept_dt}_{report_nm}_{rcept_no}")
        out = out_dir / f"{safe_name}.zip"
        out.write_bytes(content)
        return out
    return None

def main():
    print("\n=== DART 자동 다운로더 v2 (검색 개선) ===\n")
    api_key = input_nonempty("🔑 OpenDART API Key: ")

    master = fetch_corp_master(api_key)
    while True:
        keyword = input_nonempty("\n🏢 회사명(부분 일치 가능): ")
        cand = search_companies(master, keyword)
        if cand.empty:
            print("⚠️ 검색 결과가 없습니다. 다시 입력하세요.")
            continue

        cand = cand.reset_index(drop=True)
        print(f"\n🔎 검색 결과 {len(cand)}건 (최대 200건 표시)")
        for i, row in cand.head(200).iterrows():
            sc = f" / 주식코드:{row['stock_code']}" if row["stock_code"] else ""
            print(f"  [{i}] {row['corp_name']}  (corp_code:{row['corp_code']}{sc})")

        sel = input("선택할 번호 입력 (다시 검색하려면 'r'): ").strip()
        if sel.lower() == 'r' or sel == "":
            continue
        if not sel.isdigit() or int(sel) < 0 or int(sel) >= len(cand):
            print("⚠️ 올바른 번호를 입력하세요.")
            continue
        pick = cand.iloc[int(sel)]
        corp_code = pick["corp_code"]
        corp_name = pick["corp_name"]
        break

    year = input_nonempty("\n📅 다운로드 연도 (예: 2024): ")
    if not (len(year) == 4 and year.isdigit()):
        print("⚠️ 연도 형식이 올바르지 않아 현재 연도로 진행합니다.")
        year = str(datetime.now().year)

    safe_corp = sanitize_filename(corp_name)
    out_dir = Path(f"D:/DART_공시자료/DART_{year}_{safe_corp}_{corp_code}_ZIP")
    out_dir.mkdir(exist_ok=True)
    print(f"\n📁 저장 폴더: {out_dir.resolve()}")

    items = fetch_list(api_key, corp_code, year)
    if not items:
        print("종료합니다. (목록 없음)")
        return

    summary = []
    for idx, it in enumerate(items, start=1):
        rcept_no  = it.get("rcept_no", "")
        rcept_dt  = it.get("rcept_dt", "")
        report_nm = it.get("report_nm") or it.get("rpt_nm") or "unknown_report"
        file_label = f"{rcept_dt}_{sanitize_filename(report_nm)}.zip"

        print(f"[{idx}/{len(items)}] {report_nm} ({rcept_dt})  접수번호:{rcept_no}")
        existing = out_dir / f"{rcept_no}.zip"
        if existing.exists() and existing.stat().st_size > 1000:
            print("  ↪ 이미 존재 (건너뜀)")
        else:
            out = download_zip(api_key, rcept_no, out_dir, rcept_dt, report_nm)
            if out:
                print(f"  ✅ 저장: {out.name}  → 표시용: {file_label}")
            else:
                print(f"  ⚠️ 실패 (document.xml 응답이 ZIP이 아님)")

        summary.append({
            "기업명": corp_name,
            "corp_code": corp_code,
            "보고서명": report_nm,
            "접수번호": rcept_no,
            "제출일": rcept_dt,
            "ZIP저장파일": out.name if out else f"{rcept_no}.zip",
            "DART링크": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        })

        time.sleep(0.35)

    df = pd.DataFrame(summary)
    excel_path = out_dir / f"공시ZIP요약_{year}.xlsx"
    csv_path   = out_dir / f"공시ZIP요약_{year}.csv"
    try:
        df.to_excel(excel_path, index=False)
    except Exception as e:
        print("  ⚠️ 엑셀 저장 실패:", e)
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    except Exception as e:
        print("  ⚠️ CSV 저장 실패:", e)

    print("\n🎉 완료!")
    print(f"📊 요약(Excel): {excel_path}")
    print(f"📄 요약(CSV)  : {csv_path}")
    print(f"🧾 ZIP 개수   : {len(df)} (폴더 내 rcept_no.zip 형태)")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n종료합니다.")
