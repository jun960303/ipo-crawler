# crawler/ipo38.py

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from .base import insert_ipo, get_write_conn

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

URLS = {
    "bidding": {
        "base": "http://www.38.co.kr/html/fund/index.htm?o=k&page=",
        "summary": "공모주 청약일정",
    },
    "bookbuilding": {
        "base": "http://www.38.co.kr/html/fund/index.htm?o=r&page=",
        "summary": "수요예측일정",
    },
    "listing": {
        "base": "http://www.38.co.kr/html/fund/index.htm?o=nw&page=",
        "summary": "신규상장종목",
    },
}

# 카테고리별 최대 페이지 (핵심 데이터만 크롤링)
MAX_PAGES = {
    "bidding": 5,  # 공모청약일정: 최근 5페이지
    "bookbuilding": 5,  # 수요예측일정: 최근 5페이지
    "listing": 3,  # 신규상장종목: 최근 3페이지
}


def crawl_38_all(log_func=None, stop_checker=None):
    """
    38커뮤니케이션 전체 크롤링
    - 카테고리별 페이지 제한 적용
    - 오늘 이후 일정만 DB 저장
    - stop_checker()가 True면 중간 종료
    """
    total = 0

    total += crawl_category("bidding", log_func, stop_checker)
    total += crawl_category("bookbuilding", log_func, stop_checker)
    total += crawl_category("listing", log_func, stop_checker)

    # 🔥 모든 INSERT 끝나고 마지막에 commit 1번만
    conn = get_write_conn()
    conn.commit()

    if log_func:
        log_func(f"✅ 38커뮤니케이션 전체 {total}건 저장 완료")

    return total


# ---------------------- 공통 유틸 ----------------------


def get_html(url):
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def get_rows(url, summary):
    soup = get_html(url)
    table = soup.find("table", {"summary": summary})
    if not table:
        return None, 0

    rows = table.find_all("tr")[2:]  # 헤더 2줄 제외
    return rows, len(rows)


# ---------------------- 카테고리 반복 ----------------------


def crawl_category(key, log_func=None, stop_checker=None):
    base = URLS[key]["base"]
    summary = URLS[key]["summary"]
    max_page = MAX_PAGES.get(key, 3)

    if log_func:
        log_func(f"▶ {summary} 전체 크롤링 시작... (최대 {max_page} 페이지)")

    page = 1
    count_total = 0

    while page <= max_page:
        # 중지 요청이면 바로 종료
        if stop_checker and stop_checker():
            if log_func:
                log_func("⛔ 사용자 요청으로 크롤링 중단")
            break

        url = base + str(page)
        if log_func:
            log_func(f"  ▶ 페이지 {page} 크롤링...")

        rows, row_count = get_rows(url, summary)

        if rows is None or row_count == 0:
            # 더 이상 데이터 없으면 종료
            break

        if key == "bidding":
            count_total += parse_bidding(rows)
        elif key == "bookbuilding":
            count_total += parse_bookbuilding(rows)
        else:
            count_total += parse_listing(rows)

        page += 1

    if log_func:
        log_func(f"  └ {summary} {count_total}건 저장")

    return count_total


# ---------------------- 파싱 함수 ----------------------


def parse_bidding(rows):
    """공모주 청약일정"""
    count = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        cols = [td.get_text(strip=True) for td in tds]

        stock = cols[0]
        date_range = cols[1]
        offer = cols[2]
        broker = cols[5]

        start, end = parse_range(date_range)

        # ✅ 오늘 이후 일정만 저장 (청약 종료일 기준)
        if end:
            if end < today:
                continue
        elif start:
            if start < today:
                continue

        insert_ipo(
            {
                "stock_name": stock,
                "status": "공모청약",
                "lead_manager": broker,
                "brokers": broker,
                "offer_price": to_float(offer),
                "sub_start": start,
                "sub_end": end,
                "listing_date": None,
                "demand_start": None,
                "demand_end": None,
                "refund_date": None,
                "source": "공모청약일정",
            }
        )
        count += 1
    return count


def parse_bookbuilding(rows):
    """수요예측일정"""
    count = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        cols = [td.get_text(strip=True) for td in tds]

        stock = cols[0]
        date_range = cols[1]
        offer = cols[3] if len(cols) > 3 else None
        broker = cols[5] if len(cols) > 5 else ""

        start, end = parse_range(date_range)

        # ✅ 오늘 이후 일정만 저장 (수요예측 종료일 기준)
        if end:
            if end < today:
                continue
        elif start:
            if start < today:
                continue

        insert_ipo(
            {
                "stock_name": stock,
                "status": "수요예측",
                "lead_manager": broker,
                "brokers": broker,
                "offer_price": to_float(offer),
                "sub_start": None,
                "sub_end": None,
                "listing_date": None,
                "demand_start": start,
                "demand_end": end,
                "refund_date": None,
                "source": "수요예측일정",
            }
        )
        count += 1
    return count


def parse_listing(rows):
    """신규상장종목"""
    count = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        cols = [td.get_text(strip=True) for td in tds]
        stock = cols[0]
        listing_raw = cols[1]

        offer = cols[4] if len(cols) >= 5 else None
        listing_date = normalize_date(listing_raw)

        # ✅ 오늘 이후 상장 예정만 저장
        if listing_date and listing_date < today:
            continue

        insert_ipo(
            {
                "stock_name": stock,
                "status": "상장",
                "lead_manager": None,
                "brokers": None,
                "offer_price": to_float(offer),
                "sub_start": None,
                "sub_end": None,
                "listing_date": listing_date,
                "demand_start": None,
                "demand_end": None,
                "refund_date": None,
                "source": "신규상장종목",
            }
        )

        count += 1
    return count


# ---------------------- 날짜/숫자 유틸 ----------------------


def normalize_date(text):
    if not text:
        return None
    try:
        dt = datetime.strptime(text.strip(), "%Y.%m.%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_range(text):
    if not text:
        return None, None
    text = text.replace(" ", "")

    if "~" not in text:
        return normalize_date(text), None

    start_raw, end_raw = text.split("~")
    year = start_raw.split(".")[0]

    if end_raw.count(".") == 1:
        end_raw = f"{year}.{end_raw}"

    return normalize_date(start_raw), normalize_date(end_raw)


def to_float(v):
    if not v:
        return None
    v = v.replace(",", "").replace("원", "").strip()
    return float(v) if v.isdigit() else None
