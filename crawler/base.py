# crawler/base.py
import os
import sys
import sqlite3
from datetime import datetime


# -------------------------------
# 🔥 PyInstaller 호환 DB 경로 처리
# -------------------------------
def resource_path(relative_path: str):
    """
    개발환경(소스코드 실행)과 PyInstaller EXE 실행 모두에서
    리소스 파일(DB 등)을 올바르게 찾는 경로 반환
    """
    # EXE로 실행된 경우(_MEIPASS 존재)
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS
    else:
        # 개발환경에서는 프로젝트 기준 상대경로
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


# 🔥 실제 DB 경로 (db/ipo.db)
DB_PATH = resource_path(os.path.join("db", "ipo.db"))

# 크롤링 중에만 쓰는 전역 write connection
WRITE_CONN = None


def get_write_conn():
    """크롤링 동안 하나의 쓰기 전용 커넥션만 유지"""
    global WRITE_CONN
    if WRITE_CONN is None:
        WRITE_CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
    return WRITE_CONN


def get_connection():
    """일반 조회용 커넥션 (사용 후 반드시 close)"""
    return sqlite3.connect(DB_PATH)


def init_db():
    """테이블 생성"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ipo_schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_name TEXT,
            status TEXT,
            lead_manager TEXT,
            brokers TEXT,
            offer_price REAL,
            sub_start TEXT,
            sub_end TEXT,
            listing_date TEXT,
            demand_start TEXT,
            demand_end TEXT,
            refund_date TEXT,
            source TEXT,
            created_at TEXT
        );
        """
    )

    conn.commit()
    conn.close()


def insert_ipo(data):
    conn = get_write_conn()
    cur = conn.cursor()

    # 🔥 중복 체크
    cur.execute(
        """
        SELECT id FROM ipo_schedules
        WHERE stock_name = ?
        AND status = ?
        AND IFNULL(sub_start, '') = IFNULL(?, '')
        AND IFNULL(demand_start, '') = IFNULL(?, '')
        AND IFNULL(listing_date, '') = IFNULL(?, '')
        """,
        (
            data["stock_name"],
            data["status"],
            data["sub_start"],
            data["demand_start"],
            data["listing_date"],
        ),
    )

    exists = cur.fetchone()
    if exists:
        return  # 이미 동일 데이터 있음 → INSERT 안 함

    # 중복 없으면 INSERT 수행
    cur.execute(
        """
        INSERT INTO ipo_schedules
        (stock_name, status, lead_manager, brokers, offer_price,
         sub_start, sub_end, listing_date, demand_start, demand_end,
         refund_date, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["stock_name"],
            data["status"],
            data["lead_manager"],
            data["brokers"],
            data["offer_price"],
            data["sub_start"],
            data["sub_end"],
            data["listing_date"],
            data["demand_start"],
            data["demand_end"],
            data["refund_date"],
            data["source"],
        ),
    )


def get_upcoming_by_broker(broker_name: str):
    """
    특정 증권사가 주관하는 '오늘 이후 예정 공모주'만 조회
    """
    conn = get_connection()
    cur = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")

    query = """
    SELECT stock_name, status, sub_start, sub_end, demand_start, demand_end, listing_date, source
    FROM ipo_schedules
    WHERE
        brokers LIKE ? AND (
            (sub_start IS NOT NULL AND sub_start >= ?) OR
            (demand_start IS NOT NULL AND demand_start >= ?) OR
            (listing_date IS NOT NULL AND listing_date >= ?)
        )
    ORDER BY
        CASE
            WHEN sub_start IS NOT NULL THEN sub_start
            WHEN demand_start IS NOT NULL THEN demand_start
            WHEN listing_date IS NOT NULL THEN listing_date
        END
    """

    cur.execute(query, (f"%{broker_name}%", today, today, today))
    rows = cur.fetchall()

    conn.close()
    return rows
