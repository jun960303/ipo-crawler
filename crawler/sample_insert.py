# crawler/sample_insert.py
from .base import insert_ipo

def insert_sample_data():
    sample = {
        "stock_name": "테스트기업",
        "status": "예정",
        "lead_manager": "미래에셋",
        "brokers": "미래에셋,KB,NH",
        "offer_price": 25000,
        "sub_start": "2025-01-10",
        "sub_end": "2025-01-11",
        "listing_date": "2025-01-20",
        "demand_start": "2025-01-05",
        "demand_end": "2025-01-06",
        "refund_date": "2025-01-13",
        "source": "샘플데이터"
    }

    insert_ipo(sample)
