"""
etl/transformer.py — Transform RawListing → clean dict ready for DB insert
Handles: price parsing, area parsing, district normalization, date parsing
"""

import re
import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

# -------------------------------------------------------
# District normalization map
# raw (lowercase, stripped) → canonical
# Extend as needed when new raw names appear
# -------------------------------------------------------
DISTRICT_MAP = {
    # HCMC — numbered districts
    "quận 1": "District 1",        "q. 1": "District 1",       "q1": "District 1",
    "quận 2": "Thu Duc City",      "q. 2": "Thu Duc City",
    "quận 3": "District 3",        "q. 3": "District 3",
    "quận 4": "District 4",        "q. 4": "District 4",
    "quận 5": "District 5",        "q. 5": "District 5",
    "quận 6": "District 6",        "q. 6": "District 6",
    "quận 7": "District 7",        "q. 7": "District 7",
    "quận 8": "District 8",        "q. 8": "District 8",
    "quận 9": "Thu Duc City",      "q. 9": "Thu Duc City",
    "quận 10": "District 10",      "q. 10": "District 10",
    "quận 11": "District 11",      "q. 11": "District 11",
    "quận 12": "District 12",      "q. 12": "District 12",
    # HCMC — named districts
    "bình thạnh": "Binh Thanh",    "q. bình thạnh": "Binh Thanh",
    "phú nhuận": "Phu Nhuan",      "q. phú nhuận": "Phu Nhuan",
    "gò vấp": "Go Vap",            "q. gò vấp": "Go Vap",
    "tân bình": "Tan Binh",        "q. tân bình": "Tan Binh",
    "tân phú": "Tan Phu",          "q. tân phú": "Tan Phu",
    "bình tân": "Binh Tan",        "q. bình tân": "Binh Tan",
    "thủ đức": "Thu Duc City",     "tp. thủ đức": "Thu Duc City",  "tp thủ đức": "Thu Duc City",
    "nhà bè": "Nha Be",            "h. nhà bè": "Nha Be",
    "bình chánh": "Binh Chanh",    "h. bình chánh": "Binh Chanh",
    "hóc môn": "Hoc Mon",          "h. hóc môn": "Hoc Mon",
    "củ chi": "Cu Chi",            "h. củ chi": "Cu Chi",
    "cần giờ": "Can Gio",          "h. cần giờ": "Can Gio",
    # HN
    "hoàn kiếm": "Hoan Kiem",      "q. hoàn kiếm": "Hoan Kiem",
    "ba đình": "Ba Dinh",          "q. ba đình": "Ba Dinh",
    "đống đa": "Dong Da",          "q. đống đa": "Dong Da",
    "hai bà trưng": "Hai Ba Trung","q. hai bà trưng": "Hai Ba Trung",
    "hoàng mai": "Hoang Mai",      "q. hoàng mai": "Hoang Mai",
    "thanh xuân": "Thanh Xuan",    "q. thanh xuân": "Thanh Xuan",
    "cầu giấy": "Cau Giay",        "q. cầu giấy": "Cau Giay",
    "tây hồ": "Tay Ho",            "q. tây hồ": "Tay Ho",
    "long biên": "Long Bien",      "q. long biên": "Long Bien",
    "bắc từ liêm": "Bac Tu Liem",  "q. bắc từ liêm": "Bac Tu Liem",
    "nam từ liêm": "Nam Tu Liem",  "q. nam từ liêm": "Nam Tu Liem",
    "hà đông": "Ha Dong",          "q. hà đông": "Ha Dong",
    "gia lâm": "Gia Lam",          "h. gia lâm": "Gia Lam",
    "đông anh": "Dong Anh",        "h. đông anh": "Dong Anh",
    "sóc sơn": "Soc Son",          "h. sóc sơn": "Soc Son",
    "h. hoài đức":  "Hoai Duc",
    "h. thạch thất": "Thach That",
    "h. thanh trì": "Thanh Tri",
}


def normalize_district(raw: Optional[str]) -> Optional[str]:
    """
    Normalize raw district string → canonical name.
    Strips ward info: "Quận 7 (P. Tân Mỹ mới)" → "District 7"
    """
    if not raw:
        return None

    # strip ward info in parentheses: "Quận 7 (P. Tân Mỹ mới)" → "Quận 7"
    cleaned = re.sub(r"\(.*?\)", "", raw).strip()
    key = cleaned.lower().strip()

    canonical = DISTRICT_MAP.get(key)
    if not canonical:
        logger.debug(f"Unknown district: '{raw}' (key='{key}') — keeping raw")
        return cleaned  # keep cleaned version if not in map

    return canonical


def parse_price(price_raw: Optional[str]) -> Optional[int]:
    """
    Parse price string → VND integer.
    Examples:
        "2,7 tỷ"        → 2_700_000_000
        "450 triệu"     → 450_000_000
        "1.5 tỷ"        → 1_500_000_000
        "Giá thỏa thuận"→ None
        "Thỏa thuận"    → None
    """
    if not price_raw:
        return None

    text = price_raw.lower().strip()

    # negotiable prices → None
    if any(kw in text for kw in ["thỏa thuận", "thoa thuan", "liên hệ", "lien he"]):
        return None

    # normalize decimal separator: "2,7" → "2.7"
    text = text.replace(",", ".")

    # extract number
    match = re.search(r"([\d.]+)", text)
    if not match:
        return None

    value = float(match.group(1))

    if "tỷ" in text or "ty" in text:
        return int(value * 1_000_000_000)
    elif "triệu" in text or "trieu" in text:
        return int(value * 1_000_000)
    else:
        return int(value)


def parse_area(area_raw: Optional[str]) -> Optional[float]:
    """
    Parse area string → float m².
    Examples:
        "70 m²" → 70.0
        "68.5m²"→ 68.5
    """
    if not area_raw:
        return None

    match = re.search(r"([\d.,]+)", area_raw.replace(",", "."))
    if not match:
        return None

    try:
        return float(match.group(1))
    except ValueError:
        return None


def parse_bedrooms(bedrooms_raw: Optional[str]) -> Optional[int]:
    """
    Parse bedroom count → int.
    Examples: "2" → 2, "3+" → 3
    """
    if not bedrooms_raw:
        return None

    match = re.search(r"(\d+)", bedrooms_raw)
    if not match:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def parse_date(date_raw: Optional[str]) -> Optional[str]:
    """
    Parse date string → ISO format YYYY-MM-DD.
    Examples:
        "09/05/2026"   → "2026-05-09"
        "Đăng hôm nay" → today's date
        "Hôm qua"      → None (skip, too ambiguous)
    """
    if not date_raw:
        return None

    text = date_raw.strip()

    # aria-label format: "09/05/2026"
    match = re.match(r"(\d{2})/(\d{2})/(\d{4})", text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"

    # fallback: today
    if "hôm nay" in text.lower() or "today" in text.lower():
        return date.today().isoformat()

    return None


def transform(raw) -> Optional[dict]:
    """
    Transform a RawListing → clean dict for DB insert.
    Returns None if listing should be skipped (missing critical fields).
    """
    listing_id  = raw.listing_id
    source_url  = raw.source_url
    city        = raw.city

    if not listing_id or not source_url:
        return None

    title        = raw.title
    district_raw = raw.district_raw
    district     = normalize_district(district_raw)
    price_vnd    = parse_price(raw.price_raw)
    area_m2      = parse_area(raw.area_raw)
    bedrooms     = parse_bedrooms(raw.bedrooms_raw)
    posted_date  = parse_date(raw.posted_date)
    scraped_at   = raw.scraped_at.isoformat() if raw.scraped_at else datetime.now().isoformat()

    # compute price_per_m2
    price_per_m2 = None
    if price_vnd and area_m2 and area_m2 > 0:
        price_per_m2 = int(price_vnd / area_m2)

    return {
        "listing_id":   listing_id,
        "source_url":   source_url,
        "title":        title,
        "city":         city,
        "district_raw": district_raw,
        "district":     district,
        "area_m2":      area_m2,
        "price_vnd":    price_vnd,
        "price_per_m2": price_per_m2,
        "bedrooms":     bedrooms,
        "posted_date":  posted_date,
        "scraped_at":   scraped_at,
    }


# -------------------------------------------------------
# Quick test
# -------------------------------------------------------
if __name__ == "__main__":
    from dataclasses import dataclass
    from datetime import datetime

    @dataclass
    class MockRaw:
        listing_id:   str
        source_url:   str
        title:        str
        city:         str
        district_raw: str
        price_raw:    str
        area_raw:     str
        bedrooms_raw: str
        posted_date:  str
        scraped_at:   datetime = None

        def __post_init__(self):
            self.scraped_at = self.scraped_at or datetime.now()

    tests = [
        MockRaw("1", "https://x.com/1", "Test A", "HCMC", "Quận 7 (P. Tân Mỹ mới)", "2,7 tỷ",        "70 m²", "2", "09/05/2026"),
        MockRaw("2", "https://x.com/2", "Test B", "HCMC", "Q. Bình Tân (P. An Lạc mới)", "4,1 tỷ",   "68 m²", "2", "09/05/2026"),
        MockRaw("3", "https://x.com/3", "Test C", "HCMC", "Quận 9 (P. Long Bình mới)",  "Giá thỏa thuận", "70 m²", "2", "09/05/2026"),
        MockRaw("4", "https://x.com/4", "Test D", "HN",   "Cầu Giấy",                   "3.2 tỷ",     "65 m²", "3", "08/05/2026"),
    ]

    for raw in tests:
        result = transform(raw)
        print(f"[{result['listing_id']}] {result['district_raw']} → {result['district']}")
        print(f"  price_vnd   : {result['price_vnd']:,}" if result['price_vnd'] else "  price_vnd   : None (negotiable)")
        print(f"  price/m²    : {result['price_per_m2']:,}" if result['price_per_m2'] else "  price/m²    : None")
        print(f"  area_m2     : {result['area_m2']}")
        print(f"  posted_date : {result['posted_date']}")
        print()
