"""
data/geojson/convert_geojson.py
Convert dvhcvn format → standard GeoJSON + map district names to canonical
Run: python data/geojson/convert_geojson.py
"""

import json
import os

# -------------------------------------------------------
# Name mapping: dvhcvn Vietnamese name → canonical
# -------------------------------------------------------
DISTRICT_CANONICAL = {
    # HCMC
    "Quận 1":           "District 1",
    "Quận 2":           "Thu Duc City",
    "Quận 3":           "District 3",
    "Quận 4":           "District 4",
    "Quận 5":           "District 5",
    "Quận 6":           "District 6",
    "Quận 7":           "District 7",
    "Quận 8":           "District 8",
    "Quận 9":           "Thu Duc City",
    "Quận 10":          "District 10",
    "Quận 11":          "District 11",
    "Quận 12":          "District 12",
    "Quận Bình Thạnh":  "Binh Thanh",
    "Quận Phú Nhuận":   "Phu Nhuan",
    "Quận Gò Vấp":      "Go Vap",
    "Quận Tân Bình":    "Tan Binh",
    "Quận Tân Phú":     "Tan Phu",
    "Quận Bình Tân":    "Binh Tan",
    "Thành phố Thủ Đức":"Thu Duc City",
    "Huyện Nhà Bè":     "Nha Be",
    "Huyện Bình Chánh": "Binh Chanh",
    "Huyện Hóc Môn":    "Hoc Mon",
    "Huyện Củ Chi":     "Cu Chi",
    "Huyện Cần Giờ":    "Can Gio",
    # HN
    "Quận Hoàn Kiếm":   "Hoan Kiem",
    "Quận Ba Đình":      "Ba Dinh",
    "Quận Đống Đa":      "Dong Da",
    "Quận Hai Bà Trưng": "Hai Ba Trung",
    "Quận Hoàng Mai":    "Hoang Mai",
    "Quận Thanh Xuân":   "Thanh Xuan",
    "Quận Cầu Giấy":     "Cau Giay",
    "Quận Tây Hồ":       "Tay Ho",
    "Quận Long Biên":    "Long Bien",
    "Quận Bắc Từ Liêm":  "Bac Tu Liem",
    "Quận Nam Từ Liêm":  "Nam Tu Liem",
    "Quận Hà Đông":      "Ha Dong",
    "Huyện Gia Lâm":     "Gia Lam",
    "Huyện Đông Anh":    "Dong Anh",
    "Huyện Sóc Sơn":     "Soc Son",
    "Huyện Thanh Trì":   "Thanh Tri",
    "Huyện Hoài Đức":    "Hoai Duc",
    "Huyện Đan Phượng":  "Dan Phuong",
    "Huyện Thường Tín":  "Thuong Tin",
    "Huyện Phú Xuyên":   "Phu Xuyen",
    "Huyện Ứng Hòa":     "Ung Hoa",
    "Huyện Mỹ Đức":      "My Duc",
    "Huyện Chương Mỹ":   "Chuong My",
    "Huyện Thanh Oai":   "Thanh Oai",
    "Huyện Thạch Thất":  "Thach That",
    "Huyện Quốc Oai":    "Quoc Oai",
    "Huyện Ba Vì":        "Ba Vi",
    "Huyện Phúc Thọ":    "Phuc Tho",
    "Huyện Mê Linh":     "Me Linh",
    "Thị xã Sơn Tây":    "Son Tay",
}


def convert(input_path: str, output_path: str, city: str):
    with open(input_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    features = []
    skipped  = []

    for district in raw.get("level2s", []):
        raw_name   = district.get("name", "")
        canonical  = DISTRICT_CANONICAL.get(raw_name)

        if not canonical:
            skipped.append(raw_name)
            canonical = raw_name  # fallback: keep original

        coordinates = district.get("coordinates", [])

        # dvhcvn stores as list of polygons (each polygon = list of rings)
        # need to detect if MultiPolygon or Polygon
        if not coordinates:
            continue

        # if coordinates[0][0] is a list of lists → MultiPolygon
        # if coordinates[0][0] is a list of numbers → Polygon
        try:
            is_multi = isinstance(coordinates[0][0][0][0], float) or isinstance(coordinates[0][0][0][0], int)
        except (IndexError, TypeError):
            is_multi = False

        if len(coordinates) > 1:
            geometry = {
                "type": "MultiPolygon",
                "coordinates": coordinates
            }
        else:
            geometry = {
                "type": "Polygon",
                "coordinates": coordinates[0]
            }

        feature = {
            "type": "Feature",
            "properties": {
                "name_raw":   raw_name,
                "name":       canonical,
                "city":       city,
                "level2_id":  district.get("level2_id", ""),
            },
            "geometry": geometry
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"[{city}] Converted {len(features)} districts → {output_path}")
    if skipped:
        print(f"  Unmapped names (used raw): {skipped}")


if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__))

    convert(
        input_path=os.path.join(base, "79.json"),
        output_path=os.path.join(base, "hcmc.geojson"),
        city="HCMC",
    )
    convert(
        input_path=os.path.join(base, "1.json"),
        output_path=os.path.join(base, "hn.geojson"),
        city="HN",
    )
