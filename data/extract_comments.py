import json
import pandas as pd
import requests
import time
import os

# Thiết lập session để tránh bị chặn
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': 'application/json'
})

def tim_file_du_lieu(thu_muc_hien_tai):
    """Tìm file JSON dữ liệu trong thư mục hiện tại hoặc thư mục cha."""
    ten_file_goc = "dataset_tiktok-scraper_2026-02-05_18-13-38-530.json"
    thu_muc_cha = os.path.dirname(thu_muc_hien_tai)
    
    # 1. Tìm theo tên chính xác
    duong_dan = os.path.join(thu_muc_cha, ten_file_goc)
    if os.path.exists(duong_dan): return duong_dan
    
    duong_dan = os.path.join(thu_muc_hien_tai, ten_file_goc)
    if os.path.exists(duong_dan): return duong_dan

    # 2. Nếu không thấy, tìm bất kỳ file .json nào trong thư mục data
    try:
        ung_vien = [f for f in os.listdir(thu_muc_hien_tai) if f.endswith('.json')]
        if ung_vien:
            duong_dan = os.path.join(thu_muc_hien_tai, ung_vien[0])
            print(f"⚠️  Không tìm thấy file gốc. Đang sử dụng file thay thế: '{ung_vien[0]}'")
            return duong_dan
    except Exception:
        pass
    return None

def main():
    # --- BƯỚC 1: THU THẬP DỮ LIỆU (DATA COLLECTION - LECTURE 2) ---
    thu_muc_hien_tai = os.path.dirname(__file__)
    duong_dan_json = tim_file_du_lieu(thu_muc_hien_tai)

    if not duong_dan_json:
        print("❌ Lỗi: Không tìm thấy file dữ liệu JSON nào trong thư mục data.")
        return

    print(f"📂 Đang đọc dữ liệu từ: {duong_dan_json}")
    with open(duong_dan_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    comments = []
    url_cache = {}

    print(f"🔄 Bắt đầu xử lý {len(data)} video...")
    for i, video in enumerate(data):
        video_id = video.get("id")
        
        # --- BƯỚC 2: TÍCH HỢP DỮ LIỆU (DATA INTEGRATION - LECTURE 3) ---
        # Lấy link video gốc để gắn vào từng bình luận (Mapping)
        parent_video_url = video.get("webVideoUrl") or video.get("videoWebUrl") or video.get("submittedVideoUrl")

        # Trường hợp 1: Bình luận đã có sẵn trong file JSON
        if "comments" in video and video["comments"]:
            for c in video["comments"]:
                # Chuẩn hóa dữ liệu nếu bị lỗi format string/bytes
                if not isinstance(c, dict):
                    try:
                        if isinstance(c, (bytes, bytearray)): c = c.decode('utf-8', errors='replace')
                        if isinstance(c, str): c = json.loads(c)
                    except Exception: continue
                
                if not isinstance(c, dict): continue
                
                try:
                    # Trích xuất thông tin quan trọng
                    author_meta = c.get("authorMeta") if isinstance(c.get("authorMeta"), dict) else {}
                    author = c.get("authorName") or c.get("uniqueId") or author_meta.get("nickName")
                    text = c.get("text") or (c.get("input") or {}).get("text")
                    
                    comments.append({
                        "cid": c.get("cid") or c.get("id"),
                        "videoId": video_id,
                        "videoUrl": parent_video_url,
                        "author": author,
                        "text": text,
                        "likes": c.get("diggCount") or c.get("likes") or 0,
                        "reply_count": c.get("replyCommentTotal") or 0,
                        "time": c.get("createTime") or c.get("createTimeISO")
                    })
                except Exception: continue

        # Trường hợp 2: Cần tải bình luận từ API (Apify Dataset)
        elif "commentsDatasetUrl" in video and video["commentsDatasetUrl"]:
            url = video["commentsDatasetUrl"]
            print(f"   ⬇️ Đang tải bình luận từ API cho video {i+1}/{len(data)}...")
            
            # Cơ chế Caching: Tránh gọi API nhiều lần cho cùng 1 URL
            if url in url_cache:
                items = url_cache[url]
            else:
                time.sleep(0.2) # Nghỉ nhẹ để tránh bị chặn
                try:
                    resp = session.get(url, timeout=30)
                    resp.raise_for_status()
                    items = resp.json()
                    url_cache[url] = items
                except Exception as e:
                    print(f"   ⚠️ Lỗi tải URL: {e}")
                    continue

            if isinstance(items, dict) and "items" in items:
                items = items["items"]

            if not isinstance(items, list): continue

            for c in items:
                if not isinstance(c, dict):
                    try:
                        if isinstance(c, (bytes, bytearray)): c = c.decode('utf-8', errors='replace')
                        if isinstance(c, str): c = json.loads(c)
                    except Exception: continue
                
                if not isinstance(c, dict): continue

                try:
                    author_meta = c.get("authorMeta") if isinstance(c.get("authorMeta"), dict) else {}
                    author = c.get("authorName") or c.get("uniqueId") or author_meta.get("nickName")
                    text = c.get("text") or (c.get("input") or {}).get("text")
                    
                    # Ưu tiên link video của chính comment đó nếu có
                    video_comment_url = c.get("videoWebUrl") or c.get("webVideoUrl") or parent_video_url
                    
                    comments.append({
                        "cid": c.get("cid") or c.get("id"),
                        "videoId": video_id,
                        "videoUrl": video_comment_url,
                        "author": author,
                        "text": text,
                        "likes": c.get("diggCount") or c.get("likes") or 0,
                        "reply_count": c.get("replyCommentTotal") or 0,
                        "time": c.get("createTime") or c.get("createTimeISO")
                    })
                except Exception:
                    continue
        
    # Chuyển sang DataFrame
    df = pd.DataFrame(comments)

    # Sắp xếp lại cột cho đẹp (đưa videoUrl lên ngay sau videoId)
    cols = list(df.columns)
    if 'videoUrl' in cols and 'videoId' in cols:
        cols.remove('videoUrl')
        try:
            idx = cols.index('videoId') + 1
        except ValueError:
            idx = 0
        cols.insert(idx, 'videoUrl')
        df = df[cols]

    # Xuất ra CSV
    output_path = os.path.join(thu_muc_hien_tai, "comments_only.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ Xong! Đã lưu {len(df)} bình luận vào file '{output_path}' (đã hoàn thành Integration).")

if __name__ == "__main__":
    main()
