import pandas as pd
import json
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import os

# --- CẤU HÌNH ĐƯỜNG DẪN ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
# Tự động tìm file json trong thư mục data nếu tên thay đổi
json_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
FILE_JSON = os.path.join(DATA_DIR, json_files[0]) if json_files else os.path.join(DATA_DIR, 'dataset_tiktok-scraper.json')
FILE_CSV = os.path.join(DATA_DIR, 'comments_only.csv')

def main():
    print("🚀 Bắt đầu phân tích...")

    # --- BƯỚC 1: TÍCH HỢP (Lecture 3) ---
    print(f"   Đọc metadata từ: {os.path.basename(FILE_JSON)}")
    if not os.path.exists(FILE_JSON):
        print(f"❌ Không tìm thấy file JSON trong {DATA_DIR}")
        return

    with open(FILE_JSON, 'r', encoding='utf-8') as f:
        meta = json.load(f)
    
    df_meta = pd.DataFrame(meta)[['id', 'playCount', 'diggCount']]
    df_meta = df_meta.rename(columns={'id': 'videoId'})
    
    print(f"   Đọc comments từ: {os.path.basename(FILE_CSV)}")
    if not os.path.exists(FILE_CSV):
        print(f"❌ Không tìm thấy file CSV: {FILE_CSV}")
        return
        
    df_comments = pd.read_csv(FILE_CSV)

    df_meta['videoId'] = df_meta['videoId'].astype(str)
    df_comments['videoId'] = df_comments['videoId'].astype(str)
    df = pd.merge(df_comments, df_meta, on='videoId', how='left')
    print(f"   ✓ Đã tích hợp {len(df)} dòng dữ liệu.")

    # --- BƯỚC 2: TIỀN XỬ LÝ (Lecture 3) ---
    print("🔄 Đang tiền xử lý dữ liệu...")
    def clean_pipeline(t):
        t = str(t)
        if pd.isna(t) or '[sticker]' in t.lower(): return None
        t = re.sub(r'@\w+', '', t) # Xóa tag
        t = t.lower()
        # Chuẩn hóa Teencode
        teencode = {'ko': 'không', 'k': 'không', 'đc': 'được', 'j': 'gì', 'khum': 'không'}
        for word, rep in teencode.items():
            t = re.sub(fr'\b{word}\b', rep, t)
        t = re.sub(r'[^\w\s]', '', t) # Xóa ký tự lạ
        return t.strip() if len(t.strip()) > 0 else None

    df['cleaned'] = df['text'].apply(clean_pipeline)
    df = df.dropna(subset=['cleaned'])

    # --- BƯỚC 3: PHÂN TÍCH CẢM XÚC (Lecture 7) ---
    print("🤖 Đang phân tích cảm xúc (Keyword-based)...")
    pos = ['hay', 'tuyệt', 'vui', 'thích', 'ok', 'hài', 'mèo', 'cute', 'mê']
    neg = ['dở', 'tệ', 'buồn', 'ghét', 'xấu']

    def get_sentiment(t):
        p = sum(1 for w in pos if w in t)
        n = sum(1 for w in neg if w in t)
        return 'Positive' if p > n else ('Negative' if n > p else 'Neutral')

    df['sentiment'] = df['cleaned'].apply(get_sentiment)

    # --- BƯỚC 4: TRỰC QUAN HÓA (Lecture 5) ---
    print("📊 Đang vẽ biểu đồ...")
    # Biểu đồ tròn
    plt.figure(figsize=(8,6))
    df['sentiment'].value_counts().plot(kind='pie', autopct='%1.1f%%', colors=['#99ff99','#66b3ff','#ff9999'])
    plt.title('Phân bổ cảm xúc người xem - BTL IT4142')
    plt.ylabel('')
    output_chart = os.path.join(DATA_DIR, 'sentiment_final.png')
    plt.savefig(output_chart)
    print(f"   ✓ Đã lưu biểu đồ: {output_chart}")

    # WordCloud
    print("☁️  Đang tạo WordCloud...")
    text_combined = " ".join(df['cleaned'])
    if text_combined:
        wc = WordCloud(width=800, height=400, background_color='white').generate(text_combined)
        plt.figure(figsize=(10,5))
        plt.imshow(wc, interpolation='bilinear')
        plt.axis('off')
        output_wc = os.path.join(DATA_DIR, 'wordcloud_final.png')
        plt.savefig(output_wc)
        print(f"   ✓ Đã lưu WordCloud: {output_wc}")
    else:
        print("   ⚠️ Không đủ dữ liệu để tạo WordCloud.")

    print("\n✅ Xong! Ông lấy 2 ảnh sentiment_final.png và wordcloud_final.png trong thư mục data dán báo cáo nhé.")

if __name__ == "__main__":
    main()