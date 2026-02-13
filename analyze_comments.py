# BTL IT4142 - Phân tích dữ liệu lớn (chỉ ghi trong code, không đưa lên tiêu đề biểu đồ)
import pandas as pd
import json
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import os

# --- CẤU HÌNH ĐƯỜNG DẪN ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
CHART_DIR = os.path.join(BASE_DIR, 'chart')

if os.path.isdir(DATA_DIR):
    json_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
else:
    json_files = []

FILE_JSON = os.path.join(DATA_DIR, json_files[0]) if json_files else os.path.join(DATA_DIR, 'dataset_tiktok-scraper.json')
FILE_CSV = os.path.join(DATA_DIR, 'comments_only.csv')

def main():
    print("🚀 Bắt đầu xử lý dữ liệu...")

    # --- BƯỚC 1: TÍCH HỢP DỮ LIỆU ---
    if not os.path.exists(FILE_JSON):
        print(f"❌ Không tìm thấy file JSON trong {DATA_DIR}")
        return

    if not os.path.exists(FILE_CSV):
        print(f"❌ Không tìm thấy file CSV: {FILE_CSV}")
        return

    print(f"📂 Đọc metadata từ: {os.path.basename(FILE_JSON)}")
    with open(FILE_JSON, 'r', encoding='utf-8') as f:
        meta = json.load(f)

    df_meta = pd.DataFrame(meta)[['id', 'playCount', 'diggCount']]
    df_meta = df_meta.rename(columns={'id': 'videoId'})

    print(f"📂 Đọc dữ liệu bình luận từ: {os.path.basename(FILE_CSV)}")
    df_comments = pd.read_csv(FILE_CSV)

    df_meta['videoId'] = df_meta['videoId'].astype(str)
    df_comments['videoId'] = df_comments['videoId'].astype(str)

    df = pd.merge(df_comments, df_meta, on='videoId', how='left')
    print(f"✓ Đã tích hợp {len(df)} dòng dữ liệu.")

    # --- BƯỚC 2: TIỀN XỬ LÝ ---
    print("🔄 Đang làm sạch dữ liệu...")

    def clean_pipeline(t):
        if pd.isna(t):
            return None

        t = str(t)

        if '[sticker]' in t.lower():
            return None

        t = re.sub(r'@\w+', '', t)
        t = t.lower()

        teencode = {
            'ko': 'không',
            'k': 'không',
            'đc': 'được',
            'j': 'gì',
            'khum': 'không'
        }

        for word, rep in teencode.items():
            t = re.sub(fr'\b{word}\b', rep, t)

        t = re.sub(r'[^\w\s]', '', t)
        t = re.sub(r'\s+', ' ', t)
        t = t.strip()

        return t if len(t) > 0 else None

    df['cleaned'] = df['text'].apply(clean_pipeline)

    df = df.dropna(subset=['cleaned'])
    df = df.drop_duplicates(subset=['cleaned'])

    print(f"✓ Còn lại {len(df)} dòng sau khi làm sạch.")

    # --- BƯỚC 3: PHÂN TÍCH CẢM XÚC (Keyword-based) ---
    print("🤖 Đang phân loại cảm xúc...")

    pos = ['hay', 'tuyệt', 'vui', 'thích', 'ok', 'hài', 'mèo', 'cute', 'mê']
    neg = ['dở', 'tệ', 'buồn', 'ghét', 'xấu']

    def get_sentiment(t):
        p = sum(1 for w in pos if w in t)
        n = sum(1 for w in neg if w in t)

        if p > n:
            return 'Positive'
        elif n > p:
            return 'Negative'
        else:
            return 'Neutral'

    df['sentiment'] = df['cleaned'].apply(get_sentiment)

    # Lưu file kết quả
    output_csv = os.path.join(DATA_DIR, 'comments_analyzed.csv')
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"✓ Đã lưu dữ liệu phân tích: {output_csv}")

    # --- BƯỚC 4: TRỰC QUAN HÓA ---
    os.makedirs(CHART_DIR, exist_ok=True)

    order = ['Positive', 'Neutral', 'Negative']
    labels_vi = {
        'Positive': 'Tích cực',
        'Neutral': 'Trung tính',
        'Negative': 'Tiêu cực'
    }

    colors = {
        'Positive': '#99ff99',
        'Neutral': '#66b3ff',
        'Negative': '#ff9999'
    }

    counts = df['sentiment'].value_counts()
    sizes = [counts.get(c, 0) for c in order]
    colors_list = [colors[c] for c in order]

    # --- Biểu đồ cột (tiêu đề học thuật + tổng số bình luận) ---
    print("📊 Đang tạo biểu đồ cột...")
    total = sum(sizes)

    fig, ax = plt.subplots(figsize=(8, 6))
    x_labels = [labels_vi[c] for c in order]
    bars = ax.bar(x_labels, sizes, color=colors_list)

    ax.set_title(f'Phân bố cảm xúc bình luận TikTok (Tổng: {total:,} bình luận)')
    ax.set_ylabel('Số lượng bình luận')
    ax.set_xlabel('Loại cảm xúc')

    for bar, val in zip(bars, sizes):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f'{val:,}',
                ha='center',
                va='bottom')

    output_bar = os.path.join(CHART_DIR, 'sentiment_chart.png')
    plt.tight_layout()
    plt.savefig(output_bar, dpi=150)
    plt.close()

    print(f"✓ Đã lưu: {output_bar}")

    # --- Biểu đồ phân bố sentiment theo từng video (phân tích sâu) ---
    print("📊 Đang tạo biểu đồ sentiment theo video...")
    by_video = df.groupby(['videoId', 'sentiment']).size().unstack(fill_value=0)
    by_video = by_video.reindex(columns=order, fill_value=0)
    by_video['total'] = by_video.sum(axis=1)
    by_video = by_video.sort_values('total', ascending=False).head(10)
    by_video = by_video.drop(columns=['total'])

    if len(by_video) > 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        x_pos = range(len(by_video))
        width = 0.25
        for i, sentiment in enumerate(order):
            offset = (i - 1) * width
            ax.bar([p + offset for p in x_pos], by_video[sentiment], width, label=labels_vi[sentiment], color=colors[sentiment])
        ax.set_xticks(x_pos)
        ax.set_xticklabels([f"Video {i+1}" for i in range(len(by_video))], rotation=0)
        ax.set_ylabel('Số lượng bình luận')
        ax.set_xlabel('Video')
        ax.set_title('Phân bố cảm xúc bình luận theo từng video (Top 10 video có nhiều bình luận)')
        ax.legend()
        plt.tight_layout()
        output_by_video = os.path.join(CHART_DIR, 'sentiment_by_video.png')
        plt.savefig(output_by_video, dpi=150)
        plt.close()
        print(f"✓ Đã lưu: {output_by_video}")

    # --- WordCloud (loại stopwords tiếng Việt để có ý nghĩa phân tích hơn) ---
    print("☁️ Đang tạo WordCloud...")
    stopwords_vi = {"là", "và", "của", "thì", "mà", "cái", "nhưng", "rất", "có", "được", "cho", "với", "này", "đó", "trong", "các", "những", "khi", "như", "để", "nên", "hay", "hoặc", "vì", "nếu", "thế", "ra", "lên", "xuống", "vào", "đến"}

    text_combined = " ".join(df['cleaned'])

    if text_combined:
        wc = WordCloud(
            width=800,
            height=400,
            background_color='white',
            stopwords=stopwords_vi
        ).generate(text_combined)

        plt.figure(figsize=(10, 5))
        plt.imshow(wc, interpolation='bilinear')
        plt.axis('off')
        plt.title('WordCloud từ bình luận TikTok')

        output_wc = os.path.join(CHART_DIR, 'wordcloud.png')
        plt.savefig(output_wc, dpi=150)
        plt.close()

        print(f"✓ Đã lưu: {output_wc}")
    else:
        print("⚠ Không đủ dữ liệu để tạo WordCloud.")

    print(f"\n✅ Hoàn tất! Biểu đồ nằm trong: {CHART_DIR}")

if __name__ == "__main__":
    main()
