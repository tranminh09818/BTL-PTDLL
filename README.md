# PHÂN TÍCH CẢM XÚC NGƯỜI DÙNG MXH TIK TOK

Dự án này thực hiện thu thập, làm sạch, tích hợp và phân tích cảm xúc từ các bình luận trên mạng xã hội TikTok.

## 📂 Cấu trúc dự án

- `data/`: Chứa dữ liệu thô (JSON), script trích xuất và các file CSV kết quả.
- `chart/`: Chứa các biểu đồ trực quan hóa sau khi phân tích.
- `analyze_comments.py`: Script chính để làm sạch dữ liệu, phân tích cảm xúc và vẽ biểu đồ.
- `data/extract_comments.py`: Script để chuyển đổi dữ liệu thô từ JSON sang CSV.

## 🚀 Hướng dẫn cài đặt

Cài đặt các thư viện Python cần thiết:

```bash
pip install pandas requests matplotlib wordcloud emoji underthesea
```

## 📝 Hướng dẫn sử dụng

### Bước 1: Thu thập và Tích hợp dữ liệu
Chạy file `extract_comments.py` để trích xuất bình luận từ file JSON (thu thập từ Apify) và lưu vào file CSV.

```bash
python data/extract_comments.py
```
*   **Input**: File JSON trong thư mục `data/` (ví dụ: `dataset_tiktok-scraper.json`).
*   **Output**: `data/comments_only.csv`.

### Bước 2: Làm sạch, Phân tích và Trực quan hóa
Chạy file `analyze_comments.py` để xử lý dữ liệu, tính điểm cảm xúc và tạo biểu đồ.

```bash
python analyze_comments.py
```
*   **Input**: `data/comments_only.csv`.
*   **Output**:
    *   File dữ liệu đã xử lý: `data/comments_analyzed.csv`.
    *   Biểu đồ trong thư mục `chart/`:
        *   `eda_text_length_hist.png`: Phân bố độ dài bình luận.
        *   `eda_sentiment_bar.png`: Phân bố cảm xúc (Tích cực/Tiêu cực/Trung tính).
        *   `eda_wordcloud.png`: Mây từ khóa phổ biến.
        *   `eda_likes_vs_length_scatter.png`: Tương quan giữa độ dài và lượt thích.

---
*Bài tập lớn môn Phân tích dữ liệu lớn (PTDLL)*