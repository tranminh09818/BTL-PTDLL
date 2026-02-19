# BTL - Phân tích cảm xúc người dùng mxh TikTok

## Mục tiêu
Phân tích cảm xúc (sentiment analysis) từ comments dưới 3 video TikTok kênh @rixi404, dùng dữ liệu từ Apify scraper.

## Quy trình (theo chu trình môn học)
1. Thu thập dữ liệu (Lecture 2): Apify TikTok Scraper → JSON metadata → extract/fetch comments → `comments_only.csv`.
2. Làm sạch & chuẩn hóa (Lecture 3): Clean missing/duplicates, giữ emoji/multilingual, thêm features (text_length, emoji_sent_score, sentiment_simple).
3. EDA & Trực quan hóa (Lecture 4-6): Stats (mean length, top words, phân bố sentiment), chart (histogram, bar sentiment, wordcloud, scatter).
4. Modeling (Lecture 7): Sentiment classification (rule-based) và Hồi quy tuyến tính (Linear Regression) để dự đoán số like.

## Cách chạy
- Cài thư viện: `pip install pandas matplotlib wordcloud emoji underthesea scikit-learn`
- Chạy: `python analyze_comments.py`
- Output:
  - `data/comments_analyzed.csv` (data sạch + features)
  - `chart/` (5 file png: bao gồm biểu đồ hồi quy dự đoán like)

## 🐳 Chạy bằng Docker

1. **Build image:**
   ```bash
   docker build -t btl-tiktok-sentiment .
   ```
2. **Run container:**
   ```bash
   docker run -v ${PWD}/chart:/app/chart btl-tiktok-sentiment
   ```
   *(Lệnh trên sẽ chạy phân tích và lưu biểu đồ vào thư mục `chart/` trên máy thật)*

## Insight chính từ EDA
- Neutral chiếm ~87% (comment trung lập hoặc ngắn).
- Positive ~8.1% > Negative ~4.7% → xu hướng tích cực từ emoji và nội dung video.
- Top từ phổ biến: 'i', 'a', 'you', 'the', 'to'... (mixed tiếng Anh + tiếng Việt, emoji góp phần lớn positive).

## Thành viên
- [TRẦN HOÀNG MINH 671688 / Nhóm 11],
- [LÊ VĂN NGUYÊN 6666757/ Nhóm 11]

## Nguồn dữ liệu
- Apify TikTok Scraper
- Video phân tích:
  - https://vt.tiktok.com/ZSmjrStC5/
  - https://vt.tiktok.com/ZSmjrrewk/
  - https://vt.tiktok.com/ZSmjrH41n/
*Bài tập lớn môn Phân tích dữ liệu lớn (PTDLL)*
