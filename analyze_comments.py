# Phân tích và khám phá dữ liệu (EDA) [Lecture 4] + trực quan hóa [đầu Lecture 5+6]-> chart/ (các png EDA)
# Input: data/comments_only.csv
# Output: data/comments_analyzed.csv + chart/ (các png EDA)

from wordcloud import WordCloud
from collections import Counter
import pandas as pd
import emoji
from underthesea import word_tokenize
import re
import os
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# Tạo thư mục
os.makedirs('data', exist_ok=True)
os.makedirs('chart', exist_ok=True)

input_file = 'data/comments_only.csv'
analyzed_file = 'data/comments_analyzed.csv'

# Cấu hình font tiếng Việt cho Matplotlib (Windows)
plt.rcParams['font.family'] = 'Segoe UI'
plt.rcParams['font.sans-serif'] = ['Segoe UI', 'Arial', 'Tahoma']

# PHẦN 1: LÀM SẠCH & CHUẨN HÓA
print("BƯỚC 1: LÀM SẠCH & CHUẨN HÓA DỮ LIỆU")

try:
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    print(f"Load raw: {len(df)} comments")
except FileNotFoundError:
    print(f"Không tìm thấy {input_file}. Kiểm tra bước extract!")
    exit()

# Quality check
print("\nQuality Check:")
print("Missing:\n", df.isnull().sum())
print("Duplicates:", df.duplicated(subset=['cid', 'text'] if 'cid' in df.columns else ['text']).sum())

df = df.dropna(subset=['text'])
df = df.drop_duplicates(subset=['cid', 'text'] if 'cid' in df.columns else ['text'])
df = df.reset_index(drop=True)

# Clean text nhẹ
def clean_text_light(text):
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    return text.lower()

df['text_original'] = df['text']
df['text_clean'] = df['text'].apply(clean_text_light)

# Emoji khai thác
def extract_emojis(text):
    return [e for e in str(text) if emoji.is_emoji(e)]

def emoji_to_desc(text):
    return emoji.demojize(text, delimiters=(" ", " "))

df['emojis'] = df['text_original'].apply(extract_emojis)
df['emoji_count'] = df['emojis'].apply(len)
df['text_with_emoji_desc'] = df['text_clean'].apply(emoji_to_desc)

# Emoji score
emoji_sent_dict = {'😍':2, '❤️':2, '🥰':2, '👍':1.5, '😂':1, '🔥':1.5, '👏':1.5,
                   '😢':-1.5, '😔':-1, '😡':-2, '👎':-1.5, '🤬':-2, '😭':-1.5}

df['emoji_sent_score'] = df['emojis'].apply(lambda em: sum(emoji_sent_dict.get(e, 0) for e in em) / len(em) if em else 0)

# Tokenize & features
def tokenize_keep_emoji(text):
    return [t for t in word_tokenize(text) if len(t.strip()) > 1]

df['tokens'] = df['text_with_emoji_desc'].apply(tokenize_keep_emoji)
df['num_words'] = df['tokens'].apply(len)
df['text_length'] = df['text_clean'].str.len()
df['has_emoji'] = df['emoji_count'] > 0

# Sentiment tạm (rule-based text + emoji)
pos_words = {'hay', 'tốt', 'thích', 'vui', 'đẹp', 'tuyệt', 'good', 'love', 'awesome'}
neg_words = {'xấu', 'tệ', 'ghét', 'buồn', 'dở', 'bad', 'hate', 'shit'}

def simple_sentiment(text, emoji_score):
    words = set(text.split())
    pos = len(words & pos_words)
    neg = len(words & neg_words)
    total = (pos - neg) + emoji_score * 2
    return 'positive' if total > 1.5 else 'negative' if total < -1.5 else 'neutral'

df['sentiment_simple'] = df.apply(lambda r: simple_sentiment(r['text_clean'], r['emoji_sent_score']), axis=1)

# Filter outliers
df = df[df['text_length'] < 1500]
print(f"Sau clean & filter: {len(df)} comments")

df.to_csv(analyzed_file, index=False, encoding='utf-8-sig')
print(f"Clean xong! Lưu tại {analyzed_file}")

# PHẦN 2: EDA & TRỰC QUAN HÓA
print("\nBƯỚC 2: EDA & TRỰC QUAN HÓA")

# EDA stats
print("\nEDA Stats:")
if 'videoId' in df.columns:
    print("Comments per video:\n", df['videoId'].value_counts())
if 'likes' in df.columns:
    print("Mean likes:", df['likes'].mean())
print("Mean text length:", df['text_length'].mean())
print("Tỷ lệ có emoji:", df['has_emoji'].mean() * 100, "%")
print("Mean emoji sent score:", df['emoji_sent_score'].mean())
print("Phân bố sentiment tạm:", df['sentiment_simple'].value_counts(normalize=True) * 100)
print("Top 20 từ phổ biến:", Counter(' '.join(df['text_clean']).split()).most_common(20))

# Viz 1: Histogram độ dài comment
plt.figure(figsize=(8,5))
df['text_length'].hist(bins=30, color='skyblue', edgecolor='black')
plt.title('Phân bố Độ dài Comment')
plt.xlabel('Số ký tự trong comment')
plt.ylabel('Số lượng comment')
plt.grid(True, alpha=0.3)
plt.savefig('chart/eda_text_length_hist.png', dpi=300, bbox_inches='tight')
plt.close()

# Viz 2: Bar chart phân bố sentiment (sửa: xóa Lecture, thêm chú thích % + caption)
sentiment_counts = df['sentiment_simple'].value_counts()
sentiment_prop = df['sentiment_simple'].value_counts(normalize=True) * 100

# Định nghĩa màu sắc chuẩn cho từng loại cảm xúc
color_map = {'positive': 'green', 'neutral': 'gray', 'negative': 'red'}
colors = [color_map.get(x, 'blue') for x in sentiment_counts.index]

fig, ax = plt.subplots(figsize=(8,6))
bars = ax.bar(sentiment_counts.index, sentiment_counts.values, color=colors, edgecolor='black')

# Thêm bảng chú thích (Legend)
legend_elements = [Patch(facecolor='green', edgecolor='black', label='Tích cực (Positive)'),
                   Patch(facecolor='gray', edgecolor='black', label='Trung tính (Neutral)'),
                   Patch(facecolor='red', edgecolor='black', label='Tiêu cực (Negative)')]
ax.legend(handles=legend_elements, title="Loại cảm xúc")

# Chú thích % và số lượng trên cột
for bar, count, prop in zip(bars, sentiment_counts, sentiment_prop):
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, height + 50,
            f'{prop:.1f}% ({int(count)})', ha='center', va='bottom', fontsize=11, fontweight='bold')

ax.set_title('Phân bố Cảm xúc từ Comments TikTok')
ax.set_xlabel('Loại cảm xúc')
ax.set_ylabel('Số lượng comment')
ax.grid(axis='y', alpha=0.3)

# Caption insight dưới biểu đồ
plt.figtext(0.5, 0.01, 'Insight: Neutral chiếm đa số (comment ngắn hoặc trung lập). Positive nhiều hơn Negative, có thể do nội dung video tích cực và emoji hỗ trợ.', 
            ha='center', fontsize=10, color='gray', wrap=True)

plt.tight_layout(rect=[0, 0.05, 1, 0.95])
plt.savefig('chart/eda_sentiment_bar.png', dpi=300, bbox_inches='tight')
plt.close()

# Viz 3: WordCloud (sửa font cho tiếng Việt đẹp)
print("Đang tạo WordCloud...")
font_paths = [
    r'C:\Windows\Fonts\tahoma.ttf',
    r'C:\Windows\Fonts\arial.ttf',
    r'C:\Windows\Fonts\times.ttf',
    r'C:\Windows\Fonts\segoeui.ttf'
]
font_path = next((p for p in font_paths if os.path.exists(p)), None)

wordcloud = WordCloud(
    width=800, height=400,
    background_color='white',
    max_words=100,
    font_path=font_path,
    stopwords=['là', 'của', 'và', 'thì', 'mà', 'có', 'không', 'được', 'một', 'như']
).generate(' '.join(df['text_clean'].astype(str)))

plt.figure(figsize=(10,5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('WordCloud Các Từ Phổ Biến Trong Comments')
plt.savefig('chart/eda_wordcloud.png', dpi=300, bbox_inches='tight')
plt.close()

# Viz 4: Scatter likes vs text_length (nếu có)
if 'likes' in df.columns:
    plt.figure(figsize=(8,5))
    scatter = plt.scatter(df['text_length'], df['likes'], alpha=0.6, c=df['emoji_sent_score'], cmap='coolwarm', edgecolor='gray')
    plt.title('Mối quan hệ giữa Độ dài Comment và Số Likes')
    plt.xlabel('Độ dài comment (ký tự)')
    plt.ylabel('Số likes')
    plt.colorbar(scatter, label='Emoji Sentiment Score (cao = tích cực)')
    plt.grid(True, alpha=0.3)
    plt.savefig('chart/eda_likes_vs_length_scatter.png', dpi=300, bbox_inches='tight')
    plt.close()
else:
    print("Không có cột 'likes' → bỏ qua scatter.")

print("\nHoàn thành! Charts đã lưu trong chart.")

# PHẦN 3: MODELING - Hồi quy tuyến tính (Lecture 7)
print("\nBƯỚC 3: MODELING - Hồi quy tuyến tính (Lecture 7)")

if 'likes' in df.columns:
    X = df[['text_length', 'emoji_sent_score', 'num_words']].fillna(0)
    y = df['likes']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rss = mse * len(y_test)

    print(f"RSS (Loss thực nghiệm): {rss:.2f}")
    print(f"MSE: {mse:.2f}")
    print("Hệ số w (coefficients):", model.coef_)
    print("Intercept w0:", model.intercept_)
else:
    print("Không có cột 'likes' → bỏ qua hồi quy.")
