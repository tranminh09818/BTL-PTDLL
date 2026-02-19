# Sử dụng Python 3.9 slim (phiên bản nhẹ)
FROM python:3.9-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy file requirements và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code dự án vào container
COPY . .

# Tạo các thư mục output (để tránh lỗi nếu chưa có)
RUN mkdir -p data chart

# Lệnh mặc định khi chạy container: chạy file phân tích chính
CMD ["python", "analyze_comments.py"]