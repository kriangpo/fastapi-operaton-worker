# ใช้ image ฐาน Python เดียวกัน
FROM python:3.11-slim

# ตั้งค่า working directory ใน container
WORKDIR /app

# ติดตั้ง requests (สำหรับ Worker)
RUN pip install requests

# คัดลอกโค้ด worker
COPY worker_service.py .

# สั่งให้รัน Worker Script เมื่อ container เริ่มต้น
CMD ["python", "worker_service.py"]
