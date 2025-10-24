
# FRESHER DATA ENGINEER TEST

Bối cảnh team Data Engineer đang phải xây dựng một báo cáo có khả năng cập nhật metric realtime giúp cho các nhà hoạch định chiến lược nhanh chóng phát hiện vấn đề và đưa ra các quyết định chính xác, kịp thời.
Trong thử thách Maven Sales, team DE sẽ với vai trò là một Nhà phát triển BI cho MavenTech, một công ty chuyên bán phần cứng máy tính cho các doanh nghiệp lớn. Họ đã sử dụng một hệ thống CRM mới để theo dõi các cơ hội bán hàng nhưng chưa có cái nhìn tổng quan về dữ liệu ngoài nền tảng này.


## Dependencies

uv python
```bash
 uv venv

 uv sync
```
## Deployment

To deploy this project run

```bash
  sudo docker network create tit_test_de
```

```bash
  sudo docker compose up -d

  sudo docker compose -f docker-compose.airflow.yaml up -d
```
Send connect
```bash
  chmod +x ./connect.sh

  ./connect.sh
```
Giả lập load stream và map data sang warehouse
```bash
  chmod +x ./run_mapping.sh
  ./run_mapping.sh
```

```bash
  uv run scripts/mapping_schema.py
```
