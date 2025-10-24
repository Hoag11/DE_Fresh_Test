docker run -it --rm \
  --network tit_test_de \
  -v $(pwd):/app -w /app python:3.13 \
  bash -c "pip install kafka-python pymysql cryptography && python scripts/mapping_schema.py"

