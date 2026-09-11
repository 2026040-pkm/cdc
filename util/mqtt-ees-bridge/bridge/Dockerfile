FROM python:3.12-slim

# confluent-kafka(librdkafka 내장 wheel) · paho-mqtt · prometheus-client
# 셋 다 바이너리 휠이라 컴파일러가 필요 없다.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py check_contract.py ./

# 로그가 버퍼에 갇히지 않게
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "bridge.py"]
