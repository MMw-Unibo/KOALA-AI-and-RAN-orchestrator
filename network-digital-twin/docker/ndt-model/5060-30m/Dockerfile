FROM python:3.12-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY inference_pb2_grpc.py .
COPY inference_pb2.py .
COPY main.py .
COPY 1l-lstm-5060.keras ./model.keras
COPY scaler.pkl .

CMD ["python", "main.py"]