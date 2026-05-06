FROM python:3.12-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY main.py .
COPY test-5060.csv ./test.csv

CMD ["python", "main.py"]