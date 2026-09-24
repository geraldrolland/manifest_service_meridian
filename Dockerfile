FROM python:3.12-slim

WORKDIR /app

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

COPY start.sh ./
RUN chmod +x /app/start.sh

EXPOSE 8002

ENTRYPOINT ["/app/start.sh"]