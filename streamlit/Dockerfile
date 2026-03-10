FROM python:3.11-slim

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /workspace

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
