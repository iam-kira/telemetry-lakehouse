FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# --reload for the build-it-live loop. Plain uvicorn (no [standard] extra) polls file mtimes,
# which sees edits made through a Windows bind mount; inotify-based reload does not.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
