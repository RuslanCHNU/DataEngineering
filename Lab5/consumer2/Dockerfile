FROM python:3.12
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY consumer2.py .
CMD ["python", "consumer2.py"]