FROM python:3.12
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY consumer1.py .
CMD ["python", "consumer1.py"]