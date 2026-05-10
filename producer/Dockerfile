FROM python:3.12-slim
 
WORKDIR /app
 
RUN pip install --no-cache-dir kafka-python==2.0.2
 
COPY traffic_producer.py .
 
CMD ["python", "-u", "traffic_producer.py"]