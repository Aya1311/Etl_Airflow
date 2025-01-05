FROM apache/airflow:2.6.0-python3.9

# Switch to root for installations
USER root

# Copy requirements file
COPY requirements.txt /
COPY dashboard.py /app/dashboard.py

# Revert back to airflow user
USER airflow

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt


