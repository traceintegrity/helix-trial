FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

VOLUME ["/data"]

ENTRYPOINT ["python", "docker_trial_entrypoint.py"]
