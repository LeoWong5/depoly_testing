FROM node:20-slim

# 1. ADDED 'unzip' to this list
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        make \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install --no-cache-dir --break-system-packages \
    flask \
    flask-cors \
    pandas \
    numpy \
    requests

# Copy everything from GitHub into the container
COPY . /app/

# 2. UNZIP THE DATABASE
# This extracts the .db file directly into the new_database folder
RUN unzip /app/testing/new/new_database/nptg_naptan.db.zip -d /app/testing/new/new_database/

# Working directory
WORKDIR /app/testing

EXPOSE 8080

CMD ["bash", "build_and_run_webserver.sh"]
