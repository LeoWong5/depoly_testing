FROM node:20-slim

# Install Python 3, pip, and make
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        make \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# --break-system-packages is required on Debian Bookworm (PEP 668)
RUN pip3 install --no-cache-dir --break-system-packages \
    flask \
    flask-cors \
    pandas \
    numpy \
    requests

# THE FIX: Copy all the files from your GitHub repo directly into the container's /app/ folder
COPY . /app/

# Working directory
WORKDIR /app/testing

EXPOSE 8080

CMD ["bash", "build_and_run_webserver.sh"]
