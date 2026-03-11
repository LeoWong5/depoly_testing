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

# Working directory matches the bind-mount layout:
# host front-end/ -> container /app/
# so front-end/testing/ is at /app/testing/
WORKDIR /app/testing

EXPOSE 8080

CMD ["bash", "build_and_run_webserver.sh"]
