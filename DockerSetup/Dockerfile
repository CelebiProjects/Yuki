FROM python:3.10-slim

# Install system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        rabbitmq-server \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create unprivileged user
WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt
RUN pip install --user build
RUN pip install --user celebichrono

# RUN pip install --user yuki==0.0.4
COPY Yuki /app/Yuki
WORKDIR /app/Yuki

RUN python -m build
RUN pip install --user dist/*.whl

# App files
WORKDIR /app
COPY entrypoint.sh .
# COPY --chown=appuser install.sh .
RUN chmod +x entrypoint.sh
# install.sh

# App port only (high port = rootless OK)
EXPOSE 3315

CMD ["/app/entrypoint.sh"]
