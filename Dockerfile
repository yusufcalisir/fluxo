FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY pyproject.toml README.md ./
COPY src/ src/

# Install the package and dependencies
RUN pip install --no-cache-dir .

# Create a project directory for mounting user projects
WORKDIR /project

# Expose Streamlit and API ports
EXPOSE 8501
EXPOSE 8000

# Default command
ENTRYPOINT ["fluxo"]
CMD ["--help"]
