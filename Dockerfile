# Image du serveur MCP et de l'agent (mêmes dépendances, deux commandes différentes).
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY mcp_server ./mcp_server
COPY agent ./agent
COPY eval ./eval
COPY faults ./faults
RUN pip install --no-cache-dir -e ".[eval]"
ENV PYTHONUNBUFFERED=1
