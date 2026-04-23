FROM python:3.11-slim

# Dependencias del sistema + Microsoft ODBC Driver 18 para SQL Server
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libxml2-dev \
    curl \
    gnupg2 \
    && curl https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
       https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Java JRE (requerido por JayDeBeApi / JPype1 para el driver JT400 de IBM i)
RUN apt-get update && apt-get install -y default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# JT400 JAR — AS/400 Toolbox for Java JDBC Driver (mismo driver que DBeaver)
RUN curl -L "https://repo1.maven.org/maven2/net/sf/jt400/jt400/20.0.7/jt400-20.0.7.jar" \
    -o /opt/jt400.jar

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONPATH=src

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "3666", "--reload"]
