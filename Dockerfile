# Start with the official Azure Functions Python base image
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Install system dependencies for Chrome, chromedriver, and jq
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Download and install Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list
RUN apt-get update && apt-get install -y google-chrome-stable

# Download and install the matching chromedriver
# This command now uses jq to parse the correct driver version
RUN LATEST_DRIVER_URL=$(wget -qO- "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json" | jq -r ".channels.Stable.downloads.chromedriver[] | select(.platform==\"linux-x64\") | .url") && \
    wget -q "$LATEST_DRIVER_URL" -O /tmp/chromedriver.zip && \
    unzip /tmp/chromedriver.zip -d /usr/bin && \
    mv /usr/bin/chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip /usr/bin/chromedriver-linux64

# Copy your requirements.txt and install Python packages
ENV AzureWebJobsScriptRoot=/home/site/wwwroot
WORKDIR /home/site/wwwroot
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy your function app code
COPY . .
