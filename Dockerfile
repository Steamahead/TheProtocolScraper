# Start with the official Azure Functions Python base image
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Set versions for Chrome and Chromedriver for stability
ENV CHROME_VERSION="126.0.6478.126"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    ca-certificates \
    fonts-liberation \
    libu2f-udev \
    libvulkan1 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Download, install, and link Google Chrome
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip -O /tmp/chrome.zip \
    && unzip /tmp/chrome.zip -d /opt/ \
    && rm /tmp/chrome.zip \
    && ln -s /opt/chrome-linux64/chrome /usr/bin/google-chrome

# Download, install, and link Chromedriver
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chromedriver-linux64.zip -O /tmp/chromedriver.zip \
    && unzip /tmp/chromedriver.zip -d /opt/ \
    && rm /tmp/chromedriver.zip \
    && ln -s /opt/chromedriver-linux64/chromedriver /usr/bin/chromedriver

# Copy and install Python packages
ENV AzureWebJobsScriptRoot=/home/site/wwwroot
WORKDIR /home/site/wwwroot
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your function app code
COPY . .
