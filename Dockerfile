# Fidato MIS Tracker — Dockerfile for Railway
# v2.12.0 — engine migrated from whatsapp-web.js to WPPConnect (via wa-adapter.js).
# Provides system Chromium + the libraries it needs, used by BOTH:
#   - puppeteer (report/dashboard image rendering)
#   - WPPConnect (WhatsApp session)

FROM node:20-slim

# Chromium and its runtime libraries for headless operation
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-noto \
    fonts-freefont-ttf \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libglib2.0-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxshmfence1 \
    libxss1 \
    libxtst6 \
    libasound2 \
    ca-certificates \
    wget \
  && rm -rf /var/lib/apt/lists/*

# Use the system Chromium; never download a private copy.
# wa-adapter.js reads PUPPETEER_EXECUTABLE_PATH and passes it to WPPConnect.
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_SKIP_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    CHROME_PATH=/usr/bin/chromium

WORKDIR /app

# Install dependencies.
# NOTE: --omit=optional was REMOVED — WPPConnect needs optional deps to launch.
COPY package.json ./
RUN npm install --no-fund --no-audit

# Copy app source (server.js, wa-adapter.js, sales.js, dashboard.html, ...)
COPY . .

# Railway injects PORT
# Build cache bust: 2026-07-28-v2.12.0-wpp
ENV NODE_ENV=production
EXPOSE 3000

CMD ["node", "server.js"]
