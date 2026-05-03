# Fidato MIS Tracker — Dockerfile for Railway
# Provides Chromium + all deps needed by Puppeteer (image rendering)
# AND whatsapp-web.js (which also uses Puppeteer internally).

FROM node:20-slim

# Install Chromium and the libraries it needs to run headless
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
    ca-certificates \
    wget \
    --no-install-recommends \
  && rm -rf /var/lib/apt/lists/*

# Tell Puppeteer to use the system Chromium (don't try to download its own)
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_SKIP_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

# Install dependencies
COPY package.json ./
RUN npm install --omit=optional --no-fund --no-audit

# Copy app source
COPY . .

# Railway sets PORT
ENV NODE_ENV=production
EXPOSE 3000

CMD ["node", "server.js"]
