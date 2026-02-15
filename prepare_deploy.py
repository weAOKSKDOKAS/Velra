import os

# Dockerfile Multi-Stage (Build React -> Serve)
dockerfile = """
# --- STAGE 1: BUILD ---
FROM node:18-slim AS builder
WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy source code & Build
COPY . .
# Ini perintah sakti untuk mengubah kode React jadi website
RUN npm run build

# --- STAGE 2: PRODUCTION SERVER ---
FROM node:18-slim
WORKDIR /app

# Copy hasil build dari Stage 1
COPY --from=builder /app/dist ./dist

# Copy file backend & config
COPY package*.json ./
COPY server.js worker.js ./

# Install hanya production dependencies (biar ringan)
RUN npm install --production

# Jalankan server
CMD ["node", "server.js"]
"""

# .dockerignore (Biar upload cepat, jangan bawa sampah)
dockerignore = """
node_modules
dist
.git
.env
"""

# Tulis file
with open("Dockerfile", "w") as f:
    f.write(dockerfile.strip())

with open(".dockerignore", "w") as f:
    f.write(dockerignore.strip())

print("✅ SIAP! Dockerfile untuk React Build berhasil dibuat.")
