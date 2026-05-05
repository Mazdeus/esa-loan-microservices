# ======================
# 1. BUILD STAGE
# ======================
FROM node:18-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build   # 🔥 ini yang bikin dist/

# ======================
# 2. RUNTIME STAGE
# ======================
FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --only=production

COPY --from=builder /app/dist ./dist

CMD ["node", "dist/main.js"]