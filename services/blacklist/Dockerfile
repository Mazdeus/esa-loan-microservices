FROM node:18-alpine
WORKDIR /app
COPY . .
RUN npm install
WORKDIR /app/services/blacklist
RUN npm install
RUN npm run build
CMD ["node", "dist/services/blacklist/src/main.js"]
