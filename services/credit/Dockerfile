FROM node:18-alpine
WORKDIR /app
COPY . .
RUN npm install
WORKDIR /app/services/credit
RUN npm install
RUN npm run build
CMD ["node", "dist/services/credit/src/main.js"]
