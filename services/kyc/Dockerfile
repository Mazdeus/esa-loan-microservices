FROM node:18-alpine
WORKDIR /app
COPY . .
RUN npm install
WORKDIR /app/services/kyc
RUN npm install
RUN npm run build
CMD ["node", "dist/services/kyc/src/main.js"]
