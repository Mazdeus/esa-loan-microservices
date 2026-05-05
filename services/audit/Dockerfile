FROM node:18-alpine
WORKDIR /app
COPY . .
RUN npm install
WORKDIR /app/services/audit
RUN npm install
RUN npm run build
EXPOSE 3010
CMD ["node", "dist/services/audit/src/main.js"]
