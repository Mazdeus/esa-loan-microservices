FROM node:18

WORKDIR /app

COPY . .

WORKDIR /app/services/audit

RUN npm install
RUN npm install -D @types/express @types/node

RUN npm run build
RUN find /app/services/audit -name main.js
 
CMD ["node", "dist/services/audit/src/main.js"]