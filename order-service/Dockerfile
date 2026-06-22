FROM node:18-alpine

# Install build dependencies for sqlite3 compilation
RUN apk add --no-cache python3 make g++

WORKDIR /usr/src/app

COPY package.json ./
COPY shared/ ./shared/
COPY order-service/ ./order-service/

RUN npm install
RUN npm install --prefix shared
RUN npm install --prefix order-service

EXPOSE 50082

CMD ["node", "order-service/index.js"]
