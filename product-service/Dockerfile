FROM node:18-alpine

# Install build dependencies for sqlite3 compilation
RUN apk add --no-cache python3 make g++

WORKDIR /usr/src/app

COPY package.json ./
COPY shared/ ./shared/
COPY product-service/ ./product-service/

RUN npm install
RUN npm install --prefix shared
RUN npm install --prefix product-service

EXPOSE 50081

CMD ["node", "product-service/index.js"]
