FROM node:18-alpine

WORKDIR /usr/src/app

COPY package.json ./
COPY shared/ ./shared/
COPY api-gateway/ ./api-gateway/

RUN npm install
RUN npm install --prefix shared
RUN npm install --prefix api-gateway

EXPOSE 4000

CMD ["node", "api-gateway/index.js"]
