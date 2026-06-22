FROM node:18-alpine

WORKDIR /usr/src/app

COPY package.json ./
COPY shared/ ./shared/
COPY customer-service/ ./customer-service/

RUN npm install
RUN npm install --prefix shared
RUN npm install --prefix customer-service

EXPOSE 50083

CMD ["node", "customer-service/index.js"]
