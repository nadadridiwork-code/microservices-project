const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const sqlite3 = require('sqlite3').verbose();
const { Kafka } = require('../shared/kafka/kafka-client');

const ORDER_PROTO_PATH = path.join(__dirname, '../shared/protos/order.proto');
const PRODUCT_PROTO_PATH = path.join(__dirname, '../shared/protos/product.proto');
const DB_PATH = path.join(__dirname, 'orders.db');
const PORT = process.env.ORDER_SERVICE_PORT || 50082;
const PRODUCT_SERVICE_ADDR = process.env.PRODUCT_SERVICE_ADDR || 'localhost:50081';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

// 1. Initialize SQLite Database
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('[Order Service] DB connection error:', err.message);
  } else {
    console.log('[Order Service] Connected to orders.db SQLite database');
    initializeDb();
  }
});

function dbRun(query, params = []) {
  return new Promise((resolve, reject) => {
    db.run(query, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function dbGet(query, params = []) {
  return new Promise((resolve, reject) => {
    db.get(query, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function dbAll(query, params = []) {
  return new Promise((resolve, reject) => {
    db.all(query, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

async function initializeDb() {
  await dbRun(`
    CREATE TABLE IF NOT EXISTS orders (
      id TEXT PRIMARY KEY,
      customer_id TEXT NOT NULL,
      total_price REAL NOT NULL,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS order_items (
      order_id TEXT NOT NULL,
      product_id TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      price REAL NOT NULL,
      FOREIGN KEY(order_id) REFERENCES orders(id)
    )
  `);
}

// 2. Initialize gRPC Client for Product Service
const productPkgDef = protoLoader.loadSync(PRODUCT_PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const productProto = grpc.loadPackageDefinition(productPkgDef).product;
const productClient = new productProto.ProductService(PRODUCT_SERVICE_ADDR, grpc.credentials.createInsecure());

function getProductInfo(productId) {
  return new Promise((resolve, reject) => {
    productClient.GetProduct({ id: productId }, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

function updateProductStock(productId, quantityChange) {
  return new Promise((resolve, reject) => {
    productClient.UpdateStock({ id: productId, quantity_change: quantityChange }, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

// 3. Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'order-service',
  brokers: [KAFKA_BROKER]
});
const producer = kafka.producer();

async function initKafka() {
  const maxRetries = 10;
  let retryCount = 0;
  while (retryCount < maxRetries) {
    try {
      await producer.connect();
      console.log('[Order Service] Kafka integration initialized.');
      return;
    } catch (err) {
      retryCount++;
      console.warn(`[Order Service] Kafka init failed (attempt ${retryCount}/${maxRetries}): ${err.message}. Retrying in 5s...`);
      await new Promise(res => setTimeout(res, 5000));
    }
  }
  console.error('[Order Service] Kafka init failed permanently after max retries.');
}

// 4. Setup gRPC Handlers
const orderPkgDef = protoLoader.loadSync(ORDER_PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const orderProto = grpc.loadPackageDefinition(orderPkgDef).order;

const gRPCMethods = {
  CreateOrder: async (call, callback) => {
    const { customer_id, items } = call.request;

    if (!items || items.length === 0) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        details: 'Order must contain at least one item'
      });
    }

    try {
      const orderId = `order-${Date.now()}`;
      let totalPrice = 0;
      const orderItems = [];

      // Step 1: Validate each product, its stock, and fetch price via gRPC
      for (const item of items) {
        try {
          const product = await getProductInfo(item.product_id);
          if (product.stock < item.quantity) {
            return callback({
              code: grpc.status.FAILED_PRECONDITION,
              details: `Insufficient stock for product ${item.product_id} (${product.name}). Requested: ${item.quantity}, Available: ${product.stock}`
            });
          }

          const price = product.price;
          totalPrice += price * item.quantity;
          orderItems.push({
            productId: item.product_id,
            quantity: item.quantity,
            price
          });
        } catch (err) {
          console.error(`[Order Service] Failed to retrieve product ${item.product_id}:`, err.message);
          return callback({
            code: grpc.status.NOT_FOUND,
            details: `Product ${item.product_id} not found or product service is offline`
          });
        }
      }

      // Step 2: Insert Order into SQLite database
      const createdAt = new Date().toISOString();
      const status = 'PENDING';
      
      await dbRun(
        'INSERT INTO orders (id, customer_id, total_price, status, created_at) VALUES (?, ?, ?, ?, ?)',
        [orderId, customer_id, totalPrice, status, createdAt]
      );

      for (const item of orderItems) {
        await dbRun(
          'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)',
          [orderId, item.productId, item.quantity, item.price]
        );
      }

      const createdOrder = {
        id: orderId,
        customer_id,
        items: orderItems.map(item => ({
          product_id: item.productId,
          quantity: item.quantity,
          price: item.price
        })),
        total_price: totalPrice,
        status,
        created_at: createdAt
      };

      // Step 4: Publish Kafka Event
      await producer.send({
        topic: 'order-events',
        messages: [{
          value: JSON.stringify({
            type: 'OrderCreated',
            timestamp: createdAt,
            data: createdOrder
          })
        }]
      });

      console.log('[Order Service] Successfully created order and dispatched Kafka event:', orderId);
      callback(null, createdOrder);

    } catch (err) {
      console.error('[Order Service] Order creation failed:', err);
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  },

  GetOrder: async (call, callback) => {
    try {
      const order = await dbGet('SELECT * FROM orders WHERE id = ?', [call.request.id]);
      if (!order) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: `Order with ID ${call.request.id} not found`
        });
      }

      const items = await dbAll('SELECT * FROM order_items WHERE order_id = ?', [call.request.id]);
      const mappedItems = items.map((item) => ({
        product_id: item.product_id,
        quantity: item.quantity,
        price: item.price
      }));

      callback(null, {
        id: order.id,
        customer_id: order.customer_id,
        items: mappedItems,
        total_price: order.total_price,
        status: order.status,
        created_at: order.created_at
      });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  },

  ListOrders: async (call, callback) => {
    try {
      const orders = await dbAll('SELECT * FROM orders');
      const populatedOrders = [];

      for (const order of orders) {
        const items = await dbAll('SELECT * FROM order_items WHERE order_id = ?', [order.id]);
        const mappedItems = items.map((item) => ({
          product_id: item.product_id,
          quantity: item.quantity,
          price: item.price
        }));

        populatedOrders.push({
          id: order.id,
          customer_id: order.customer_id,
          items: mappedItems,
          total_price: order.total_price,
          status: order.status,
          created_at: order.created_at
        });
      }

      callback(null, { orders: populatedOrders });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  }
};

// Start Server
const server = new grpc.Server();
server.addService(orderProto.OrderService.service, gRPCMethods);
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error('[Order Service] Server bind failed:', err.message);
  } else {
    console.log(`[Order Service] gRPC running on port ${port}`);
    initKafka();
  }
});
