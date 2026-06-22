const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const sqlite3 = require('sqlite3').verbose();
const { Kafka } = require('../shared/kafka/kafka-client');

const PROTO_PATH = path.join(__dirname, '../shared/protos/product.proto');
const DB_PATH = path.join(__dirname, 'products.db');
const PORT = process.env.PRODUCT_SERVICE_PORT || 50081;
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

// 1. Initialize SQLite Database
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('[Product Service] DB connection error:', err.message);
  } else {
    console.log('[Product Service] Connected to products.db SQLite database');
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
    CREATE TABLE IF NOT EXISTS products (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      price REAL NOT NULL,
      stock INTEGER NOT NULL,
      description TEXT
    )
  `);

  // Seed default data if empty
  const countRow = await dbGet('SELECT COUNT(*) as count FROM products');
  if (countRow.count === 0) {
    console.log('[Product Service] Seeding default products...');
    const seedProducts = [
      { id: 'prod-001', name: 'Smartphone Pro', price: 999.99, stock: 15, description: 'High-end smartphone with advanced features.' },
      { id: 'prod-002', name: 'Wireless Headphones', price: 149.99, stock: 50, description: 'Active noise cancelling wireless headphones.' },
      { id: 'prod-003', name: 'Ultra-thin Laptop', price: 1299.00, stock: 8, description: 'Powerhouse thin-and-light laptop for productivity.' }
    ];
    for (const p of seedProducts) {
      await dbRun(
        'INSERT INTO products (id, name, price, stock, description) VALUES (?, ?, ?, ?, ?)',
        [p.id, p.name, p.price, p.stock, p.description]
      );
    }
  }
}

// 2. Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'product-service',
  brokers: [KAFKA_BROKER]
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'product-group' });

async function initKafka() {
  const maxRetries = 10;
  let retryCount = 0;
  while (retryCount < maxRetries) {
    try {
      await producer.connect();
      await consumer.connect();
      
      // Subscribe to order events to manage stock deduction
      await consumer.subscribe({ topic: 'order-events', fromBeginning: true });
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const event = JSON.parse(message.value.toString());
          console.log(`[Product Service] Received Kafka event [${topic}]:`, event);
          
          if (event.type === 'OrderCreated') {
            for (const item of event.data.items) {
              await handleStockDeduction(item.product_id, item.quantity);
            }
          }
        }
      });
      console.log('[Product Service] Kafka integration initialized.');
      return;
    } catch (err) {
      retryCount++;
      console.warn(`[Product Service] Kafka init failed (attempt ${retryCount}/${maxRetries}): ${err.message}. Retrying in 5s...`);
      await new Promise(res => setTimeout(res, 5000));
    }
  }
  console.error('[Product Service] Kafka init failed permanently after max retries.');
}

async function handleStockDeduction(productId, quantity) {
  try {
    const product = await dbGet('SELECT * FROM products WHERE id = ?', [productId]);
    if (!product) {
      console.error(`[Product Service] Cannot deduct stock. Product ${productId} not found.`);
      return;
    }

    const newStock = product.stock - quantity;
    await dbRun('UPDATE products SET stock = ? WHERE id = ?', [newStock, productId]);
    console.log(`[Product Service] Stock updated for ${productId}: ${product.stock} -> ${newStock}`);

    // If stock level is low, emit a Kafka warning
    if (newStock < 5) {
      console.log(`[Product Service] LOW STOCK Alert for ${productId}!`);
      await producer.send({
        topic: 'product-events',
        messages: [{
          value: JSON.stringify({
            type: 'ProductLowStock',
            timestamp: new Date().toISOString(),
            data: {
              productId,
              name: product.name,
              remainingStock: newStock
            }
          })
        }]
      });
    }
  } catch (err) {
    console.error('[Product Service] Stock deduction failed:', err.message);
  }
}

// 3. Setup gRPC Handlers
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});
const productProto = grpc.loadPackageDefinition(packageDefinition).product;

const gRPCMethods = {
  GetProduct: async (call, callback) => {
    try {
      const row = await dbGet('SELECT * FROM products WHERE id = ?', [call.request.id]);
      if (row) {
        callback(null, row);
      } else {
        callback({
          code: grpc.status.NOT_FOUND,
          details: `Product with ID ${call.request.id} not found`
        });
      }
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  },

  ListProducts: async (call, callback) => {
    try {
      const rows = await dbAll('SELECT * FROM products');
      callback(null, { products: rows });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  },

  CreateProduct: async (call, callback) => {
    const { name, price, stock, description } = call.request;
    const id = `prod-${Date.now()}`;
    try {
      await dbRun(
        'INSERT INTO products (id, name, price, stock, description) VALUES (?, ?, ?, ?, ?)',
        [id, name, price, stock, description]
      );
      callback(null, { id, name, price, stock, description });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  },

  UpdateStock: async (call, callback) => {
    const { id, quantity_change } = call.request;
    try {
      const product = await dbGet('SELECT * FROM products WHERE id = ?', [id]);
      if (!product) {
        return callback({
          code: grpc.status.NOT_FOUND,
          details: `Product ${id} not found`
        });
      }

      const newStock = product.stock + quantity_change;
      if (newStock < 0) {
        return callback({
          code: grpc.status.FAILED_PRECONDITION,
          details: `Insufficient stock for product ${id}. Current: ${product.stock}, change requested: ${quantity_change}`
        });
      }

      await dbRun('UPDATE products SET stock = ? WHERE id = ?', [newStock, id]);
      
      // Publish event
      await producer.send({
        topic: 'product-events',
        messages: [{
          value: JSON.stringify({
            type: 'ProductStockUpdated',
            timestamp: new Date().toISOString(),
            data: { id, name: product.name, stock: newStock }
          })
        }]
      });

      callback(null, { ...product, stock: newStock });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  }
};

// Start Server
const server = new grpc.Server();
server.addService(productProto.ProductService.service, gRPCMethods);
server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error('[Product Service] Server bind failed:', err.message);
  } else {
    console.log(`[Product Service] gRPC running on port ${port}`);
    initKafka();
  }
});
