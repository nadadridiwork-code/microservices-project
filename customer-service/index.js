const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const sqlite3 = require('sqlite3').verbose();
const { Kafka } = require('../shared/kafka/kafka-client');

const PROTO_PATH = path.join(__dirname, '../shared/protos/customer.proto');
const DB_PATH = path.join(__dirname, 'customers.db');
const PORT = process.env.CUSTOMER_SERVICE_PORT || 50083;
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

// 1. Initialize SQLite Database
const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) {
    console.error('[Customer Service] DB connection error:', err.message);
  } else {
    console.log('[Customer Service] Connected to customers.db SQLite database');
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
    CREATE TABLE IF NOT EXISTS customers (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      loyalty_points INTEGER NOT NULL
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS activities (
      id TEXT PRIMARY KEY,
      customer_id TEXT NOT NULL,
      activity_type TEXT NOT NULL,
      description TEXT NOT NULL,
      timestamp TEXT NOT NULL
    )
  `);

  // Seed default data if empty
  const countRow = await dbGet('SELECT COUNT(*) as count FROM customers');
  if (countRow.count === 0) {
    console.log('[Customer Service] Seeding default customers...');
    const defaultCustomers = [
      { id: 'cust-001', name: 'John Doe', email: 'john.doe@example.com', loyalty_points: 100 },
      { id: 'cust-002', name: 'Jane Smith', email: 'jane.smith@example.com', loyalty_points: 250 }
    ];
    for (const c of defaultCustomers) {
      await dbRun(
        'INSERT INTO customers (id, name, email, loyalty_points) VALUES (?, ?, ?, ?)',
        [c.id, c.name, c.email, c.loyalty_points]
      );
    }
  }
}

// 2. Initialize Kafka integration
const kafka = new Kafka({
  clientId: 'customer-service',
  brokers: [KAFKA_BROKER]
});
const consumer = kafka.consumer({ groupId: 'customer-group' });

async function initKafka() {
  const maxRetries = 10;
  let retryCount = 0;
  while (retryCount < maxRetries) {
    try {
      await consumer.connect();
      // Subscribe to both order-events and product-events
      await consumer.subscribe({ topic: 'order-events', fromBeginning: true });
      await consumer.subscribe({ topic: 'product-events', fromBeginning: true });

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const event = JSON.parse(message.value.toString());
          console.log(`[Customer Service] Received Kafka event [${topic}]:`, event);

          if (topic === 'order-events' && event.type === 'OrderCreated') {
            await handleOrderCreatedEvent(event.data);
          } else if (topic === 'product-events' && event.type === 'ProductLowStock') {
            await handleProductLowStockEvent(event.data);
          }
        }
      });
      console.log('[Customer Service] Kafka integration initialized.');
      return;
    } catch (err) {
      retryCount++;
      console.warn(`[Customer Service] Kafka init failed (attempt ${retryCount}/${maxRetries}): ${err.message}. Retrying in 5s...`);
      await new Promise(res => setTimeout(res, 5000));
    }
  }
  console.error('[Customer Service] Kafka init failed permanently after max retries.');
}

async function handleOrderCreatedEvent(orderData) {
  const { customer_id, total_price, id: orderId } = orderData;
  try {
    // 1. Fetch customer from SQLite
    const customer = await dbGet('SELECT * FROM customers WHERE id = ?', [customer_id]);
    if (!customer) {
      console.warn(`[Customer Service] Received order for unknown customer: ${customer_id}`);
      return;
    }

    // 2. Award loyalty points (1 point per $10 spent)
    const pointsToAward = Math.round(total_price / 10);
    const updatedPoints = customer.loyalty_points + pointsToAward;

    await dbRun('UPDATE customers SET loyalty_points = ? WHERE id = ?', [updatedPoints, customer_id]);
    console.log(`[Customer Service] Updated loyalty points for customer ${customer_id}: +${pointsToAward} (Total: ${updatedPoints})`);

    // 3. Log customer activity in SQLite
    const activityId = `act-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`;
    await dbRun(
      'INSERT INTO activities (id, customer_id, activity_type, description, timestamp) VALUES (?, ?, ?, ?, ?)',
      [
        activityId,
        customer_id,
        'ORDER_PLACED',
        `Placed order ${orderId} totaling $${total_price.toFixed(2)}. Awarded ${pointsToAward} loyalty points.`,
        new Date().toISOString()
      ]
    );
  } catch (err) {
    console.error('[Customer Service] Failed to handle OrderCreated event:', err.message);
  }
}

async function handleProductLowStockEvent(productData) {
  const { productId, name, remainingStock } = productData;
  try {
    // Log administrative activity alert in customer service
    console.log(`[Customer Service] Logging low stock event for ${name}`);
    const activityId = `act-${Date.now()}-${Math.random().toString(36).substr(2, 4)}`;
    await dbRun(
      'INSERT INTO activities (id, customer_id, activity_type, description, timestamp) VALUES (?, ?, ?, ?, ?)',
      [
        activityId,
        'SYSTEM_ADMIN',
        'LOW_STOCK_WARNING',
        `Product ${name} (${productId}) is running low on stock. Remaining: ${remainingStock}.`,
        new Date().toISOString()
      ]
    );
  } catch (err) {
    console.error('[Customer Service] Failed to handle ProductLowStock event:', err.message);
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
const customerProto = grpc.loadPackageDefinition(packageDefinition).customer;

const gRPCMethods = {
  GetCustomer: async (call, callback) => {
    try {
      const row = await dbGet('SELECT * FROM customers WHERE id = ?', [call.request.id]);
      if (row) {
        callback(null, row);
      } else {
        callback({
          code: grpc.status.NOT_FOUND,
          details: `Customer with ID ${call.request.id} not found`
        });
      }
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  },

  CreateCustomer: async (call, callback) => {
    const { name, email } = call.request;
    const id = `cust-${Date.now()}`;
    try {
      await dbRun(
        'INSERT INTO customers (id, name, email, loyalty_points) VALUES (?, ?, ?, 0)',
        [id, name, email]
      );
      console.log(`[Customer Service] Registered new customer: ${name}`);
      callback(null, { id, name, email, loyalty_points: 0 });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  },

  GetActivityLogs: async (call, callback) => {
    try {
      const rows = await dbAll(
        'SELECT * FROM activities WHERE customer_id = ? ORDER BY timestamp DESC',
        [call.request.customer_id]
      );
      callback(null, { logs: rows });
    } catch (err) {
      callback({ code: grpc.status.INTERNAL, details: err.message });
    }
  }
};

// Main Startup
async function start() {
  const server = new grpc.Server();
  server.addService(customerProto.CustomerService.service, gRPCMethods);
  
  server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('[Customer Service] Server bind failed:', err.message);
    } else {
      console.log(`[Customer Service] gRPC running on port ${port}`);
      initKafka();
    }
  });
}

start().catch(err => {
  console.error('[Customer Service] Startup crash:', err);
});
