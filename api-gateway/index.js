const path = require('path');
const express = require('express');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');

const PORT = process.env.PORT || 4000;

// gRPC Client configs
const PRODUCT_SERVICE_ADDR = process.env.PRODUCT_SERVICE_ADDR || 'localhost:50081';
const ORDER_SERVICE_ADDR = process.env.ORDER_SERVICE_ADDR || 'localhost:50082';
const CUSTOMER_SERVICE_ADDR = process.env.CUSTOMER_SERVICE_ADDR || 'localhost:50083';

// Load gRPC Protos
const productPkgDef = protoLoader.loadSync(path.join(__dirname, '../shared/protos/product.proto'), { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
const orderPkgDef = protoLoader.loadSync(path.join(__dirname, '../shared/protos/order.proto'), { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
const customerPkgDef = protoLoader.loadSync(path.join(__dirname, '../shared/protos/customer.proto'), { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });

const productProto = grpc.loadPackageDefinition(productPkgDef).product;
const orderProto = grpc.loadPackageDefinition(orderPkgDef).order;
const customerProto = grpc.loadPackageDefinition(customerPkgDef).customer;

// Instantiate gRPC Clients
const productClient = new productProto.ProductService(PRODUCT_SERVICE_ADDR, grpc.credentials.createInsecure());
const orderClient = new orderProto.OrderService(ORDER_SERVICE_ADDR, grpc.credentials.createInsecure());
const customerClient = new customerProto.CustomerService(CUSTOMER_SERVICE_ADDR, grpc.credentials.createInsecure());

// Helper Promisifiers
const promProduct = {
  getProduct: (req) => new Promise((res, rej) => productClient.GetProduct(req, (e, r) => e ? rej(e) : res(r))),
  listProducts: (req) => new Promise((res, rej) => productClient.ListProducts(req, (e, r) => e ? rej(e) : res(r))),
  createProduct: (req) => new Promise((res, rej) => productClient.CreateProduct(req, (e, r) => e ? rej(e) : res(r))),
  updateStock: (req) => new Promise((res, rej) => productClient.UpdateStock(req, (e, r) => e ? rej(e) : res(r)))
};

const promOrder = {
  createOrder: (req) => new Promise((res, rej) => orderClient.CreateOrder(req, (e, r) => e ? rej(e) : res(r))),
  getOrder: (req) => new Promise((res, rej) => orderClient.GetOrder(req, (e, r) => e ? rej(e) : res(r))),
  listOrders: (req) => new Promise((res, rej) => orderClient.ListOrders(req, (e, r) => e ? rej(e) : res(r)))
};

const promCustomer = {
  getCustomer: (req) => new Promise((res, rej) => customerClient.GetCustomer(req, (e, r) => e ? rej(e) : res(r))),
  createCustomer: (req) => new Promise((res, rej) => customerClient.CreateCustomer(req, (e, r) => e ? rej(e) : res(r))),
  getActivityLogs: (req) => new Promise((res, rej) => customerClient.GetActivityLogs(req, (e, r) => e ? rej(e) : res(r)))
};

// Initialize Express
const app = express();
app.use(cors());
app.use(express.json());

// ============================================
// 1. REST Endpoints
// ============================================

// Products REST Endpoints
app.get('/api/products', async (req, res) => {
  try {
    const data = await promProduct.listProducts({});
    res.json(data.products || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/products/:id', async (req, res) => {
  try {
    const product = await promProduct.getProduct({ id: req.params.id });
    res.json(product);
  } catch (err) {
    res.status(err.code === grpc.status.NOT_FOUND ? 404 : 500).json({ error: err.details || err.message });
  }
});

app.post('/api/products', async (req, res) => {
  try {
    const { name, price, stock, description } = req.body;
    const newProduct = await promProduct.createProduct({ name, price, stock, description });
    res.status(201).json(newProduct);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.put('/api/products/:id/stock', async (req, res) => {
  try {
    const { quantity_change } = req.body;
    const updatedProduct = await promProduct.updateStock({ id: req.params.id, quantity_change });
    res.json(updatedProduct);
  } catch (err) {
    res.status(400).json({ error: err.details || err.message });
  }
});

// Orders REST Endpoints
app.post('/api/orders', async (req, res) => {
  try {
    const { customer_id, items } = req.body;
    const newOrder = await promOrder.createOrder({ customer_id, items });
    res.status(201).json(newOrder);
  } catch (err) {
    res.status(err.code === grpc.status.FAILED_PRECONDITION ? 400 : 500).json({ error: err.details || err.message });
  }
});

app.get('/api/orders/:id', async (req, res) => {
  try {
    const order = await promOrder.getOrder({ id: req.params.id });
    res.json(order);
  } catch (err) {
    res.status(err.code === grpc.status.NOT_FOUND ? 404 : 500).json({ error: err.details || err.message });
  }
});

app.get('/api/orders', async (req, res) => {
  try {
    const data = await promOrder.listOrders({});
    res.json(data.orders || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Customers REST Endpoints
app.post('/api/customers', async (req, res) => {
  try {
    const { name, email } = req.body;
    const customer = await promCustomer.createCustomer({ name, email });
    res.status(201).json(customer);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/customers/:id', async (req, res) => {
  try {
    const customer = await promCustomer.getCustomer({ id: req.params.id });
    res.json(customer);
  } catch (err) {
    res.status(err.code === grpc.status.NOT_FOUND ? 404 : 500).json({ error: err.details || err.message });
  }
});

app.get('/api/customers/:id/activities', async (req, res) => {
  try {
    const data = await promCustomer.getActivityLogs({ customer_id: req.params.id });
    res.json(data.logs || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================
// 2. GraphQL Endpoint Setup
// ============================================

const typeDefs = `#graphql
  type Product {
    id: ID!
    name: String!
    price: Float!
    stock: Int!
    description: String
  }

  type OrderItem {
    product_id: ID!
    quantity: Int!
    price: Float!
  }

  type Order {
    id: ID!
    customer_id: String!
    items: [OrderItem!]!
    total_price: Float!
    status: String!
    created_at: String!
  }

  type Customer {
    id: ID!
    name: String!
    email: String!
    loyalty_points: Int!
  }

  type ActivityLog {
    id: ID!
    customer_id: String!
    activity_type: String!
    description: String!
    timestamp: String!
  }

  input OrderItemInput {
    product_id: ID!
    quantity: Int!
  }

  type Query {
    product(id: ID!): Product
    products: [Product!]!
    order(id: ID!): Order
    orders: [Order!]!
    customer(id: ID!): Customer
    customerActivities(customerId: ID!): [ActivityLog!]!
  }

  type Mutation {
    createProduct(name: String!, price: Float!, stock: Int!, description: String): Product
    createOrder(customerId: ID!, items: [OrderItemInput!]!): Order
    createCustomer(name: String!, email: String!): Customer
  }
`;

const resolvers = {
  Query: {
    product: async (_, { id }) => promProduct.getProduct({ id }),
    products: async () => {
      const data = await promProduct.listProducts({});
      return data.products || [];
    },
    order: async (_, { id }) => promOrder.getOrder({ id }),
    orders: async () => {
      const data = await promOrder.listOrders({});
      return data.orders || [];
    },
    customer: async (_, { id }) => promCustomer.getCustomer({ id }),
    customerActivities: async (_, { customerId }) => {
      const data = await promCustomer.getActivityLogs({ customer_id: customerId });
      return data.logs || [];
    }
  },
  Mutation: {
    createProduct: async (_, { name, price, stock, description }) => {
      return promProduct.createProduct({ name, price, stock, description });
    },
    createOrder: async (_, { customerId, items }) => {
      // Map input to proto items
      const protoItems = items.map(it => ({ product_id: it.product_id, quantity: it.quantity }));
      return promOrder.createOrder({ customer_id: customerId, items: protoItems });
    },
    createCustomer: async (_, { name, email }) => {
      return promCustomer.createCustomer({ name, email });
    }
  }
};

// Apollo Server initialization
const apolloServer = new ApolloServer({
  typeDefs,
  resolvers,
});

async function startServer() {
  await apolloServer.start();
  app.use('/graphql', expressMiddleware(apolloServer));

  app.listen(PORT, () => {
    console.log(`[API Gateway] REST running at http://localhost:${PORT}`);
    console.log(`[API Gateway] GraphQL running at http://localhost:${PORT}/graphql`);
  });
}

startServer().catch(err => {
  console.error('[API Gateway] Startup crash:', err);
});
