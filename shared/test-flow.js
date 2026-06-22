/**
 * End-to-End Test Suite for Microservices Project
 * Demonstrates both REST and GraphQL flows.
 * Uses global fetch (available in Node.js 18+).
 */

const GATEWAY_URL = 'http://localhost:4000';
const GRAPHQL_URL = 'http://localhost:4000/graphql';

// Helper for delay
const delay = ms => new Promise(res => setTimeout(res, ms));

async function runTests() {
  console.log('\n======================================================');
  console.log('🚀 Starting Microservices Integration Test Flow');
  console.log('======================================================\n');

  try {
    // -----------------------------------------------------------------
    // 1. REST Flow: Fetch catalog and create a new product
    // -----------------------------------------------------------------
    console.log('--- Step 1: REST API (Fetch Initial Catalog) ---');
    const initialProductsRes = await fetch(`${GATEWAY_URL}/api/products`);
    const initialProducts = await initialProductsRes.json();
    console.log(`[REST] Retrieved ${initialProducts.length} products:`);
    initialProducts.forEach(p => console.log(`  - ID: ${p.id} | Name: ${p.name} | Price: $${p.price} | Stock: ${p.stock}`));

    console.log('\n--- Step 2: REST API (Create New Product) ---');
    const newProductPayload = {
      name: 'Wireless Mechanical Keyboard',
      price: 129.99,
      stock: 25,
      description: 'RGB mechanical keyboard with red switches.'
    };
    const createProdRes = await fetch(`${GATEWAY_URL}/api/products`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newProductPayload)
    });
    const newProduct = await createProdRes.json();
    console.log('[REST] Product Created Successfully:');
    console.log(`  - ID: ${newProduct.id} | Name: ${newProduct.name} | Price: $${newProduct.price} | Stock: ${newProduct.stock}`);
    
    // -----------------------------------------------------------------
    // 2. REST Flow: Register a customer
    // -----------------------------------------------------------------
    console.log('\n--- Step 3: REST API (Create Customer) ---');
    const newCustomerPayload = {
      name: 'Alice Cooper',
      email: 'alice@example.com'
    };
    const createCustRes = await fetch(`${GATEWAY_URL}/api/customers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newCustomerPayload)
    });
    const newCustomer = await createCustRes.json();
    console.log('[REST] Customer Created Successfully:');
    console.log(`  - ID: ${newCustomer.id} | Name: ${newCustomer.name} | Email: ${newCustomer.email} | Points: ${newCustomer.loyalty_points}`);

    // -----------------------------------------------------------------
    // 3. REST Flow: Place an order
    // -----------------------------------------------------------------
    console.log('\n--- Step 4: REST API (Place Order - Customer Buys Products) ---');
    // Let's buy 2 of prod-001 (Smartphone Pro, stock was 15) and 1 of the new keyboard
    const orderPayload = {
      customer_id: newCustomer.id,
      items: [
        { product_id: 'prod-001', quantity: 2 },
        { product_id: newProduct.id, quantity: 1 }
      ]
    };
    console.log('[REST] Submitting Order Payload:', JSON.stringify(orderPayload));
    const createOrderRes = await fetch(`${GATEWAY_URL}/api/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(orderPayload)
    });
    const orderResult = await createOrderRes.json();
    if (orderResult.error) {
      throw new Error(`Order placement failed: ${orderResult.error}`);
    }
    console.log('[REST] Order Created Successfully:');
    console.log(`  - Order ID: ${orderResult.id}`);
    console.log(`  - Total Price: $${orderResult.total_price}`);
    console.log(`  - Status: ${orderResult.status}`);
    
    // Wait a brief moment to allow Kafka messages to propagate to other services
    console.log('\n⏱ Waiting 2 seconds for Kafka event propagation...');
    await delay(2000);

    // -----------------------------------------------------------------
    // 4. GraphQL Flow: Query stock levels and customer loyalty points
    // -----------------------------------------------------------------
    console.log('\n--- Step 5: GraphQL Query (Fetch Product Stock Levels) ---');
    const gqlProductsQuery = {
      query: `
        query GetProducts {
          products {
            id
            name
            stock
          }
        }
      `
    };
    const gqlProductsRes = await fetch(GRAPHQL_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(gqlProductsQuery)
    });
    const gqlProductsData = await gqlProductsRes.json();
    console.log('[GraphQL] Product Stock Levels Post-Order:');
    gqlProductsData.data.products.forEach(p => {
      console.log(`  - ID: ${p.id} | Name: ${p.name} | Remaining Stock: ${p.stock}`);
    });

    console.log('\n--- Step 6: GraphQL Query (Fetch Customer Points & History) ---');
    const gqlCustomerQuery = {
      query: `
        query GetCustomerDetails($id: ID!, $customerId: ID!) {
          customer(id: $id) {
            id
            name
            loyalty_points
          }
          customerActivities(customerId: $customerId) {
            id
            activity_type
            description
            timestamp
          }
        }
      `,
      variables: {
        id: newCustomer.id,
        customerId: newCustomer.id
      }
    };
    const gqlCustomerRes = await fetch(GRAPHQL_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(gqlCustomerQuery)
    });
    const gqlCustomerData = await gqlCustomerRes.json();
    const customerInfo = gqlCustomerData.data.customer;
    const activities = gqlCustomerData.data.customerActivities;

    console.log('[GraphQL] Customer Info:');
    console.log(`  - Name: ${customerInfo.name}`);
    console.log(`  - Updated Loyalty Points: ${customerInfo.loyalty_points} (Awarded 1 point per $10 spent)`);
    console.log('[GraphQL] Customer Activity Logs (from SQLite3 DB):');
    activities.forEach(act => {
      console.log(`  - [${act.activity_type}] ${act.description} at ${act.timestamp}`);
    });

    console.log('\n======================================================');
    console.log('✅ End-to-End Microservices Integration Test Success!');
    console.log('======================================================\n');

  } catch (err) {
    console.error('\n❌ Test Flow Failed:', err.message);
  }
}

runTests();
