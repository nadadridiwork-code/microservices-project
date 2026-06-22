const sqlite3 = require('sqlite3').verbose();
const path = require('path');

function printTable(dbPath, tableName) {
  return new Promise((resolve) => {
    const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY, (err) => {
      if (err) {
        console.error(`Error opening database ${path.basename(dbPath)}:`, err.message);
        return resolve();
      }
    });

    db.all(`SELECT * FROM ${tableName}`, [], (err, rows) => {
      if (err) {
        if (!err.message.includes("no such table")) {
          console.error(`Error querying table ${tableName}:`, err.message);
        }
        db.close();
        return resolve();
      }

      console.log(`\n=== Table [${tableName}] in ${path.basename(dbPath)} ===`);
      if (rows.length === 0) {
        console.log("(empty table)");
      } else {
        console.table(rows);
      }
      db.close();
      resolve();
    });
  });
}

async function viewAll() {
  const root = path.join(__dirname, '..');
  
  // 1. Order Service Database
  const orderDb = path.join(root, 'order-service', 'orders.db');
  await printTable(orderDb, 'orders');
  await printTable(orderDb, 'order_items');

  // 2. Product Service Database
  const productDb = path.join(root, 'product-service', 'products.db');
  await printTable(productDb, 'products');

  // 3. Customer Service Database
  const customerDb = path.join(root, 'customer-service', 'customers.db');
  await printTable(customerDb, 'customers');
  await printTable(customerDb, 'activities');
}

viewAll();
