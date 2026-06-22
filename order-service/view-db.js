const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(__dirname, 'orders.db');
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error('Error connecting to orders.db:', err.message);
    process.exit(1);
  }
});

console.log('--- TABLES IN orders.db ---');

db.all('SELECT * FROM orders', [], (err, rows) => {
  if (err) throw err;
  console.log('\n--- ORDERS ---');
  console.table(rows);

  db.all('SELECT * FROM order_items', [], (err, items) => {
    if (err) throw err;
    console.log('\n--- ORDER ITEMS ---');
    console.table(items);
    
    db.close();
  });
});
