const net = require('net');

const PORT = process.env.KAFKA_PORT || 9092;
const clients = new Set();

// Map of socket -> Set of subscribed topics
const subscriptions = new Map();

const server = net.createServer((socket) => {
  console.log(`[Kafka Broker] New connection established from ${socket.remoteAddress}:${socket.remotePort}`);
  clients.add(socket);
  subscriptions.set(socket, new Set());

  let buffer = '';

  socket.on('data', (data) => {
    buffer += data.toString();
    let boundary = buffer.indexOf('\n');
    
    while (boundary !== -1) {
      const line = buffer.substring(0, boundary).trim();
      buffer = buffer.substring(boundary + 1);
      boundary = buffer.indexOf('\n');
      
      if (line) {
        try {
          const payload = JSON.parse(line);
          handleMessage(socket, payload);
        } catch (err) {
          console.error('[Kafka Broker] Failed to parse message:', line, err.message);
        }
      }
    }
  });

  socket.on('end', () => {
    console.log('[Kafka Broker] Client disconnected');
    cleanupClient(socket);
  });

  socket.on('error', (err) => {
    console.log(`[Kafka Broker] Connection error from client: ${err.message}`);
    cleanupClient(socket);
  });
});

function cleanupClient(socket) {
  clients.delete(socket);
  subscriptions.delete(socket);
}

function handleMessage(socket, payload) {
  const { action, topic, topics, message } = payload;

  if (action === 'subscribe') {
    const subs = subscriptions.get(socket) || new Set();
    const topicList = topics || [topic];
    topicList.forEach((t) => {
      subs.add(t);
      console.log(`[Kafka Broker] Socket subscribed to topic: ${t}`);
    });
    subscriptions.set(socket, subs);
  } else if (action === 'publish') {
    console.log(`[Kafka Broker] Publish on topic [${topic}]:`, JSON.stringify(message));
    
    // Broadcast message to all subscribed sockets
    for (const client of clients) {
      const subs = subscriptions.get(client);
      if (subs && subs.has(topic)) {
        try {
          client.write(JSON.stringify({ topic, message }) + '\n');
        } catch (err) {
          console.error('[Kafka Broker] Error writing to consumer socket:', err.message);
        }
      }
    }
  } else {
    console.log('[Kafka Broker] Unknown action:', action);
  }
}

server.listen(PORT, () => {
  console.log(`[Kafka Broker] Running on port ${PORT} (TCP mock mode)`);
});
