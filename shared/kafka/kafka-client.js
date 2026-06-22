const net = require('net');

class MockProducer {
  constructor(brokerAddress) {
    const [host, port] = brokerAddress.split(':');
    this.host = host || 'localhost';
    this.port = parseInt(port || '9092', 10);
    this.client = null;
    this.connected = false;
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.client = new net.Socket();
      this.client.connect(this.port, this.host, () => {
        this.connected = true;
        console.log(`[Mock Kafka Producer] Connected to broker at ${this.host}:${this.port}`);
        resolve();
      });

      this.client.on('error', (err) => {
        console.error('[Mock Kafka Producer] Socket error:', err.message);
        reject(err);
      });
    });
  }

  async send({ topic, messages }) {
    if (!this.connected) {
      throw new Error('[Mock Kafka Producer] Producer not connected. Call connect() first.');
    }
    
    for (const msg of messages) {
      const payload = {
        action: 'publish',
        topic,
        message: JSON.parse(msg.value.toString())
      };
      
      this.client.write(JSON.stringify(payload) + '\n');
    }
  }

  async disconnect() {
    if (this.client) {
      this.client.end();
      this.connected = false;
    }
  }
}

class MockConsumer {
  constructor(brokerAddress, groupId) {
    const [host, port] = brokerAddress.split(':');
    this.host = host || 'localhost';
    this.port = parseInt(port || '9092', 10);
    this.groupId = groupId;
    this.client = null;
    this.connected = false;
    this.subscribedTopics = [];
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.client = new net.Socket();
      this.client.connect(this.port, this.host, () => {
        this.connected = true;
        console.log(`[Mock Kafka Consumer] Connected to broker at ${this.host}:${this.port} (group: ${this.groupId})`);
        resolve();
      });

      this.client.on('error', (err) => {
        console.error('[Mock Kafka Consumer] Socket error:', err.message);
        reject(err);
      });
    });
  }

  async subscribe({ topic, fromBeginning }) {
    if (!this.connected) {
      throw new Error('[Mock Kafka Consumer] Consumer not connected. Call connect() first.');
    }
    this.subscribedTopics.push(topic);
    const payload = {
      action: 'subscribe',
      topic
    };
    this.client.write(JSON.stringify(payload) + '\n');
  }

  async run({ eachMessage }) {
    let buffer = '';

    this.client.on('data', (data) => {
      buffer += data.toString();
      let boundary = buffer.indexOf('\n');

      while (boundary !== -1) {
        const line = buffer.substring(0, boundary).trim();
        buffer = buffer.substring(boundary + 1);
        boundary = buffer.indexOf('\n');

        if (line) {
          try {
            const payload = JSON.parse(line);
            if (this.subscribedTopics.includes(payload.topic)) {
              const valueBuffer = Buffer.from(JSON.stringify(payload.message));
              
              eachMessage({
                topic: payload.topic,
                partition: 0,
                message: {
                  value: valueBuffer
                }
              }).catch(err => {
                console.error('[Mock Kafka Consumer] Error in eachMessage handler:', err);
              });
            }
          } catch (err) {
            console.error('[Mock Kafka Consumer] Failed to process incoming message line:', err.message);
          }
        }
      }
    });
  }

  async disconnect() {
    if (this.client) {
      this.client.end();
      this.connected = false;
    }
  }
}

class Kafka {
  constructor(config) {
    this.brokers = config.brokers;
    this.clientId = config.clientId;
    this.useReal = process.env.USE_REAL_KAFKA !== 'false';
    
    if (this.useReal) {
      const { Kafka: RealKafka } = require('kafkajs');
      this.realKafka = new RealKafka(config);
    }
  }

  producer() {
    if (this.useReal) {
      return this.realKafka.producer();
    }
    return new MockProducer(this.brokers[0]);
  }

  consumer({ groupId }) {
    if (this.useReal) {
      return this.realKafka.consumer({ groupId });
    }
    return new MockConsumer(this.brokers[0], groupId);
  }
}

module.exports = { Kafka };
