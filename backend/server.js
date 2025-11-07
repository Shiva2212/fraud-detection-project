const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const transactionSchema = new mongoose.Schema({
  transactionId: String,
  accountId: String,
  amount: Number,
  merchant: String,
  category: String,
  location: Object,
  mlScore: Number,
  riskLevel: String,
  fraudIndicators: [String],
  timestamp: { type: Date, default: Date.now }
});

const alertSchema = new mongoose.Schema({
  alertId: String,
  transactionId: String,
  transaction: Object,
  mlScore: Object,
  riskLevel: String,
  reasons: Array,
  status: { type: String, default: 'PENDING' },
  assignedTo: String,
  reviewedAt: Date,
  action: String,
  comments: String,
  createdAt: { type: Date, default: Date.now }
});

const Transaction = mongoose.model('Transaction', transactionSchema);
const Alert = mongoose.model('Alert', alertSchema);

mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.info('MongoDB connection established.'))
  .catch(err => console.error('MongoDB connection error:', err));

const kafkaBrokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : [];
const kafka = new Kafka({
  clientId: 'fraud-detection-system',
  brokers: kafkaBrokers
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'fraud-detection-group' });

(async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'transactions', fromBeginning: false });

    console.info('Kafka connected. Fraud Detection consumer is running.');
    console.info(`User: sukshamrainaa | Startup: ${new Date().toISOString()}`);

    consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const txn = JSON.parse(message.value.toString());
          const transactionId = txn.id || txn.transactionId;

          if (!transactionId) {
            console.warn('Skipping transaction without an identifier.');
            return;
          }

          console.info(`Processing transaction: ${transactionId}`);

          let score = 0;
          const indicators = [];

          if (txn.location?.risk === 'high') {
            score += 0.5;
            indicators.push({ code: 'HIGH_RISK_GEOGRAPHY', message: `High risk geography: ${txn.location.country || 'unknown'}` });
          }

          if (txn.cvvMatch === false) {
            score += 0.3;
            indicators.push({ code: 'CVV_VERIFICATION_FAILED', message: 'CVV verification failed' });
          }

          if (txn.avsMatch === false) {
            score += 0.15;
            indicators.push({ code: 'ADDRESS_VERIFICATION_FAILED', message: 'Address verification failed' });
          }

          if (typeof txn.amount === 'number' && txn.amount > 100000) {
            score += 0.2;
            indicators.push({ code: 'UNUSUAL_TRANSACTION_AMOUNT', message: `High amount: ₹${txn.amount.toLocaleString()}` });
          } else if (typeof txn.amount === 'number' && txn.amount > 50000) {
            score += 0.1;
            indicators.push({ code: 'UNUSUAL_TRANSACTION_AMOUNT', message: `Medium amount: ₹${txn.amount.toLocaleString()}` });
          }

          const suspiciousMerchants = ['UNKNOWN_MERCHANT', 'CRYPTO_EXCHANGE', 'OFFSHORE_CASINO', 'HIGH_RISK_VENDOR'];
          if (typeof txn.merchant === 'string' && suspiciousMerchants.some(m => txn.merchant.includes(m))) {
            score += 0.3;
            indicators.push({ code: 'HIGH_RISK_MERCHANT', message: `Suspicious merchant: ${txn.merchant}` });
          }

          const suspiciousCategories = ['GAMBLING', 'CRYPTO', 'WIRE_TRANSFER', 'GIFT_CARDS'];
          if (suspiciousCategories.includes(txn.category)) {
            score += 0.25;
            indicators.push({ code: 'HIGH_RISK_MERCHANT_CATEGORY', message: `Category: ${txn.category}` });
          }

          const suspiciousDevices = ['Emulator', 'Unknown Device', 'Rooted Android', 'Jailbroken iPhone'];
          if (suspiciousDevices.includes(txn.device)) {
            score += 0.2;
            indicators.push({ code: 'SUSPICIOUS_DEVICE', message: 'Suspicious device detected' });
          }

          if (txn.isVPN || txn.isTor) {
            score += 0.15;
            indicators.push({ code: 'VPN_OR_PROXY_DETECTED', message: txn.isTor ? 'Tor network detected' : 'VPN/proxy detected' });
          }

          if (Number.isInteger(txn.previousDeclines) && txn.previousDeclines >= 3) {
            score += 0.3;
            indicators.push({ code: 'CARD_TESTING_PATTERN', message: `Previous declines: ${txn.previousDeclines}` });
          }

          if (typeof txn.accountAge === 'number' && txn.accountAge < 30) {
            score += 0.1;
            indicators.push({ code: 'NEW_ACCOUNT_RISK', message: 'New account risk' });
          }

          const txnTime = txn.timestamp ? new Date(txn.timestamp) : new Date();
          const hour = txnTime.getHours();
          if (hour >= 23 || hour <= 5) {
            score += 0.08;
            indicators.push({ code: 'UNUSUAL_TRANSACTION_TIME', message: 'Night-time transaction' });
          }

          const riskLevel = score >= 0.7 ? 'CRITICAL' :
                            score >= 0.5 ? 'HIGH' :
                            score >= 0.3 ? 'MEDIUM' : 'LOW';

          console.info(`Result for ${transactionId} => score: ${score.toFixed(2)}, riskLevel: ${riskLevel}`);

          await Transaction.create({
            transactionId,
            accountId: txn.accountId,
            amount: txn.amount,
            merchant: txn.merchant,
            category: txn.category,
            location: txn.location,
            mlScore: score,
            riskLevel,
            fraudIndicators: indicators.map(i => i.message),
            timestamp: txnTime
          });

          console.info(`Transaction saved: ${transactionId}`);

          if (score >= 0.4) {
            const alert = await Alert.create({
              alertId: `ALERT${Date.now()}${Math.random().toString(36).slice(2, 7)}`,
              transactionId,
              transaction: txn,
              mlScore: { score },
              riskLevel,
              reasons: indicators,
              status: 'PENDING'
            });
            console.info(`Alert created: ${alert.alertId} (risk: ${riskLevel})`);
          } else {
            console.info(`Transaction marked clean: ${transactionId} (score: ${score.toFixed(2)})`);
          }
        } catch (err) {
          console.error('Processing error:', err && err.message ? err.message : err);
        }
      }
    });
  } catch (err) {
    console.error('Kafka initialization error:', err && err.message ? err.message : err);
    process.exit(1);
  }
})();

app.post('/api/transactions', async (req, res) => {
  try {
    await producer.send({
      topic: 'transactions',
      messages: [{ value: JSON.stringify(req.body) }]
    });
    res.json({ success: true });
  } catch (error) {
    console.error('Failed to publish transaction:', error && error.message ? error.message : error);
    res.status(500).json({ success: false, message: error.message });
  }
});

app.get('/api/transactions', async (req, res) => {
  try {
    const transactions = await Transaction.find().sort({ timestamp: -1 }).limit(5000);
    res.json({ success: true, data: transactions });
  } catch (error) {
    console.error('Failed to fetch transactions:', error && error.message ? error.message : error);
    res.status(500).json({ success: false, message: error.message });
  }
});

app.get('/api/alerts', async (req, res) => {
  try {
    const alerts = await Alert.find().sort({ createdAt: -1 }).limit(5000);
    res.json({ success: true, data: alerts });
  } catch (error) {
    console.error('Failed to fetch alerts:', error && error.message ? error.message : error);
    res.status(500).json({ success: false, message: error.message });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const totalTx = await Transaction.countDocuments();
    const totalAlerts = await Alert.countDocuments();
    const criticalAlerts = await Alert.countDocuments({ riskLevel: 'CRITICAL' });
    const pendingAlerts = await Alert.countDocuments({ status: 'PENDING' });

    res.json({
      success: true,
      data: {
        totalTransactions: totalTx,
        totalAlerts,
        criticalAlerts,
        pendingAlerts,
        alertRate: totalTx > 0 ? ((totalAlerts / totalTx) * 100).toFixed(1) : '0.0'
      }
    });
  } catch (error) {
    console.error('Failed to compute stats:', error && error.message ? error.message : error);
    res.status(500).json({ success: false, message: error.message });
  }
});

app.put('/api/alerts/:alertId', async (req, res) => {
  try {
    const { alertId } = req.params;
    const { action, comments, assignedTo } = req.body;

    const alert = await Alert.findOneAndUpdate(
      { alertId },
      {
        status: action,
        action,
        comments,
        assignedTo,
        reviewedAt: new Date()
      },
      { new: true }
    );

    res.json({ success: true, data: alert });
  } catch (error) {
    console.error('Failed to update alert:', error && error.message ? error.message : error);
    res.status(500).json({ success: false, message: error.message });
  }
});

app.delete('/api/cleanup/all', async (req, res) => {
  try {
    const { password } = req.body;
    const CLEANUP_PASSWORD = process.env.CLEANUP_PASSWORD || '140301';

    if (!password) {
      res.status(401).json({
        success: false,
        message: 'Password required',
        error: 'MISSING_PASSWORD'
      });
      return;
    }

    if (password !== CLEANUP_PASSWORD) {
      console.warn(`Failed cleanup attempt with invalid password at ${new Date().toISOString()}`);
      res.status(403).json({
        success: false,
        message: 'Invalid password',
        error: 'INVALID_PASSWORD'
      });
      return;
    }

    const timestamp = new Date().toISOString();
    console.info(`Database cleanup requested by user at ${timestamp}`);

    const deletedTransactions = await Transaction.deleteMany({});
    const deletedAlerts = await Alert.deleteMany({});

    const txCount = await Transaction.countDocuments();
    const alertCount = await Alert.countDocuments();

    console.info('Database cleanup completed successfully');
    console.info(`Deleted transactions: ${deletedTransactions.deletedCount}, deleted alerts: ${deletedAlerts.deletedCount}`);

    res.json({
      success: true,
      message: 'Database cleaned successfully',
      timestamp,
      deleted: {
        transactions: deletedTransactions.deletedCount,
        alerts: deletedAlerts.deletedCount
      },
      remaining: {
        transactions: txCount,
        alerts: alertCount
      }
    });
  } catch (error) {
    console.error('Cleanup failed:', error && error.message ? error.message : error);
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
});

app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    message: 'Advanced Fraud Detection API',
    user: 'sukshamrainaa',
    version: '3.2.0',
    modelVersion: 'v3.2.0',
    alertThreshold: '0.4 (40%)',
    timestamp: new Date().toISOString()
  });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.info(`Backend running on port ${PORT}`);
  console.info(`Started at ${new Date().toISOString()}`);
});