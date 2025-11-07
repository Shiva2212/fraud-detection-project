// @ts-nocheck
import React, { useState, useEffect } from 'react';
import { AlertTriangle, CheckCircle, XCircle, TrendingUp, Activity, Clock, Shield, Filter, Search, Eye, Lock, Unlock, AlertCircle, BarChart3, Database, Zap } from 'lucide-react';
import './FraudDetectionApp.css';

class FraudDetectionService {
  constructor() {
    const savedTransactions = localStorage.getItem('fraudDetection_transactions');
    const savedAlerts = localStorage.getItem('fraudDetection_alerts');
    const savedProfiles = localStorage.getItem('fraudDetection_profiles');

    this.transactions = savedTransactions ? JSON.parse(savedTransactions) : [];
    this.alerts = savedAlerts ? JSON.parse(savedAlerts).map(alert => ({
      ...alert,
      createdAt: new Date(alert.createdAt),
      reviewedAt: alert.reviewedAt ? new Date(alert.reviewedAt) : null
    })) : [];
    this.accountProfiles = savedProfiles ? new Map(JSON.parse(savedProfiles)) : new Map();
    
    this.features = new Map();
    this.mlScores = new Map();
    this.actions = [];
    this.fraudPatterns = this.initializeFraudPatterns();
  }

  saveToLocalStorage() {
    localStorage.setItem('fraudDetection_transactions', JSON.stringify(this.transactions));
    localStorage.setItem('fraudDetection_alerts', JSON.stringify(this.alerts));
    localStorage.setItem('fraudDetection_profiles', JSON.stringify(Array.from(this.accountProfiles.entries())));
  }

  initializeFraudPatterns() {
    return {
      velocityThreshold: 5,
      amountThreshold: 50000,
      geoDistanceThreshold: 500,
      suspiciousMerchants: ['UNKNOWN_MERCHANT', 'HIGH_RISK_VENDOR', 'CRYPTO_EXCHANGE', 'OFFSHORE_CASINO'],
      suspiciousCategories: ['GAMBLING', 'CRYPTO', 'WIRE_TRANSFER', 'GIFT_CARDS'],
      nightTimeStart: 23,
      nightTimeEnd: 5,
      highRiskCountries: ['NK', 'IR', 'SY'],
      deviceFingerprints: ['EMULATOR', 'ROOTED', 'JAILBROKEN']
    };
  }

  generateTransaction() {
    const accounts = ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005'];
    const merchants = ['Amazon India', 'Flipkart', 'Swiggy', 'BookMyShow', 'UNKNOWN_MERCHANT', 'Zomato', 'BigBazaar', 'CRYPTO_EXCHANGE', 'OFFSHORE_CASINO'];
    const categories = ['SHOPPING', 'FOOD', 'ENTERTAINMENT', 'GAMBLING', 'TRAVEL', 'UTILITIES', 'CRYPTO', 'WIRE_TRANSFER'];
    const locations = [
      { city: 'Mumbai', lat: 19.0760, lon: 72.8777, country: 'IN' },
      { city: 'Delhi', lat: 28.7041, lon: 77.1025, country: 'IN' },
      { city: 'Bangalore', lat: 12.9716, lon: 77.5946, country: 'IN' },
      { city: 'Chennai', lat: 13.0827, lon: 80.2707, country: 'IN' },
      { city: 'Kolkata', lat: 22.5726, lon: 88.3639, country: 'IN' }
    ];
    const devices = ['iOS 17.2', 'Android 14', 'Windows 11', 'EMULATOR', 'ROOTED'];
    const browsers = ['Chrome', 'Safari', 'Firefox', 'TOR_BROWSER', 'Unknown'];

    const accountId = accounts[Math.floor(Math.random() * accounts.length)];
    const merchant = merchants[Math.floor(Math.random() * merchants.length)];
    const category = categories[Math.floor(Math.random() * categories.length)];
    const location = locations[Math.floor(Math.random() * locations.length)];
    const amount = Math.floor(Math.random() * 150000) + 100;
    const isFraudulent = Math.random() < 0.15;
    const device = devices[Math.floor(Math.random() * devices.length)];
    const browser = browsers[Math.floor(Math.random() * browsers.length)];

    const transaction = {
      id: `TXN${Date.now()}${Math.random().toString(36).substr(2, 9)}`,
      accountId,
      cardLast4: Math.floor(1000 + Math.random() * 9000).toString(),
      amount,
      currency: 'INR',
      merchant,
      category,
      location,
      timestamp: new Date(),
      deviceId: `DEV${Math.floor(Math.random() * 1000)}`,
      device,
      browser,
      ipAddress: `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
      channel: ['ONLINE', 'POS', 'MOBILE', 'ATM'][Math.floor(Math.random() * 4)],
      isVPN: Math.random() < 0.1,
      isTor: browser === 'TOR_BROWSER',
      cvvMatch: Math.random() < 0.9,
      avsMatch: Math.random() < 0.95,
      previousDeclines: Math.floor(Math.random() * 5),
      accountAge: Math.floor(Math.random() * 365),
      isFraudulent
    };

    return transaction;
  }

  extractFeatures(transaction) {
    const accountHistory = this.transactions.filter(t => 
      t.accountId === transaction.accountId && 
      new Date(t.timestamp) > new Date(Date.now() - 10 * 60 * 1000)
    );

    const velocityCount = accountHistory.length;
    const avgAmount = accountHistory.length > 0 
      ? accountHistory.reduce((sum, t) => sum + t.amount, 0) / accountHistory.length 
      : transaction.amount;
    
    const amountDelta = Math.abs(transaction.amount - avgAmount);
    
    const lastLocation = this.accountProfiles.get(transaction.accountId)?.lastLocation;
    const geoDistance = lastLocation ? this.calculateDistance(lastLocation, transaction.location) : 0;

    const hour = new Date(transaction.timestamp).getHours();
    const isNightTime = hour >= this.fraudPatterns.nightTimeStart || hour <= this.fraudPatterns.nightTimeEnd;

    const features = {
      transactionId: transaction.id,
      velocityCount,
      avgAmount: Math.round(avgAmount),
      amountDelta: Math.round(amountDelta),
      geoDistance: Math.round(geoDistance),
      isNightTime,
      isSuspiciousMerchant: this.fraudPatterns.suspiciousMerchants.includes(transaction.merchant),
      isSuspiciousCategory: this.fraudPatterns.suspiciousCategories.includes(transaction.category),
      isHighAmount: transaction.amount > this.fraudPatterns.amountThreshold,
      isVPN: transaction.isVPN,
      isTor: transaction.isTor,
      cvvMatch: transaction.cvvMatch,
      avsMatch: transaction.avsMatch,
      isSuspiciousDevice: this.fraudPatterns.deviceFingerprints.some(d => transaction.device.includes(d)),
      previousDeclines: transaction.previousDeclines,
      isNewAccount: transaction.accountAge < 30,
      timestamp: new Date()
    };

    this.features.set(transaction.id, features);
    return features;
  }

  calculateDistance(loc1, loc2) {
    const R = 6371;
    const dLat = (loc2.lat - loc1.lat) * Math.PI / 180;
    const dLon = (loc2.lon - loc1.lon) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(loc1.lat * Math.PI / 180) * Math.cos(loc2.lat * Math.PI / 180) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
  }

  calculateMLScore(transaction, features) {
    let score = 0;
    const weights = {
      velocityCount: 0.15,
      amountDelta: 0.10,
      geoDistance: 0.18,
      isNightTime: 0.08,
      isSuspiciousMerchant: 0.15,
      isSuspiciousCategory: 0.12,
      isHighAmount: 0.08,
      isVPN: 0.05,
      isTor: 0.10,
      cvvFail: 0.12,
      avsFail: 0.08,
      suspiciousDevice: 0.10,
      cardTesting: 0.12,
      newAccount: 0.07
    };

    const contributions = {};

    if (features.velocityCount > this.fraudPatterns.velocityThreshold) {
      const contribution = (features.velocityCount / 10) * weights.velocityCount;
      score += contribution;
      contributions.velocityCount = contribution;
    }

    if (features.amountDelta > 10000) {
      const contribution = Math.min((features.amountDelta / 50000), 1) * weights.amountDelta;
      score += contribution;
      contributions.amountDelta = contribution;
    }

    if (features.geoDistance > this.fraudPatterns.geoDistanceThreshold) {
      const contribution = Math.min((features.geoDistance / 1000), 1) * weights.geoDistance;
      score += contribution;
      contributions.geoDistance = contribution;
    }

    if (features.isNightTime) {
      score += weights.isNightTime;
      contributions.isNightTime = weights.isNightTime;
    }

    if (features.isSuspiciousMerchant) {
      score += weights.isSuspiciousMerchant;
      contributions.isSuspiciousMerchant = weights.isSuspiciousMerchant;
    }

    if (features.isSuspiciousCategory) {
      score += weights.isSuspiciousCategory;
      contributions.isSuspiciousCategory = weights.isSuspiciousCategory;
    }

    if (features.isHighAmount) {
      score += weights.isHighAmount;
      contributions.isHighAmount = weights.isHighAmount;
    }

    if (features.isVPN) {
      score += weights.isVPN;
      contributions.isVPN = weights.isVPN;
    }

    if (features.isTor) {
      score += weights.isTor;
      contributions.isTor = weights.isTor;
    }

    if (!features.cvvMatch) {
      score += weights.cvvFail;
      contributions.cvvFail = weights.cvvFail;
    }

    if (!features.avsMatch) {
      score += weights.avsFail;
      contributions.avsFail = weights.avsFail;
    }

    if (features.isSuspiciousDevice) {
      score += weights.suspiciousDevice;
      contributions.suspiciousDevice = weights.suspiciousDevice;
    }

    if (features.previousDeclines >= 3) {
      score += weights.cardTesting;
      contributions.cardTesting = weights.cardTesting;
    }

    if (features.isNewAccount) {
      score += weights.newAccount;
      contributions.newAccount = weights.newAccount;
    }

    score = Math.min(score, 1);

    const mlScore = {
      transactionId: transaction.id,
      score: parseFloat(score.toFixed(3)),
      contributions,
      timestamp: new Date(),
      modelVersion: 'v3.2.0'
    };

    this.mlScores.set(transaction.id, mlScore);
    return mlScore;
  }

  generateReasonCodes(transaction, features, mlScore) {
    const reasons = [];

    if (features.velocityCount > this.fraudPatterns.velocityThreshold) {
      reasons.push({
        code: 'VELOCITY_HIGH',
        message: `${features.velocityCount} transactions in 10 minutes (threshold: ${this.fraudPatterns.velocityThreshold})`,
        explanation: `Abnormal transaction frequency detected. This account made ${features.velocityCount} transactions within 10 minutes, exceeding normal patterns by ${features.velocityCount - this.fraudPatterns.velocityThreshold}. Such rapid activity often indicates card testing or compromised credentials.`,
        severity: 'high',
        impact: 'Critical'
      });
    }

    if (features.geoDistance > this.fraudPatterns.geoDistanceThreshold) {
      reasons.push({
        code: 'IMPOSSIBLE_TRAVEL',
        message: `${Math.round(features.geoDistance)} km impossible travel distance`,
        explanation: `Transaction location is ${Math.round(features.geoDistance)} kilometers from the previous transaction. The time gap makes this travel physically impossible, indicating potential card cloning or account takeover.`,
        severity: 'high',
        impact: 'Critical'
      });
    }

    if (!features.cvvMatch) {
      reasons.push({
        code: 'CVV_VERIFICATION_FAILED',
        message: 'CVV verification failed',
        explanation: 'The Card Verification Value (CVV) provided does not match bank records. This is a strong indicator that the physical card is not present and the transaction may be using stolen card details.',
        severity: 'high',
        impact: 'Critical'
      });
    }

    if (!features.avsMatch) {
      reasons.push({
        code: 'ADDRESS_VERIFICATION_FAILED',
        message: 'AVS (Address Verification System) mismatch',
        explanation: 'The billing address provided does not match the address on file with the card issuer. This suggests the cardholder information may be incomplete or stolen.',
        severity: 'medium',
        impact: 'High'
      });
    }

    if (features.previousDeclines >= 3) {
      reasons.push({
        code: 'CARD_TESTING_PATTERN',
        message: `${features.previousDeclines} previous declined attempts`,
        explanation: `This card has been declined ${features.previousDeclines} times recently. Multiple decline patterns often indicate fraudsters testing stolen card numbers to find valid ones.`,
        severity: 'high',
        impact: 'Critical'
      });
    }

    if (features.isTor) {
      reasons.push({
        code: 'TOR_NETWORK_DETECTED',
        message: 'Transaction via TOR network',
        explanation: 'The transaction originated from the TOR anonymity network. While legitimate users may use TOR, it is frequently used by fraudsters to mask their real location and identity.',
        severity: 'high',
        impact: 'Critical'
      });
    }

    if (features.isVPN) {
      reasons.push({
        code: 'VPN_PROXY_DETECTED',
        message: 'VPN or proxy detected',
        explanation: 'The transaction came through a VPN or proxy server. This masks the true origin of the transaction and is a common technique used in fraudulent activities.',
        severity: 'medium',
        impact: 'Medium'
      });
    }

    if (features.isSuspiciousDevice) {
      reasons.push({
        code: 'SUSPICIOUS_DEVICE',
        message: 'Emulator or rooted device detected',
        explanation: `Device fingerprinting indicates an emulator, rooted, or jailbroken device (${transaction.device}). These modified devices are commonly used by fraudsters to bypass security controls.`,
        severity: 'high',
        impact: 'Critical'
      });
    }

    if (features.isHighAmount) {
      reasons.push({
        code: 'UNUSUAL_TRANSACTION_AMOUNT',
        message: `High amount: ₹${transaction.amount.toLocaleString('en-IN')} exceeds threshold`,
        explanation: `Transaction amount significantly exceeds the normal threshold of ₹${this.fraudPatterns.amountThreshold.toLocaleString('en-IN')}. Large atypical transactions often indicate account takeover or fraudulent purchases.`,
        severity: 'medium',
        impact: 'High'
      });
    }

    if (features.isNightTime) {
      reasons.push({
        code: 'UNUSUAL_TRANSACTION_TIME',
        message: 'Late night transaction (11 PM - 5 AM)',
        explanation: 'Transaction occurred during unusual hours when legitimate cardholders are typically inactive. Fraudsters often operate during these hours to delay detection.',
        severity: 'low',
        impact: 'Low'
      });
    }

    if (features.isSuspiciousMerchant) {
      reasons.push({
        code: 'HIGH_RISK_MERCHANT',
        message: `High-risk merchant: ${transaction.merchant}`,
        explanation: `The merchant "${transaction.merchant}" is flagged as high-risk due to historical fraud patterns, unusual business practices, or regulatory concerns. Transactions with such merchants require enhanced scrutiny.`,
        severity: 'high',
        impact: 'Critical'
      });
    }

    if (features.isSuspiciousCategory) {
      reasons.push({
        code: 'HIGH_RISK_MERCHANT_CATEGORY',
        message: `High-risk category: ${transaction.category}`,
        explanation: `Transaction category "${transaction.category}" has statistically higher fraud rates. Categories like gambling, cryptocurrency, wire transfers, and gift cards are frequently targeted by fraudsters.`,
        severity: 'medium',
        impact: 'Medium'
      });
    }

    if (features.isNewAccount) {
      reasons.push({
        code: 'NEW_ACCOUNT_RISK',
        message: `New account (${transaction.accountAge} days old)`,
        explanation: `Account is only ${transaction.accountAge} days old. Newly created accounts have higher fraud rates as fraudsters often create fresh accounts using stolen identities to avoid detection.`,
        severity: 'medium',
        impact: 'Medium'
      });
    }

    if (features.amountDelta > 20000) {
      reasons.push({
        code: 'SPENDING_PATTERN_ANOMALY',
        message: `₹${Math.round(features.amountDelta).toLocaleString('en-IN')} deviation from normal spending`,
        explanation: `Transaction deviates by ₹${Math.round(features.amountDelta).toLocaleString('en-IN')} from the historical average of ₹${Math.round(features.avgAmount).toLocaleString('en-IN')}. Sudden spending pattern changes often indicate account compromise.`,
        severity: 'medium',
        impact: 'High'
      });
    }

    return reasons;
  }

  processTransaction(transaction) {
    this.transactions.push(transaction);

    const features = this.extractFeatures(transaction);
    const mlScore = this.calculateMLScore(transaction, features);
    const reasons = this.generateReasonCodes(transaction, features, mlScore);

    let riskLevel = 'LOW';
    let shouldAlert = false;

    if (mlScore.score > 0.75 || transaction.isFraudulent) {
      riskLevel = 'CRITICAL';
      shouldAlert = true;
    } else if (mlScore.score > 0.55) {
      riskLevel = 'HIGH';
      shouldAlert = true;
    } else if (mlScore.score > 0.35) {
      riskLevel = 'MEDIUM';
      shouldAlert = mlScore.score > 0.45;
    }

    if (shouldAlert) {
      const alert = {
        id: `ALERT${Date.now()}${Math.random().toString(36).substr(2, 9)}`,
        transactionId: transaction.id,
        transaction,
        features,
        mlScore,
        riskLevel,
        reasons,
        status: 'PENDING',
        createdAt: new Date(),
        assignedTo: null,
        reviewedAt: null,
        action: null,
        comments: ''
      };
      this.alerts.push(alert);
    }

    this.accountProfiles.set(transaction.accountId, {
      lastLocation: transaction.location,
      lastTransaction: transaction.timestamp
    });

    this.saveToLocalStorage();
    return { features, mlScore, riskLevel, shouldAlert };
  }

  getAlerts(filter = 'ALL') {
    let filtered = [...this.alerts];
    
    if (filter === 'PENDING') {
      filtered = filtered.filter(a => a.status === 'PENDING');
    } else if (filter === 'REVIEWED') {
      filtered = filtered.filter(a => a.status !== 'PENDING');
    } else if (filter === 'HIGH') {
      filtered = filtered.filter(a => a.riskLevel === 'HIGH');
    } else if (filter === 'CRITICAL') {
      filtered = filtered.filter(a => a.riskLevel === 'CRITICAL');
    }

    return filtered.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  }

  takeAction(alertId, action, comments) {
    const alert = this.alerts.find(a => a.id === alertId);
    if (alert) {
      alert.status = action;
      alert.action = action;
      alert.reviewedAt = new Date();
      alert.comments = comments;
      alert.assignedTo = 'Suksham Raina';

      this.actions.push({
        alertId,
        transactionId: alert.transactionId,
        action,
        comments,
        timestamp: new Date(),
        analyst: 'Suksham Raina'
      });

      this.saveToLocalStorage();
    }
  }

  getStats() {
    const totalTransactions = this.transactions.length;
    const totalAlerts = this.alerts.length;
    const pendingAlerts = this.alerts.filter(a => a.status === 'PENDING').length;
    const criticalAlerts = this.alerts.filter(a => a.riskLevel === 'CRITICAL').length;
    const highRiskAlerts = this.alerts.filter(a => a.riskLevel === 'HIGH').length;
    const avgScore = this.transactions.length > 0
      ? Array.from(this.mlScores.values()).reduce((sum, s) => sum + s.score, 0) / this.mlScores.size
      : 0;

    return {
      totalTransactions,
      totalAlerts,
      pendingAlerts,
      criticalAlerts,
      highRiskAlerts,
      avgScore: avgScore.toFixed(3),
      alertRate: totalTransactions > 0 ? ((totalAlerts / totalTransactions) * 100).toFixed(1) : 0,
      blockedAmount: this.alerts.filter(a => a.action === 'BLOCKED').reduce((sum, a) => sum + (a.transaction?.amount || 0), 0)
    };
  }
}

const FraudDetectionApp = () => {
  const [service] = useState(() => new FraudDetectionService());
  const [activeTab, setActiveTab] = useState('dashboard');
  const [alerts, setAlerts] = useState([]);
  const [stats, setStats] = useState({});
  const [filter, setFilter] = useState('ALL');
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [isStreaming, setIsStreaming] = useState(false);
  const [recentTransactions, setRecentTransactions] = useState([]);
  const [currentTime, setCurrentTime] = useState(new Date());

  useEffect(() => {
    updateData();
  }, [filter]);

  useEffect(() => {
    let interval;
    if (isStreaming) {
      interval = setInterval(() => {
        const txn = service.generateTransaction();
        service.processTransaction(txn);
        setRecentTransactions(prev => [txn, ...prev].slice(0, 5));
        updateData();
      }, 2000);
    }
    return () => clearInterval(interval);
  }, [isStreaming]);

  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const updateData = () => {
    setAlerts(service.getAlerts(filter));
    setStats(service.getStats());
  };

  const handleAction = (alertId, action, comments) => {
    service.takeAction(alertId, action, comments);
    updateData();
    setSelectedAlert(null);
  };

  const filteredAlerts = alerts.filter(alert => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    return (
      alert.transactionId.toLowerCase().includes(search) ||
      alert.transaction.accountId.toLowerCase().includes(search) ||
      alert.transaction.merchant.toLowerCase().includes(search)
    );
  });

  const getCurrentDateTime = () => {
    const day = String(currentTime.getDate()).padStart(2, '0');
    const month = String(currentTime.getMonth() + 1).padStart(2, '0');
    const year = currentTime.getFullYear();
    const hours = String(currentTime.getHours()).padStart(2, '0');
    const minutes = String(currentTime.getMinutes()).padStart(2, '0');
    const seconds = String(currentTime.getSeconds()).padStart(2, '0');
    
    return `${day}/${month}/${year}, ${hours}:${minutes}:${seconds} IST`;
  };

  return (
    <div className="app-container">
      <header className="app-header">
        <div className="container">
          <div className="header-content">
            <div className="header-left">
              <div className="logo-box">
                <Shield className="logo-icon" />
              </div>
              <div className="header-text">
                <h1 className="header-title">Advanced Fraud Detection System</h1>
                <p className="header-subtitle">
                  Industry-Standard ML Model v3.2.0 | User: Suksham Raina | {getCurrentDateTime()}
                </p>
              </div>
            </div>
            <div className="header-actions">
              <button
                onClick={() => setIsStreaming(!isStreaming)}
                className={`stream-btn ${isStreaming ? 'stream-btn-stop' : 'stream-btn-start'}`}
              >
                {isStreaming ? 'Stop Stream' : 'Start Stream'}
              </button>
              {isStreaming && (
                <div className="live-indicator">
                  <div className="live-dot" />
                  <span className="live-text">Live</span>
                </div>
              )}
            </div>
          </div>
        </div>
      </header>

      <nav className="app-nav">
        <div className="container">
          <div className="nav-tabs">
            {[
              { id: 'dashboard', label: 'Dashboard', icon: BarChart3 },
              { id: 'alerts', label: 'Alert Management', icon: AlertTriangle },
              { id: 'stream', label: 'Transaction Stream', icon: Activity }
            ].map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`nav-tab ${activeTab === tab.id ? 'nav-tab-active' : ''}`}
              >
                <tab.icon className="nav-tab-icon" />
                {tab.label}
                {activeTab === tab.id && <div className="nav-tab-indicator" />}
              </button>
            ))}
          </div>
        </div>
      </nav>

      <main className="main-content container">
        {activeTab === 'dashboard' && (
          <DashboardTab stats={stats} alerts={alerts} />
        )}
        {activeTab === 'alerts' && (
          <AlertsTab
            alerts={filteredAlerts}
            filter={filter}
            setFilter={setFilter}
            searchTerm={searchTerm}
            setSearchTerm={setSearchTerm}
            selectedAlert={selectedAlert}
            setSelectedAlert={setSelectedAlert}
            handleAction={handleAction}
          />
        )}
        {activeTab === 'stream' && (
          <StreamTab
            recentTransactions={recentTransactions}
            isStreaming={isStreaming}
          />
        )}
      </main>
    </div>
  );
};

const DashboardTab = ({ stats, alerts }) => {
  const [isClearing, setIsClearing] = useState(false);

  const handleCleanDatabase = async () => {
    // First confirmation
    if (!window.confirm('⚠️ This will delete ALL data:\n\n• All Transactions\n• All Alerts\n• Dashboard Stats\n\nAre you sure?')) {
      return;
    }

    // Password prompt
    const password = window.prompt('🔐 Enter cleanup password:');
    
    if (!password) {
      alert('❌ Password required to clean database');
      return;
    }

    setIsClearing(true);

    try {
      // Clear backend database with password
      const response = await fetch('http://localhost:5000/api/cleanup/all', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password })
      });

      const data = await response.json();

      if (data.success) {
        // Clear localStorage
        localStorage.removeItem('fraudDetection_transactions');
        localStorage.removeItem('fraudDetection_alerts');
        localStorage.removeItem('fraudDetection_profiles');
        localStorage.clear();

        alert(`✅ Database cleaned successfully!\n\n`);        
        // Reload page after 2 seconds
        setTimeout(() => {
          window.location.reload();
        }, 2000);
      } else {
        if (data.error === 'INVALID_PASSWORD') {
          alert('❌ Invalid password!\n\nPlease contact administrator.');
        } else if (data.error === 'MISSING_PASSWORD') {
          alert('❌ Password required!');
        } else {
          alert('❌ Failed to clean database: ' + data.message);
        }
      }
    } catch (error) {
      alert('❌ Error: ' + error.message);
    } finally {
      setIsClearing(false);
    }
  };

  const statCards = [
    {
      label: 'Total Transactions',
      value: stats.totalTransactions || 0,
      icon: Database,
      emoji: '📊',
      colorClass: 'stat-blue',
      subtitle: `${((stats.totalAlerts / stats.totalTransactions) * 100 || 0).toFixed(1)}% flagged`
    },
    {
      label: 'Critical Alerts',
      value: stats.criticalAlerts || 0,
      icon: AlertCircle,
      emoji: '🚨',
      colorClass: 'stat-red',
      subtitle: 'Require immediate action'
    },
    {
      label: 'Pending Review',
      value: stats.pendingAlerts || 0,
      icon: Clock,
      emoji: '⏰',
      colorClass: 'stat-amber',
      subtitle: 'Awaiting analyst decision'
    },
    {
      label: 'Amount Blocked',
      value: `₹${(stats.blockedAmount || 0).toLocaleString('en-IN')}`,
      icon: Shield,
      emoji: '💰',
      colorClass: 'stat-green',
      subtitle: 'Fraud prevented'
    }
  ];

  const criticalAlerts = alerts.filter(a => a.riskLevel === 'CRITICAL' || a.riskLevel === 'HIGH');

  return (
    <div className="dashboard-container">
      {/* Clean Database Button */}
      <div className="dashboard-actions">
        <button 
          onClick={handleCleanDatabase} 
          disabled={isClearing}
          className="clean-database-btn"
        >
          {isClearing ? (
            <>
              <Clock className="btn-icon spinning" />
              Cleaning...
            </>
          ) : (
            <>
              <XCircle className="btn-icon" />
              🗑️ Clear Database
            </>
          )}
        </button>
      </div>

      <div className="stat-cards-grid">
        {statCards.map((stat, idx) => (
          <div key={idx} className="stat-card">
            <div className={`stat-card-border ${stat.colorClass}`} />
            <div className="stat-card-content">
              <div className="stat-card-header">
                <div className={`stat-icon-box ${stat.colorClass}`}>
                  <stat.icon className="stat-icon" />
                  {stat.emoji && <span className="stat-emoji">{stat.emoji}</span>}
                </div>
              </div>
              <p className="stat-label">{stat.label}</p>
              <h3 className="stat-value">{stat.value}</h3>
              <p className="stat-subtitle">{stat.subtitle}</p>
            </div>
          </div>
        ))}
      </div>

      <div className="high-risk-panel">
        <div className="panel-header">
          <h3 className="panel-title">Critical & High-Risk Alerts</h3>
          <span className="critical-badge">
            {criticalAlerts.length} Urgent
          </span>
        </div>
        <div className="alert-list">
          {criticalAlerts.length > 0 ? (
            criticalAlerts.slice(0, 5).map(alert => (
              <div key={alert.id} className="high-risk-alert">
                <div className="alert-info">
                  <div className="alert-header-row">
                    <span className="alert-id">{alert.transactionId}</span>
                    <span className={`critical-tag critical-tag-${alert.riskLevel.toLowerCase()}`}>
                      {alert.riskLevel}
                    </span>
                  </div>
                  <p className="alert-details">
                    <span className="alert-merchant">{alert.transaction.merchant}</span> • ₹{alert.transaction.amount.toLocaleString('en-IN')} • {alert.transaction.location.city}
                  </p>
                </div>
                <div className="alert-score">
                  <p className="score-value">{(alert.mlScore.score * 100).toFixed(0)}%</p>
                  <p className="score-label">Risk Score</p>
                </div>
              </div>
            ))
          ) : (
            <div className="no-alerts-message">
              <CheckCircle style={{ width: 64, height: 64, color: '#22c55e', strokeWidth: 2 }} />
              <p style={{ fontSize: '1rem', color: '#6b7280', marginTop: '1rem' }}>
                No critical or high-risk alerts at this time
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const AlertsTab = ({ alerts, filter, setFilter, searchTerm, setSearchTerm, selectedAlert, setSelectedAlert, handleAction }) => {
  return (
    <div className="alerts-layout">
      <div className="alerts-main">
        <div className="search-filter-panel">
          <div className="search-box-wrapper">
            <div className="search-box">
              <Search className="search-icon" />
              <input
                type="text"
                placeholder="Search by ID, account, merchant..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="search-input"
              />
            </div>
          </div>
          <div className="filter-buttons">
            {['ALL', 'PENDING', 'CRITICAL', 'HIGH', 'REVIEWED'].map(f => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className={`filter-btn ${filter === f ? 'filter-btn-active' : ''}`}
              >
                {f}
              </button>
            ))}
          </div>
        </div>

        <div className="alerts-list">
          {alerts.map(alert => (
            <div
              key={alert.id}
              onClick={() => setSelectedAlert(alert)}
              className={`alert-card ${selectedAlert?.id === alert.id ? 'alert-card-selected' : ''}`}
            >
              <div className="alert-card-header">
                <div className="alert-badges">
                  <span className={`risk-badge risk-badge-${alert.riskLevel.toLowerCase()}`}>
                    {alert.riskLevel}
                  </span>
                  <span className="transaction-id">{alert.transactionId}</span>
                  <span className="fraud-indicators-badge">
                    {alert.reasons.length} Indicators
                  </span>
                </div>
                <div className="risk-score-display">
                  <span className="risk-score-number">{(alert.mlScore.score * 100).toFixed(0)}%</span>
                  <p className="risk-score-text">Risk</p>
                </div>
              </div>
              <div className="alert-details-grid">
                <div className="detail-box">
                  <p className="detail-label">Account</p>
                  <p className="detail-value">{alert.transaction.accountId}</p>
                </div>
                <div className="detail-box">
                  <p className="detail-label">Amount</p>
                  <p className="detail-value">₹{alert.transaction.amount.toLocaleString('en-IN')}</p>
                </div>
                <div className="detail-box">
                  <p className="detail-label">Merchant</p>
                  <p className="detail-value">{alert.transaction.merchant}</p>
                </div>
                <div className="detail-box">
                  <p className="detail-label">Location</p>
                  <p className="detail-value">{alert.transaction.location.city}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="alerts-sidebar">
        {selectedAlert ? (
          <AlertDetailsPanel alert={selectedAlert} handleAction={handleAction} />
        ) : (
          <div className="no-selection">
            <div className="no-selection-icon">
              <Eye className="eye-icon" />
            </div>
            <h3 className="no-selection-title">No Alert Selected</h3>
            <p className="no-selection-text">Select an alert to view detailed analysis</p>
          </div>
        )}
      </div>
    </div>
  );
};

const AlertDetailsPanel = ({ alert, handleAction }) => {
  const [comments, setComments] = useState('');
  const [expandedReason, setExpandedReason] = useState(null);

  const generateAISummary = () => {
    const riskLevel = alert.riskLevel;
    const score = (alert.mlScore.score * 100).toFixed(0);
    const reasonCount = alert.reasons.length;
    
    let summary = '';
    
    if (riskLevel === 'CRITICAL') {
      summary = `🚨 CRITICAL RISK ALERT: This transaction has been flagged as CRITICAL with a ${score}% fraud probability. Our ML model detected ${reasonCount} critical indicators suggesting highly probable fraudulent activity. `;
    } else if (riskLevel === 'HIGH') {
      summary = `⚠️ HIGH RISK ALERT: This transaction shows ${reasonCount} suspicious patterns with a ${score}% risk score indicating probable fraud. Immediate investigation required. `;
    } else if (riskLevel === 'MEDIUM') {
      summary = `⚡ MEDIUM RISK: This transaction shows ${reasonCount} suspicious patterns with a ${score}% risk score. While not immediately critical, it requires careful review and verification. `;
    } else {
      summary = `✓ LOW RISK: This transaction appears legitimate with only a ${score}% risk score. However, ${reasonCount} minor indicators were detected for monitoring purposes. `;
    }

    if (alert.reasons.length > 0) {
      const criticalReasons = alert.reasons.filter(r => r.severity === 'high');
      if (criticalReasons.length > 0) {
        summary += `\n\n🔴 Critical Concerns: ${criticalReasons.map(r => r.code).join(', ')}. `;
      }
    }

    summary += `\n\n💡 Recommended Action: ${
      riskLevel === 'CRITICAL' ? '🚫 BLOCK immediately and initiate fraud investigation' : 
      riskLevel === 'HIGH' ? '⏸️ HOLD for manual review and customer verification' :
      riskLevel === 'MEDIUM' ? '🔍 REVIEW with enhanced authentication' : 
      '✅ APPROVE with continued monitoring'
    }`;
    
    return summary;
  };

  return (
    <div className="detail-panel">
      <div className="detail-panel-header">
        <div className="detail-panel-title-row">
          <h3 className="detail-panel-title">Alert Details</h3>
          <span className={`risk-badge-large risk-badge-${alert.riskLevel.toLowerCase()}`}>
            {alert.riskLevel}
          </span>
        </div>
        <p className="detail-panel-id">{alert.id}</p>
      </div>

      <div className={`ai-assessment ai-assessment-${alert.riskLevel.toLowerCase()}`}>
        <div className="ai-assessment-content">
          <div className="ai-icon-box">
            <Shield className="ai-icon" />
          </div>
          <div className="ai-text">
            <h4 className="ai-title">🤖 AI Risk Assessment</h4>
            <p className="ai-summary">{generateAISummary()}</p>
          </div>
        </div>
      </div>

      <div className="score-panel">
        <div className="score-panel-header">
          <p className="score-panel-label">📊 Risk Score Analysis</p>
          <span className="fraud-count-badge">
            {alert.reasons.length} Fraud Indicators Detected
          </span>
        </div>
        <div className="score-bar-container">
          <div className="score-bar-bg">
            <div
              className={`score-bar-fill score-bar-${alert.mlScore.score > 0.75 ? 'critical' : alert.mlScore.score > 0.55 ? 'high' : alert.mlScore.score > 0.35 ? 'medium' : 'low'}`}
              style={{ width: `${alert.mlScore.score * 100}%` }}
            />
          </div>
          <span className="score-percentage">{(alert.mlScore.score * 100).toFixed(0)}%</span>
        </div>
        <div className="score-metrics">
          <div className="score-metric">
            <span className="metric-label">Fraud Probability</span>
            <span className="metric-value">{(alert.mlScore.score * 100).toFixed(1)}%</span>
          </div>
          <div className="score-metric">
            <span className="metric-label">Model Version</span>
            <span className="metric-value">{alert.mlScore.modelVersion}</span>
          </div>
          <div className="score-metric">
            <span className="metric-label">Confidence</span>
            <span className="metric-value">
              {alert.mlScore.score > 0.75 ? 'Very High' : 
               alert.mlScore.score > 0.55 ? 'High' : 
               alert.mlScore.score > 0.35 ? 'Medium' : 'Low'}
            </span>
          </div>
        </div>
      </div>

      <div className="indicators-section">
        <div className="indicators-header">
          <h4 className="indicators-title">🚨 Fraud Indicators ({alert.reasons.length})</h4>
          <div className="indicators-severity-summary">
            <span className="severity-count severity-high">
              {alert.reasons.filter(r => r.severity === 'high').length} Critical
            </span>
            <span className="severity-count severity-medium">
              {alert.reasons.filter(r => r.severity === 'medium').length} High
            </span>
            <span className="severity-count severity-low">
              {alert.reasons.filter(r => r.severity === 'low').length} Low
            </span>
          </div>
        </div>
        <div className="indicators-list">
          {alert.reasons.map((reason, idx) => (
            <div key={idx} className={`indicator-item indicator-${reason.severity}`}>
              <button
                onClick={() => setExpandedReason(expandedReason === idx ? null : idx)}
                className="indicator-button"
              >
                <div className="indicator-content">
                  <div className="indicator-header">
                    <span className={`impact-badge impact-${reason.severity}`}>
                      {reason.impact}
                    </span>
                    <span className="reason-code">{reason.code}</span>
                  </div>
                  <p className="reason-message">{reason.message}</p>
                </div>
                <AlertTriangle className={`indicator-icon ${expandedReason === idx ? 'indicator-icon-expanded' : ''}`} />
              </button>
              
              {expandedReason === idx && (
                <div className="indicator-explanation">
                  <div className="explanation-box">
                    <p className="explanation-title">
                      <span className="explanation-bar" />
                      📋 Detailed Explanation
                    </p>
                    <p className="explanation-text">{reason.explanation}</p>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
        {alert.reasons.length === 0 && (
          <div className="no-indicators">
            <CheckCircle className="no-indicators-icon" />
            <p className="no-indicators-text">No fraud indicators detected. Transaction appears legitimate.</p>
          </div>
        )}
      </div>

      <div className="transaction-details">
        <h4 className="section-title">💳 Transaction Details</h4>
        <div className="details-box">
          {[
            { label: 'Account ID', value: alert.transaction.accountId, icon: '👤' },
            { label: 'Card', value: `****${alert.transaction.cardLast4}`, icon: '💳' },
            { label: 'Amount', value: `₹${alert.transaction.amount.toLocaleString('en-IN')}`, icon: '💰' },
            { label: 'Merchant', value: alert.transaction.merchant, icon: '🏪' },
            { label: 'Category', value: alert.transaction.category, icon: '📂' },
            { label: 'Location', value: alert.transaction.location.city, icon: '📍' },
            { label: 'Channel', value: alert.transaction.channel, icon: '📱' },
            { label: 'Device', value: alert.transaction.device || 'Unknown', icon: '💻' },
            { label: 'Browser', value: alert.transaction.browser || 'Unknown', icon: '🌐' },
            { label: 'IP Address', value: alert.transaction.ipAddress, icon: '🔌' },
            { label: 'Time', value: new Date(alert.transaction.timestamp).toLocaleString('en-IN', { 
              year: 'numeric',
              month: 'short', 
              day: '2-digit',
              hour: '2-digit', 
              minute: '2-digit',
              second: '2-digit'
            }), icon: '🕐' }
          ].map((item, idx) => (
            <div key={idx} className="detail-row">
              <span className="detail-row-label">
                <span className="detail-icon">{item.icon}</span>
                {item.label}
              </span>
              <span className="detail-row-value">{item.value}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="contributions-section">
        <h4 className="section-title">🎯 Model Feature Contributions</h4>
        <div className="contributions-list">
          {Object.entries(alert.mlScore.contributions).map(([key, value]) => (
            <div key={key} className="contribution-item">
              <div className="contribution-header">
                <span className="contribution-label">
                  {key.replace(/([A-Z])/g, ' $1').trim().replace(/^./, str => str.toUpperCase())}
                </span>
                <span className="contribution-value">{(value * 100).toFixed(1)}%</span>
              </div>
              <div className="contribution-bar-bg">
                <div
                  className="contribution-bar"
                  style={{ width: `${(value / alert.mlScore.score) * 100}%` }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      {alert.status === 'PENDING' && (
        <div className="action-section">
          <h4 className="section-title">⚡ Take Action</h4>
          <textarea
            value={comments}
            onChange={(e) => setComments(e.target.value)}
            placeholder="Add your analysis comments here... (e.g., Contacted customer, verified transaction, additional checks performed)"
            className="comments-textarea"
            rows={3}
          />
          <div className="action-buttons">
            <button
              onClick={() => handleAction(alert.id, 'APPROVED', comments)}
              className="action-btn action-btn-approve"
            >
              <CheckCircle className="action-btn-icon" />
              Approve
            </button>
            <button
              onClick={() => handleAction(alert.id, 'BLOCKED', comments)}
              className="action-btn action-btn-block"
            >
              <XCircle className="action-btn-icon" />
              Block
            </button>
            <button
              onClick={() => handleAction(alert.id, 'ESCALATED', comments)}
              className="action-btn action-btn-escalate"
            >
              <AlertTriangle className="action-btn-icon" />
              Escalate
            </button>
          </div>
        </div>
      )}

      {alert.status !== 'PENDING' && (
        <div className="status-panel">
          <div className="status-header">
            {alert.status === 'APPROVED' && <CheckCircle className="status-icon status-icon-approved" />}
            {alert.status === 'BLOCKED' && <Lock className="status-icon status-icon-blocked" />}
            {alert.status === 'ESCALATED' && <AlertTriangle className="status-icon status-icon-escalated" />}
            <span className="status-text">{alert.status}</span>
          </div>
          {alert.comments && (
            <div className="status-comments-box">
              <p className="status-comments-label">Analyst Comments:</p>
              <p className="status-comments">{alert.comments}</p>
            </div>
          )}
          <p className="status-timestamp">
            ✅ Reviewed by {alert.assignedTo} on {new Date(alert.reviewedAt).toLocaleString('en-IN')}
          </p>
        </div>
      )}
    </div>
  );
};

const StreamTab = ({ recentTransactions, isStreaming }) => {
  return (
    <div className="stream-container">
      <div className="stream-panel">
        <div className="stream-header">
          <h3 className="stream-title">📡 Live Transaction Stream</h3>
          {isStreaming ? (
            <div className="streaming-badge">
              <div className="streaming-dot" />
              <span className="streaming-text">Streaming Active</span>
            </div>
          ) : (
            <span className="streaming-paused">⏸️ Streaming Paused</span>
          )}
        </div>

        {recentTransactions.length > 0 ? (
          <div className="transactions-list">
            {recentTransactions.map((txn) => (
              <div key={txn.id} className="transaction-item">
                <div className="transaction-header">
                  <div className="transaction-info">
                    <div className={`transaction-dot ${txn.isFraudulent ? 'transaction-dot-fraud' : 'transaction-dot-clean'}`} />
                    <span className="transaction-id">{txn.id}</span>
                    {txn.isFraudulent && (
                      <span className="fraud-tag">🚨 FRAUD</span>
                    )}
                  </div>
                  <span className="transaction-time">
                    {new Date(txn.timestamp).toLocaleTimeString('en-IN')}
                  </span>
                </div>
                <div className="transaction-details-grid">
                  {[
                    { label: 'Account', value: txn.accountId, icon: '👤' },
                    { label: 'Amount', value: `₹${txn.amount.toLocaleString('en-IN')}`, icon: '💰' },
                    { label: 'Merchant', value: txn.merchant, icon: '🏪' },
                    { label: 'Location', value: txn.location.city, icon: '📍' }
                  ].map((item, idx) => (
                    <div key={idx} className="transaction-detail-box">
                      <p className="transaction-detail-label">
                        <span className="detail-icon-small">{item.icon}</span>
                        {item.label}
                      </p>
                      <p className="transaction-detail-value">{item.value}</p>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="no-stream">
            <div className="no-stream-icon">
              <Activity className="activity-icon" />
            </div>
            <h3 className="no-stream-title">No Active Stream</h3>
            <p className="no-stream-text">Click "Start Stream" to begin monitoring transactions</p>
          </div>
        )}
      </div>

      <div className="info-grid">
        {[
          {
            title: 'Kafka Topics',
            icon: Database,
            colorClass: 'info-blue',
            items: [
              { label: 'transactions', status: 'Active' },
              { label: 'scores', status: 'Active' },
              { label: 'alerts', status: 'Active' }
            ]
          },
          {
            title: 'ML Model',
            icon: Shield,
            colorClass: 'info-purple',
            items: [
              { label: 'Version', status: 'v3.2.0' },
              { label: 'Type', status: 'ONNX Runtime' },
              { label: 'Latency', status: '~45ms' }
            ]
          },
          {
            title: 'Performance',
            icon: Zap,
            colorClass: 'info-green',
            items: [
              { label: 'Throughput', status: '500/sec' },
              { label: 'Accuracy', status: '94.3%' },
              { label: 'Uptime', status: '99.9%' }
            ]
          }
        ].map((section, idx) => (
          <div key={idx} className="info-card">
            <div className={`info-card-border ${section.colorClass}`} />
            <div className="info-card-content">
              <div className="info-card-header">
                <div className={`info-icon-box ${section.colorClass}`}>
                  <section.icon className="info-icon" />
                </div>
                <h4 className="info-title">{section.title}</h4>
              </div>
              <div className="info-items">
                {section.items.map((item, itemIdx) => (
                  <div key={itemIdx} className="info-item">
                    <span className="info-item-label">{item.label}</span>
                    <span className="info-item-value">{item.status}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default FraudDetectionApp;