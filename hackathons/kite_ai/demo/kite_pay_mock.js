#!/usr/bin/env node
/**
 * Kite Pay Mock Server - x402 Payment Simulation
 * 
 * Simulates the kite.pay() x402 payment flow:
 * - Server responds with HTTP 402 (Payment Required)
 * - Includes X-PAYMENT header with payment details
 * - Facilitator settlement simulation
 * 
 * Usage:
 *   node kite_pay_mock.js
 *   # Server runs on http://localhost:8080
 */

import http from 'node:http';
import crypto from 'node:crypto';

const PORT = process.env.PORT || 8080;
const FACILITATOR_ADDRESS = '0xFacilitator1234567890abcdefABCDEF1234567890Ab';

// In-memory payment log
const paymentLog = [];

/**
 * Generate a payment request ID
 */
function generatePaymentId() {
  return 'pay_' + crypto.randomBytes(8).toString('hex');
}

/**
 * Create X-PAYMENT header value
 */
function createPaymentHeader(paymentRequest) {
  return JSON.stringify(paymentRequest);
}

/**
 * Parse X-PAYMENT header
 */
function parsePaymentHeader(headerValue) {
  try {
    return JSON.parse(headerValue);
  } catch {
    return null;
  }
}

/**
 * Simulate facilitator settlement
 */
async function settlePayment(paymentRequest) {
  console.log(`\n💱 [FACILITATOR] Processing settlement...`);
  console.log(`   Payment ID: ${paymentRequest.id}`);
  console.log(`   Amount: ${paymentRequest.amount} ${paymentRequest.currency}`);
  console.log(`   From: ${paymentRequest.from}`);
  console.log(`   To: ${paymentRequest.to}`);
  
  // Simulate async settlement
  await new Promise(resolve => setTimeout(resolve, 100));
  
  paymentRequest.status = 'settled';
  paymentRequest.settledAt = Date.now();
  paymentRequest.facilitatorTx = '0x' + crypto.randomBytes(32).toString('hex');
  
  console.log(`   ✅ Settled! Facilitator TX: ${paymentRequest.facilitatorTx}`);
  
  return paymentRequest;
}

/**
 * Handle incoming HTTP requests
 */
async function handleRequest(req, res) {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const method = req.method;
  
  console.log(`\n🌐 ${method} ${url.pathname}`);
  
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-PAYMENT, X-REQUEST-ID');
  
  if (method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  
  // Health check
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', timestamp: Date.now() }));
    return;
  }
  
  // Get payment status
  if (url.pathname === '/payments' && method === 'GET') {
    const paymentId = url.searchParams.get('id');
    
    if (paymentId) {
      const payment = paymentLog.find(p => p.id === paymentId);
      if (payment) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(payment));
        return;
      }
    }
    
    // Return all payments
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ payments: paymentLog }));
    return;
  }
  
  // Simulate premium API endpoint (requires payment)
  if (url.pathname === '/api/v1/signals' && method === 'GET') {
    const paymentHeader = req.headers['x-payment'];
    const requestId = req.headers['x-request-id'];
    
    console.log(`   X-PAYMENT: ${paymentHeader ? 'present' : 'missing'}`);
    console.log(`   X-REQUEST-ID: ${requestId || 'missing'}`);
    
    if (paymentHeader) {
      const payment = parsePaymentHeader(paymentHeader);
      
      if (payment && payment.status === 'settled') {
        // Payment already settled, return premium signals
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          signals: [
            { id: 'sig_001', asset: 'KITE', direction: 'bullish', confidence: 0.87, ttl: 3600 },
            { id: 'sig_002', asset: 'BTC', direction: 'bullish', confidence: 0.82, ttl: 3600 },
          ],
          source: 'volva-causal-engine',
          timestamp: Date.now(),
        }));
        return;
      }
      
      if (payment && payment.status === 'pending') {
        // Settle the payment
        await settlePayment(payment);
        
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          signals: [
            { id: 'sig_001', asset: 'KITE', direction: 'bullish', confidence: 0.87, ttl: 3600 },
            { id: 'sig_002', asset: 'BTC', direction: 'bullish', confidence: 0.82, ttl: 3600 },
          ],
          source: 'volva-causal-engine',
          timestamp: Date.now(),
        }));
        return;
      }
    }
    
    // No valid payment - return 402 with payment request
    const paymentRequest = {
      id: generatePaymentId(),
      amount: '1.00',
      currency: 'USDC',
      description: 'Premium causal signal subscription',
      from: '0xUserWalletAddress...',
      to: FACILITATOR_ADDRESS,
      status: 'pending',
      timestamp: Date.now(),
      expiresAt: Date.now() + 300000, // 5 minutes
    };
    
    // Log the payment request
    paymentLog.push(paymentRequest);
    
    console.log(`\n💳 [SERVER] Payment Required`);
    console.log(`   Payment ID: ${paymentRequest.id}`);
    console.log(`   Amount: ${paymentRequest.amount} ${paymentRequest.currency}`);
    
    res.writeHead(402, {
      'Content-Type': 'application/json',
      'X-PAYMENT': createPaymentHeader(paymentRequest),
      'X-REQUEST-ID': paymentRequest.id,
    });
    res.end(JSON.stringify({
      error: 'Payment Required',
      message: `Subscribe to premium signals for ${paymentRequest.amount} ${paymentRequest.currency}/month`,
      paymentRequest,
      subscribeUrl: `https://kite.ai/subscribe?plan=premium&utm_source=volva`,
    }));
    return;
  }
  
  // Premium signal with explicit payment body
  if (url.pathname === '/api/v1/signals/premium' && method === 'POST') {
    let body = '';
    
    req.on('data', chunk => { body += chunk; });
    
    await new Promise(resolve => req.on('end', resolve));
    
    let paymentData;
    try {
      paymentData = JSON.parse(body);
    } catch {
      paymentData = {};
    }
    
    const paymentRequest = {
      id: generatePaymentId(),
      amount: paymentData.amount || '1.00',
      currency: paymentData.currency || 'USDC',
      description: paymentData.description || 'Premium causal signal subscription',
      from: paymentData.wallet || '0xUserWallet...',
      to: FACILITATOR_ADDRESS,
      status: 'pending',
      timestamp: Date.now(),
      expiresAt: Date.now() + 300000,
    };
    
    paymentLog.push(paymentRequest);
    
    console.log(`\n💳 [SERVER] Processing payment request...`);
    console.log(`   Amount: ${paymentRequest.amount} ${paymentRequest.currency}`);
    
    // Auto-settle for demo purposes
    await settlePayment(paymentRequest);
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      success: true,
      payment: paymentRequest,
      signals: [
        { id: 'sig_prem_001', asset: 'KITE', direction: 'bullish', confidence: 0.91, ttl: 7200, source: 'volva-premium' },
        { id: 'sig_prem_002', asset: 'ETH', direction: 'neutral', confidence: 0.68, ttl: 7200, source: 'volva-premium' },
      ],
    }));
    return;
  }
  
  // Payment settlement webhook
  if (url.pathname === '/api/v1/payments/settle' && method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    await new Promise(resolve => req.on('end', resolve));
    
    let settlement;
    try {
      settlement = JSON.parse(body);
    } catch {
      settlement = {};
    }
    
    console.log(`\n💱 [SETTLEMENT] Received webhook...`);
    console.log(`   Payment ID: ${settlement.paymentId}`);
    console.log(`   TX Hash: ${settlement.txHash}`);
    
    const payment = paymentLog.find(p => p.id === settlement.paymentId);
    if (payment) {
      payment.status = 'settled';
      payment.settledAt = Date.now();
      payment.facilitatorTx = settlement.txHash;
    }
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ received: true }));
    return;
  }
  
  // Default: 404
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not Found' }));
}

// ============================================================================
// Main
// ============================================================================

const server = http.createServer(handleRequest);

server.listen(PORT, () => {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('   Kite Pay Mock Server - x402 Payment Simulation             ');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`\n🟢 Server running on http://localhost:${PORT}`);
  console.log('\n📡 Available Endpoints:');
  console.log('   GET  /health                  - Health check');
  console.log('   GET  /payments                 - List all payments');
  console.log('   GET  /api/v1/signals           - Get signals (returns 402 if unpaid)');
  console.log('   POST /api/v1/signals/premium   - Get premium signals (auto-settle)');
  console.log('   POST /api/v1/payments/settle   - Settlement webhook');
  console.log('\n💳 Payment Flow:');
  console.log('   1. Client requests /api/v1/signals');
  console.log('   2. Server returns HTTP 402 with X-PAYMENT header');
  console.log('   3. Client includes X-PAYMENT header in subsequent requests');
  console.log('   4. Server returns premium signals');
  console.log('\n🔍 Payment Log:');
  console.log('   GET /payments to view all payment requests\n');
});

server.on('error', (err) => {
  console.error('❌ Server error:', err.message);
  process.exit(1);
});
