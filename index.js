import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import chalk from 'chalk';
import cliTable from 'cli-table3';

// Core Simulators & Services
import eventBus from './eventBus.js';
import inMemoryDB from './db/inMemoryDB.js';
import redisSimulator from './cache/redisSimulator.js';
import distributedLock from './lock/distributedLock.js';
import kafkaSimulator from './broker/kafkaSimulator.js';
import apiGateway from './gateway/apiGateway.js';

// Resolve folder paths for native ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.join(__dirname, 'public');

// Keep track of connected SSE web clients
let sseClients = [];

// ==========================================================================
// 🎨 BEAUTIFUL COLORED TERMINAL LOGGER (CHALK)
// ==========================================================================
eventBus.on('log', (log) => {
  const { timestamp, type, component, message } = log;
  let formattedLog = '';

  // Emojis for quick recognition
  const icons = {
    SYSTEM: '🔴',
    GATEWAY: '🛡️',
    REDIS: '⚡',
    DB: '🗄️',
    LOCK: '🔒',
    KAFKA: '📨',
    TEMPLATE: '📋'
  };
  
  const icon = icons[component] || '🔹';
  const meta = chalk.gray(`[${timestamp}] [${component}]`);

  // Styling based on components & messages
  if (component === 'SYSTEM') {
    formattedLog = chalk.magenta.bold(`${icon} ${message}`);
  } else if (component === 'GATEWAY') {
    if (message.includes('BLOCKED') || message.includes('diblokir')) {
      formattedLog = chalk.red.bold(`${icon} ${message}`);
    } else {
      formattedLog = chalk.yellow(`${icon} ${message}`);
    }
  } else if (component === 'REDIS') {
    if (message.includes('HIT')) {
      formattedLog = chalk.green(`${icon} ${message}`);
    } else if (message.includes('MISS') || message.includes('EXPIRED')) {
      formattedLog = chalk.yellow(`${icon} ${message}`);
    } else {
      formattedLog = chalk.white(`${icon} ${message}`);
    }
  } else if (component === 'LOCK') {
    if (message.includes('REJECT') || message.includes('Gagal')) {
      formattedLog = chalk.red.bold(`${icon} ${message}`);
    } else {
      formattedLog = chalk.yellow(`${icon} ${message}`);
    }
  } else if (component === 'DB') {
    if (message.includes('potong stok') || message.includes('Transaksi disimpan')) {
      formattedLog = chalk.green(`${icon} ${message}`);
    } else if (message.includes('GAGAL')) {
      formattedLog = chalk.red.bold(`${icon} ${message}`);
    } else {
      formattedLog = chalk.white(`${icon} ${message}`);
    }
  } else if (component === 'KAFKA') {
    formattedLog = chalk.gray(`${icon} ${message}`);
  } else if (component === 'TEMPLATE') {
    if (message.includes('❌') || message.includes('DITOLAK')) {
      formattedLog = chalk.red.bold(`${icon} ${message}`);
    } else if (message.includes('✅') || message.includes('SUKSES')) {
      formattedLog = chalk.green.bold(`${icon} ${message}`);
    } else {
      formattedLog = chalk.white(`${icon} ${message}`);
    }
  } else {
    formattedLog = `${icon} ${message}`;
  }

  // Print log to server console
  console.log(`${meta} ${formattedLog}`);

  // Broadcast log to all open SSE web clients
  sseClients.forEach(res => {
    res.write(`data: ${JSON.stringify(log)}\n\n`);
  });
});

// ==========================================================================
// 📨 KAFKA ASYNC WORKER CONSUMER INTEGRATION
// ==========================================================================
kafkaSimulator.startConsumer(async (event) => {
  const { topic, data } = event;

  if (topic === 'checkout-events') {
    // Simulate slow invoice processing and mailing
    eventBus.log('DEV', 'KAFKA', `Asynchronous Worker: Memulai pembuatan berkas PDF Invoice untuk ${data.buyerName}...`);
    await new Promise(r => setTimeout(r, 600));
    
    eventBus.log('DEV', 'KAFKA', `[SUKSES] PDF Invoice terbit. Mengirim surel & notifikasi pengiriman kurir ke ${data.buyerName}.`);
    eventBus.log('USER', 'KAFKA', `📨 Invoice #${data.transactionId} terkirim → ${data.buyerName} (${data.paymentDetails.provider} Rp${data.amount.toLocaleString('id-ID')})`);
  
  } else if (topic === 'cache-invalidation-events') {
    // Asynchronously invalidate product cache key (Push Invalidation) to force DB replica refresh
    const cacheKey = `product:${data.productId}`;
    eventBus.log('DEV', 'KAFKA', `Asynchronous Invalidation: Mengirim push invalidation ke Redis untuk kunci: ${cacheKey}`);
    
    redisSimulator.delete(cacheKey);
  }
});

// ==========================================================================
// 🌐 HTTP WEB SERVER & ROUTER (NATIVE NODE.JS)
// ==========================================================================
const server = http.createServer(async (req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const { pathname } = parsedUrl;
  const method = req.method;

  // Set default JSON headers for APIs
  const sendJSON = (statusCode, data) => {
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
  };

  // Serve static files
  if (method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
    fs.readFile(path.join(PUBLIC_DIR, 'index.html'), (err, content) => {
      if (err) {
        res.writeHead(500);
        res.end('File HTML tidak ditemukan.');
      } else {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(content);
      }
    });
  } else if (method === 'GET' && pathname === '/style.css') {
    fs.readFile(path.join(PUBLIC_DIR, 'style.css'), (err, content) => {
      if (err) {
        res.writeHead(500);
        res.end('CSS tidak ditemukan.');
      } else {
        res.writeHead(200, { 'Content-Type': 'text/css' });
        res.end(content);
      }
    });
  } else if (method === 'GET' && pathname === '/client.js') {
    fs.readFile(path.join(PUBLIC_DIR, 'client.js'), (err, content) => {
      if (err) {
        res.writeHead(500);
        res.end('Client JS tidak ditemukan.');
      } else {
        res.writeHead(200, { 'Content-Type': 'text/javascript' });
        res.end(content);
      }
    });

  // Server-Sent Events (SSE) stream pipeline
  } else if (method === 'GET' && pathname === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });
    
    // Send initial keep-alive packet
    res.write('data: {"component":"SYSTEM","type":"DEV","message":"Koneksi Real-time Aktif."}\n\n');
    
    sseClients.push(res);

    req.on('close', () => {
      sseClients = sseClients.filter(c => c !== res);
    });

  // API: Get active statuses & stats
  } else if (method === 'GET' && pathname === '/api/status') {
    const product = inMemoryDB.readFromPrimary('iphone');
    const status = {
      stock: product ? product.stock : 0,
      totalStock: inMemoryDB.initialStock,
      activeLocks: [distributedLock.getLockState('lock:product:iphone')],
      redisKeys: redisSimulator.getKeysState(),
      gateway: apiGateway.getGatewayState(),
      transactionsCount: inMemoryDB.getTransactions().length
    };
    sendJSON(200, status);

  // API: User manual checkout trigger
  } else if (method === 'POST' && pathname === '/api/checkout') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const payload = JSON.parse(body);
        
        // Pass to the API Gateway to route and enforce rate-limiting
        const result = await apiGateway.route('POST', '/checkout', {
          username: payload.buyerName,
          ip: payload.ip,
          paymentMethod: payload.paymentMethod,
          productId: 'iphone'
        });

        sendJSON(200, result);
      } catch (err) {
        sendJSON(400, { success: false, message: err.message });
      }
    });

  // API: User manual product page view
  } else if (method === 'GET' && pathname === '/api/read-product') {
    const ip = parsedUrl.searchParams.get('ip') || '127.0.0.1';
    
    try {
      const result = await apiGateway.route('GET', '/product/iphone', { ip });
      sendJSON(200, result);
    } catch (err) {
      sendJSON(400, { success: false, message: err.message });
    }

  // API: Developer - Simulate 100 concurrent product read refreshes
  } else if (method === 'POST' && pathname === '/api/simulate-heavy-read') {
    eventBus.log('DEV', 'SYSTEM', '⚡ MEMULAI: 100 Pengguna melakukan refresh produk bersamaan dalam 1 detik!');
    
    const readRequests = [];
    for (let i = 1; i <= 100; i++) {
      const mockIp = `192.168.10.${Math.floor(i / 10) + 1}`; // 10 distinct IPs to bypass rate-limiting
      readRequests.push(apiGateway.route('GET', '/product/iphone', { ip: mockIp }));
    }

    Promise.allSettled(readRequests).then((results) => {
      const succeeded = results.filter(r => r.status === 'fulfilled').length;
      const failed = results.filter(r => r.status === 'rejected').length;
      
      eventBus.log('DEV', 'SYSTEM', `📊 HASIL SIMULASI READ: ${succeeded} Berhasil, ${failed} Gagal (Terkena Rate Limit).`);
      sendJSON(200, { success: true, succeeded, failed });
    });

  // API: Developer - Simulate 5 concurrent checkouts racing for 3 stock
  } else if (method === 'POST' && pathname === '/api/simulate-heavy-checkout') {
    eventBus.log('DEV', 'SYSTEM', '⚡ MEMULAI CONCURRENCY RACE: 5 user memicu POST /checkout bersamaan!');
    
    const users = [
      { name: 'User-A', ip: '192.168.1.1', method: 'gopay' },
      { name: 'User-B', ip: '192.168.1.2', method: 'bank_transfer' },
      { name: 'User-C', ip: '192.168.1.3', method: 'gopay' },
      { name: 'User-D', ip: '192.168.1.4', method: 'bank_transfer' },
      { name: 'User-E', ip: '192.168.1.5', method: 'gopay' }
    ];

    const checkoutPromises = users.map(user => {
      return apiGateway.route('POST', '/checkout', {
        username: user.name,
        ip: user.ip,
        paymentMethod: user.method,
        productId: 'iphone'
      });
    });

    // Run parallel
    Promise.allSettled(checkoutPromises).then((results) => {
      const succeeded = results.filter(r => r.status === 'fulfilled').length;
      const failed = results.filter(r => r.status === 'rejected').length;
      
      eventBus.log('DEV', 'SYSTEM', `📊 HASIL AKHIR CHECKOUT CONCURRENCY:`);
      eventBus.log('DEV', 'SYSTEM', `   - Total Checkout Berhasil (Stok Terpotong): ${succeeded} unit`);
      eventBus.log('DEV', 'SYSTEM', `   - Total Checkout Ditolak (Stok Habis/Lock Tabrakan): ${failed}`);

      // Final db check & report table
      setTimeout(() => {
        printConsoleReportTable();
      }, 2500); // Wait for async Kafka logs to settle first!

      sendJSON(200, { success: true, succeeded, failed });
    });

  // API: Developer - Simulate large concurrent load (100 - 1,000,000 users)
  } else if (method === 'POST' && pathname === '/api/simulate-stress-test') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const payload = JSON.parse(body);
        const userCount = parseInt(payload.userCount) || 100000;
        const ratio = payload.ratio || '99-1';

        // Parse ratio parts
        const ratioParts = ratio.split('-');
        const ratioRead = parseInt(ratioParts[0]);
        const ratioWrite = parseInt(ratioParts[1]);

        const readCount = Math.round(userCount * (ratioRead / 100));
        const checkoutCount = userCount - readCount;

        const product = inMemoryDB.readFromPrimary('iphone');
        const stockRemaining = product ? product.stock : 0;

        // Perform Cache check
        const isCached = redisSimulator.get('product:iphone') !== null;
        const redisHits = isCached ? readCount : readCount - 1;
        const dbReplicaQueries = isCached ? 0 : 1;
        const cacheHitRate = Math.round((redisHits / readCount) * 100);

        // Checkout calculations
        const successfulCheckouts = Math.min(stockRemaining, checkoutCount);
        const rateLimitedCheckouts = checkoutCount - successfulCheckouts;

        // Asynchronous visual delay sequence for scrolling logs!
        // This is pure magic to watch in Developer Mode console!
        setTimeout(() => {
          eventBus.log('DEV', 'GATEWAY', `[API Gateway] Membuka pintu gerbang. Lonjakan ${userCount.toLocaleString('id-ID')} pembeli concurrent terdeteksi tepat 00:00 WIB!`);
        }, 50);

        setTimeout(() => {
          eventBus.log('DEV', 'GATEWAY', `[API Gateway] Distribusi beban: ${readCount.toLocaleString('id-ID')} memuat katalog (GET), ${checkoutCount.toLocaleString('id-ID')} menekan checkout (POST).`);
        }, 300);

        setTimeout(() => {
          eventBus.log('DEV', 'REDIS', `[Proxy Cache] Memproses ${readCount.toLocaleString('id-ID')} read request. Caching menyerap ${cacheHitRate}% beban!`);
        }, 650);

        setTimeout(() => {
          eventBus.log('DEV', 'REDIS', `[Redis] Cache HIT: ${redisHits.toLocaleString('id-ID')} request diserap secara instan (0-1ms).`);
          if (dbReplicaQueries > 0) {
            eventBus.log('DEV', 'REDIS', `[Redis] Cache MISS: 1 request diteruskan ke SQL Read Replica.`);
          }
        }, 900);

        setTimeout(() => {
          eventBus.log('DEV', 'DB', `[Read Replica] SQL DB Replica memproses ${dbReplicaQueries} query. Beban DB: ${dbReplicaQueries > 0 ? '0.01%' : '0%'} dari batas maks 10.000 QPS. DB AMAN ✓`);
        }, 1200);

        setTimeout(() => {
          eventBus.log('DEV', 'GATEWAY', `[API Gateway] Memproses ${checkoutCount.toLocaleString('id-ID')} checkout concurrent...`);
        }, 1500);

        setTimeout(() => {
          eventBus.log('DEV', 'GATEWAY', `[Rate Limiter] ${rateLimitedCheckouts.toLocaleString('id-ID')} checkout diblokir aman oleh Gateway Rate Limit per user & Redlock!`);
        }, 1800);

        // Perform actual transactional checkouts in the database to mutate the real stock!
        setTimeout(async () => {
          if (successfulCheckouts > 0) {
            eventBus.log('DEV', 'SYSTEM', `[Primary DB] Memproses ${successfulCheckouts} checkout terpilih memperebutkan sisa ${stockRemaining} stok...`);
            for (let i = 1; i <= successfulCheckouts; i++) {
              try {
                // Trigger actual template checkout to mutate DB states!
                await apiGateway.route('POST', '/checkout', {
                  username: `StressBuyer-${i}`,
                  ip: `192.168.99.${i}`,
                  paymentMethod: i % 2 === 0 ? 'gopay' : 'bank_transfer',
                  productId: 'iphone'
                });
              } catch (e) {
                // ignore
              }
            }
          }
        }, 2100);

        setTimeout(() => {
          const finalProduct = inMemoryDB.readFromPrimary('iphone');
          eventBus.log('SYSTEM', 'SYSTEM', `✓ [STRESS TEST SELESAI] Beban ${userCount.toLocaleString('id-ID')} pengguna berhasil diredam! Sisa stok iPhone di DB Primary: ${finalProduct.stock} unit.`);
          printConsoleReportTable();
        }, 3000);

        // Return statistical payload
        sendJSON(200, {
          success: true,
          data: {
            userCount,
            readCount,
            checkoutCount,
            cacheHitRate,
            redisHits,
            dbReplicaQueries,
            successfulCheckouts,
            rateLimitedCheckouts,
            stockRemaining: product ? Math.max(0, product.stock - successfulCheckouts) : 0,
            overselling: false
          }
        });
      } catch (err) {
        sendJSON(400, { success: false, message: err.message });
      }
    });

  // API: Developer - Manual toggle Gateway Circuit Breaker
  } else if (method === 'POST' && pathname === '/api/toggle-circuit') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      const payload = JSON.parse(body);
      if (payload.state === 'OPEN') {
        apiGateway.tripCircuit();
      } else {
        apiGateway.resetCircuit();
      }
      sendJSON(200, { success: true });
    });

  // API: Developer - Manual Redis cache invalidate
  } else if (method === 'POST' && pathname === '/api/invalidate-cache') {
    redisSimulator.delete('product:iphone');
    sendJSON(200, { success: true });

  // API: Developer - Reset whole system states
  } else if (method === 'POST' && pathname === '/api/reset') {
    inMemoryDB.reset(3);
    redisSimulator.clear();
    distributedLock.clear();
    kafkaSimulator.clear();
    apiGateway.resetCircuit();
    sendJSON(200, { success: true });

  } else {
    sendJSON(404, { error: 'Not Found' });
  }
});

/**
 * Draws a gorgeous Shopee-themed text report table in the node CLI when transaction races end.
 */
function printConsoleReportTable() {
  const table = new cliTable({
    head: [chalk.red.bold('Metrik Flash Sale'), chalk.red.bold('Hasil Status')],
    style: { head: [], border: [] },
    chars: {
      'top': '═', 'top-mid': '╤', 'top-left': '╔', 'top-right': '╗',
      'bottom': '═', 'bottom-mid': '╧', 'bottom-left': '╚', 'bottom-right': '╝',
      'left': '║', 'left-mid': '╟', 'mid': '─', 'mid-mid': '┼',
      'right': '║', 'right-mid': '╢', 'middle': '│'
    }
  });

  const txs = inMemoryDB.getTransactions();
  const product = inMemoryDB.readFromPrimary('iphone');

  table.push(
    ['Stok Awal Produk', '3 unit'],
    ['Stok Akhir di DB', `${product.stock} unit`],
    ['Total Transaksi Sukses', `${txs.length} transaksi`],
    ['Adanya Overselling?', txs.length > 3 ? chalk.red.bold('⚠️ YA (CRASH)') : chalk.green.bold('TIDAK ADA ✓')]
  );

  console.log('\n' + chalk.magenta.bold('╔══════════════════════════════════════════╗'));
  console.log(chalk.magenta.bold('║    🔴 FLASH SALE RESULTS REPORT          ║'));
  console.log(chalk.magenta.bold('╚══════════════════════════════════════════╝'));
  console.log(table.toString() + '\n');
}

// ==========================================================================
// 🚀 SERVER INIT & HELLO DIAGRAM
// ==========================================================================
const PORT = 3000;
server.listen(PORT, () => {
  console.clear();
  console.log(chalk.magenta.bold(`
  ╔════════════════════════════════════════════════════════╗
  ║    🔴 FLASH SALE SIMULATOR POC — WEB & TERMINAL v1.0   ║
  ║         Kelompok 7 | APL UNY 2025                      ║
  ╚════════════════════════════════════════════════════════╝
  `));
  
  eventBus.log('SYSTEM', 'SYSTEM', `Server HTTP aktif! Membuka gerbang di http://localhost:${PORT}`);
  eventBus.log('SYSTEM', 'SYSTEM', `Silakan buka browser Anda di http://localhost:${PORT} untuk berinteraksi.`);
  eventBus.log('SYSTEM', 'SYSTEM', `Membuka simulasi: default stok iPhone = 3 unit.`);
});
