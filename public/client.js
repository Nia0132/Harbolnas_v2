document.addEventListener('DOMContentLoaded', () => {
  // DOM Elements - Mode Controls
  const btnUserMode = document.getElementById('btn-user-mode');
  const btnDevMode = document.getElementById('btn-dev-mode');
  const appContainer = document.getElementById('app-container');
  const developerDeck = document.getElementById('developer-deck');

  // DOM Elements - Shopping UI
  const liveStock = document.getElementById('live-stock');
  const liveStockBar = document.getElementById('live-stock-bar');
  const checkoutForm = document.getElementById('checkout-form');
  const buyerNameInput = document.getElementById('buyer-name');
  const buyerIpInput = document.getElementById('buyer-ip');
  const btnCheckout = document.getElementById('btn-checkout');
  const btnViewProduct = document.getElementById('btn-view-product');

  // DOM Elements - Topo Nodes
  const nodeClient = document.getElementById('node-client');
  const nodeGateway = document.getElementById('node-gateway');
  const nodeCatalog = document.getElementById('node-catalog');
  const nodeRedis = document.getElementById('node-redis');
  const nodeReplica = document.getElementById('node-replica');
  const nodeTransaction = document.getElementById('node-transaction');
  const nodeLock = document.getElementById('node-lock');
  const nodePrimary = document.getElementById('node-primary');
  const nodeKafka = document.getElementById('node-kafka');
  const topoClientIp = document.getElementById('topo-client-ip');
  
  // DOM Elements - Developer Stats Panels
  const redisKeysList = document.getElementById('redis-keys-list');
  const lockLed = document.getElementById('lock-led');
  const lockText = document.getElementById('lock-text');
  const lockSubtext = document.getElementById('lock-subtext');
  const gatewayCircuitState = document.getElementById('gateway-circuit-state');
  const gatewayFailuresCount = document.getElementById('gateway-failures-count');
  const btnManualCircuit = document.getElementById('btn-manual-circuit');
  const btnResetCircuit = document.getElementById('btn-reset-circuit');
  const btnInvalidateCache = document.getElementById('btn-invalidate-cache');
  const topoLockStatus = document.getElementById('topo-lock-status');
  const topoRedisStatus = document.getElementById('topo-redis-status');
  const topoCircuitBadge = document.getElementById('topo-circuit-badge');

  // DOM Elements - CLI Console & Controllers
  const cliConsole = document.getElementById('cli-console');
  const btnClearTerm = document.getElementById('btn-clear-term');
  const btnSimulateHeavyRead = document.getElementById('btn-simulate-heavy-read');
  const btnSimulateHeavyCheckout = document.getElementById('btn-simulate-heavy-checkout');
  const btnResetPoc = document.getElementById('btn-reset-poc');

  // DOM Elements - Stress Test Simulator (Developer Exclusive)
  const stressUsersSlider = document.getElementById('stress-users-slider');
  const stressUsersInput = document.getElementById('stress-users-input');
  const stressRatio = document.getElementById('stress-ratio');
  const btnRunStress = document.getElementById('btn-run-stress');
  const stressResultsBox = document.getElementById('stress-results-box');
  const resTotalLoad = document.getElementById('res-total-load');
  const resRedisAbsorb = document.getElementById('res-redis-absorb');
  const resRedisPercent = document.getElementById('res-redis-percent');
  const resReplicaLoad = document.getElementById('res-replica-load');
  const resReplicaStatus = document.getElementById('res-replica-status');
  const resCheckoutSuccess = document.getElementById('res-checkout-success');
  const resCheckoutStock = document.getElementById('res-checkout-stock');
  const resCheckoutFiltered = document.getElementById('res-checkout-filtered');
  const resOversellStatus = document.getElementById('res-oversell-status');

  // DOM Elements - Modals
  const modalInvoice = document.getElementById('modal-invoice');
  const modalError = document.getElementById('modal-error');
  const invoiceTxId = document.getElementById('invoice-tx-id');
  const invoiceBuyerName = document.getElementById('invoice-buyer-name');
  const invoicePaymentMethod = document.getElementById('invoice-payment-method');
  const invoicePaymentId = document.getElementById('invoice-payment-id');
  const btnCloseInvoice = document.getElementById('btn-close-invoice');
  const errorMessage = document.getElementById('error-message');
  const btnCloseError = document.getElementById('btn-close-error');

  // Chart.js Cache Hit Rate Setup
  const ctx = document.getElementById('hitRateChart').getContext('2d');
  let totalRequests = 0;
  let cacheHits = 0;
  
  const hitRateChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: ['Start'],
      datasets: [{
        label: 'Hit Rate %',
        data: [0],
        borderColor: '#ff3b3b',
        backgroundColor: 'rgba(255, 59, 59, 0.05)',
        borderWidth: 2,
        tension: 0.4,
        fill: true,
        pointBackgroundColor: '#ff3b3b'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false }
      },
      scales: {
        y: {
          min: 0,
          max: 100,
          grid: { color: 'rgba(255, 255, 255, 0.05)' },
          ticks: { color: '#a1a1aa', font: { size: 9 } }
        },
        x: {
          grid: { display: false },
          ticks: { color: '#a1a1aa', font: { size: 8 } }
        }
      }
    }
  });

  // State Management variables
  let sseSource = null;
  let statusPollInterval = null;

  // ==========================================================================
  // VIEW MODE SWITCHING (USER MODE vs DEV MODE)
  // ==========================================================================
  btnUserMode.addEventListener('click', () => {
    btnUserMode.classList.add('active');
    btnDevMode.classList.remove('active');
    appContainer.classList.remove('layout-dev');
    appContainer.classList.add('layout-user');
    developerDeck.classList.add('hidden');
  });

  btnDevMode.addEventListener('click', () => {
    btnDevMode.classList.add('active');
    btnUserMode.classList.remove('active');
    appContainer.classList.remove('layout-user');
    appContainer.classList.add('layout-dev');
    developerDeck.classList.remove('hidden');
    // Scroll terminal console on expansion
    cliConsole.scrollTop = cliConsole.scrollHeight;
  });

  // Keep client IP synced on diagram
  buyerIpInput.addEventListener('input', () => {
    topoClientIp.textContent = buyerIpInput.value || '127.0.0.1';
  });

  // ==========================================================================
  // REAL-TIME EVENT STREAMING (SERVER-SENT EVENTS)
  // ==========================================================================
  function initEventSource() {
    if (sseSource) {
      sseSource.close();
    }

    sseSource = new EventSource('/api/events');

    sseSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        handleLogEvent(payload);
      } catch (err) {
        console.error('Gagal memproses data SSE:', err);
      }
    };

    sseSource.onerror = (err) => {
      console.warn('Koneksi SSE terputus, mencoba menghubungkan kembali...', err);
      appendTerminalLine('SYSTEM', 'DEV', '⚠️ Koneksi real-time ke web server terputus. Mencoba reconnect...');
    };
  }

  /**
   * Evaluates incoming SSE packets, prints to virtual terminal and animates topology nodes.
   * @param {object} log - Log metadata 
   */
  function handleLogEvent(log) {
    const { timestamp, type, component, message, metadata } = log;
    
    // Print to CLI Box
    appendTerminalLine(component, type, `[${timestamp}] [${component}] ${message}`);

    // Dynamic Microservice Node Flash
    flashNode(component, metadata);
  }

  /**
   * Appends a styled text line to the dark command terminal emulator.
   */
  function appendTerminalLine(component, type, text) {
    const line = document.createElement('div');
    line.className = 'term-line';
    
    // Color lines by components
    if (component === 'SYSTEM') {
      line.classList.add('text-green');
    } else if (component === 'GATEWAY') {
      if (text.includes('BLOCKED') || text.includes('Breaker')) {
        line.classList.add('text-red');
      } else {
        line.classList.add('text-yellow');
      }
    } else if (component === 'REDIS') {
      if (text.includes('HIT')) {
        line.classList.add('text-green');
      } else if (text.includes('MISS')) {
        line.classList.add('text-yellow');
      } else {
        line.classList.add('text-gray');
      }
    } else if (component === 'LOCK') {
      if (text.includes('REJECT') || text.includes('Gagal')) {
        line.classList.add('text-red');
      } else {
        line.classList.add('text-yellow');
      }
    } else if (component === 'KAFKA') {
      line.classList.add('text-gray-dim');
    } else if (component === 'TEMPLATE') {
      if (text.includes('❌')) {
        line.classList.add('text-red');
      } else if (text.includes('✅')) {
        line.classList.add('text-green');
      } else {
        line.classList.add('text-gray');
      }
    } else {
      line.classList.add('text-gray');
    }

    line.textContent = text;
    cliConsole.appendChild(line);
    
    // Auto-scroll console to bottom
    cliConsole.scrollTop = cliConsole.scrollHeight;
  }

  /**
   * Fires interactive LED style scale transitions on diagram nodes based on microservice active state.
   */
  function flashNode(component, metadata) {
    let node = null;
    let className = 'active-flash';

    if (component === 'GATEWAY') {
      node = nodeGateway;
      if (metadata && metadata.circuitState === 'OPEN') {
        nodeGateway.classList.add('active-error');
      } else {
        nodeGateway.classList.remove('active-error');
      }
    } else if (component === 'REDIS') {
      node = nodeRedis;
      // Track hit/miss events for graph calculations
      if (metadata && typeof metadata.isHit !== 'undefined') {
        totalRequests++;
        if (metadata.isHit) {
          cacheHits++;
        }
        updateHitRateChart();
      }
    } else if (component === 'DB') {
      if (metadata && metadata.latency) {
        node = nodeReplica; // replica read
      } else {
        node = nodePrimary; // primary write mutation
      }
    } else if (component === 'LOCK') {
      node = nodeLock;
    } else if (component === 'KAFKA') {
      node = nodeKafka;
    } else if (component === 'TEMPLATE') {
      node = nodeTransaction;
    }

    if (node) {
      node.classList.add(className);
      setTimeout(() => node.classList.remove(className), 800);
    }
  }

  /**
   * Updates Chart.js Cache Hit Rate diagram metrics.
   */
  function updateHitRateChart() {
    const rate = totalRequests === 0 ? 0 : Math.round((cacheHits / totalRequests) * 100);
    
    // Limit to showing the last 10 ticks to look like a rolling chart
    if (hitRateChart.data.labels.length > 12) {
      hitRateChart.data.labels.shift();
      hitRateChart.data.datasets[0].data.shift();
    }
    
    hitRateChart.data.labels.push(`#${totalRequests}`);
    hitRateChart.data.datasets[0].data.push(rate);
    hitRateChart.update();
  }

  // ==========================================================================
  // REAL-TIME SYSTEM STATES SYNCHRONIZATION (POLLING STATUS)
  // ==========================================================================
  async function fetchStatus() {
    try {
      const res = await fetch('/api/status');
      if (!res.ok) return;

      const data = await res.json();
      updateMeters(data);
    } catch (err) {
      console.error('Gagal mengambil status berkala:', err);
    }
  }

  /**
   * Redraws all visual health indicators on screen.
   */
  function updateMeters(status) {
    const { stock, totalStock, activeLocks, redisKeys, gateway, transactionsCount } = status;

    // 1. Update Stock Count & Progress Bar
    liveStock.textContent = stock;
    const stockPercent = totalStock === 0 ? 0 : (stock / totalStock) * 100;
    liveStockBar.style.width = `${stockPercent}%`;
    
    if (stock <= 0) {
      liveStockBar.style.background = '#71717a'; // gray
      liveStock.className = 'text-gray';
    } else if (stock === 1) {
      liveStockBar.style.background = '#f59e0b'; // yellow warning
      liveStock.className = 'text-red animate-pulse';
    } else {
      liveStockBar.style.background = 'linear-gradient(90deg, #ff3b3b, #e11d48)';
      liveStock.className = 'text-red';
    }

    // 2. Update Redlock status
    const iphoneLock = activeLocks.find(l => l.key === 'lock:product:iphone');
    if (iphoneLock && iphoneLock.isLocked) {
      lockLed.className = 'led led-red';
      lockText.textContent = 'LOCKED 🔒';
      lockText.className = 'text-red';
      lockSubtext.textContent = `Sedang dikunci oleh: ${iphoneLock.holderId}`;
      
      topoLockStatus.textContent = 'LOCKED';
      topoLockStatus.className = 'text-red font-semibold';
    } else {
      lockLed.className = 'led led-green';
      lockText.textContent = 'UNLOCKED 🔓';
      lockText.className = 'text-green';
      lockSubtext.textContent = 'Produk bebas bertransaksi';
      
      topoLockStatus.textContent = 'UNLOCKED';
      topoLockStatus.className = 'text-green';
    }

    // 3. Update Redis Keys state progress bars
    const cacheKey = redisKeys.find(k => k.key === 'product:iphone');
    if (cacheKey) {
      redisKeysList.innerHTML = `
        <div class="redis-key-row animate-scale">
          <div class="key-header">
            <span class="redis-key-name">${cacheKey.key}</span>
            <span class="redis-ttl-text">${Math.round(cacheKey.ttlRemainingMs / 1000)}s</span>
          </div>
          <div class="ttl-progress-bar-bg">
            <div class="ttl-progress-bar" style="width: ${cacheKey.percentRemaining}%;"></div>
          </div>
        </div>
      `;
      topoRedisStatus.textContent = `Cached (${Math.round(cacheKey.ttlRemainingMs / 1000)}s)`;
      topoRedisStatus.className = 'text-green';
    } else {
      redisKeysList.innerHTML = `<div class="empty-state text-gray-dim">Cache kosong. Lakukan simulasi read!</div>`;
      topoRedisStatus.textContent = 'Idle';
      topoRedisStatus.className = 'text-gray';
    }

    // 4. Update API Gateway circuit state
    if (gateway.circuitState === 'OPEN') {
      gatewayCircuitState.textContent = 'OPEN 🚨';
      gatewayCircuitState.className = 'badge badge-danger';
      gatewayFailuresCount.textContent = `${gateway.consecutiveFailures} / ${gateway.failureThreshold} Kegagalan (Penuh)`;
      gatewayFailuresCount.className = 'text-red font-semibold';
      
      topoCircuitBadge.textContent = 'Circuit OPEN';
      topoCircuitBadge.className = 'badge badge-danger';
      
      btnManualCircuit.classList.add('hidden');
      btnResetCircuit.classList.remove('hidden');
    } else {
      gatewayCircuitState.textContent = 'CLOSED ✓';
      gatewayCircuitState.className = 'badge badge-success';
      gatewayFailuresCount.textContent = `${gateway.consecutiveFailures} / ${gateway.failureThreshold} Kesalahan`;
      gatewayFailuresCount.className = 'text-green';
      
      topoCircuitBadge.textContent = 'Circuit CLOSED';
      topoCircuitBadge.className = 'badge badge-success';
      
      btnResetCircuit.classList.add('hidden');
      btnManualCircuit.classList.remove('hidden');
    }
  }

  // ==========================================================================
  // VISUAL TRIGGER ACTIONS (HTTP INTERFACE TRIGGERS)
  // ==========================================================================
  
  // Checkout (Shopper primary order click)
  btnCheckout.addEventListener('click', async () => {
    const buyerName = buyerNameInput.value.trim();
    const buyerIp = buyerIpInput.value.trim();
    const paymentMethod = document.querySelector('input[name="payment-method"]:checked').value;

    if (!buyerName || !buyerIp) {
      alert('Tolong lengkapi nama pembeli dan alamat IP!');
      return;
    }

    btnCheckout.disabled = true;
    btnCheckout.textContent = 'MEMPROSES CHECKOUT...';

    // Flash Client node
    nodeClient.classList.add('active-flash');
    setTimeout(() => nodeClient.classList.remove('active-flash'), 800);

    try {
      const res = await fetch('/api/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ buyerName, ip: buyerIp, paymentMethod })
      });

      const body = await res.json();
      
      if (res.ok && body.success) {
        // Show Invoice Receipt Modal
        const tx = body.data;
        invoiceTxId.textContent = tx.id;
        invoiceBuyerName.textContent = tx.buyerName;
        invoicePaymentMethod.textContent = `${tx.paymentProvider} (${tx.paymentMethod})`;
        invoicePaymentId.textContent = tx.paymentId;
        
        modalInvoice.classList.remove('hidden');
      } else {
        // Show error Modal
        errorMessage.textContent = body.message || 'Pembelian ditolak oleh sistem.';
        modalError.classList.remove('hidden');
      }
    } catch (err) {
      errorMessage.textContent = 'Koneksi jaringan bermasalah saat checkout.';
      modalError.classList.remove('hidden');
    } finally {
      btnCheckout.disabled = false;
      btnCheckout.textContent = '⚡ BELI SEKARANG (CHECKOUT)';
      fetchStatus(); // immediate sync
    }
  });

  // View Catalog Product Halaman (Simulasi Read)
  btnViewProduct.addEventListener('click', async () => {
    const buyerIp = buyerIpInput.value.trim();
    btnViewProduct.disabled = true;

    // Flash Client node
    nodeClient.classList.add('active-flash');
    setTimeout(() => nodeClient.classList.remove('active-flash'), 800);

    try {
      const res = await fetch(`/api/read-product?ip=${buyerIp}`);
      const body = await res.json();
      
      if (!res.ok) {
        // limit exceed error alert
        errorMessage.textContent = body.message || 'Gagal mengambil data produk.';
        modalError.classList.remove('hidden');
      }
    } catch (err) {
      console.error(err);
    } finally {
      btnViewProduct.disabled = false;
      fetchStatus();
    }
  });

  // Manual Trigger: 100 Read requests in parallel
  btnSimulateHeavyRead.addEventListener('click', async () => {
    btnSimulateHeavyRead.disabled = true;
    appendTerminalLine('SYSTEM', 'DEV', '🚀 Memulai pengujian beban: Mengirim 100 GET /product/iphone secara bersamaan...');
    
    try {
      await fetch('/api/simulate-heavy-read', { method: 'POST' });
    } catch (err) {
      console.error(err);
    } finally {
      btnSimulateHeavyRead.disabled = false;
      fetchStatus();
    }
  });

  // Manual Trigger: 5 concurrent checkouts racing for stock
  btnSimulateHeavyCheckout.addEventListener('click', async () => {
    btnSimulateHeavyCheckout.disabled = true;
    appendTerminalLine('SYSTEM', 'DEV', '🔥 Memulai Simulasi CONCURRENT RACE: 5 user memicu checkout secepat kilat tepat 00:00 WIB...');
    
    try {
      await fetch('/api/simulate-heavy-checkout', { method: 'POST' });
    } catch (err) {
      console.error(err);
    } finally {
      btnSimulateHeavyCheckout.disabled = false;
      fetchStatus();
    }
  });

  // Manual trigger: Force Open Circuit Breaker
  btnManualCircuit.addEventListener('click', async () => {
    try {
      await fetch('/api/toggle-circuit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ state: 'OPEN' })
      });
      fetchStatus();
    } catch (err) {
      console.error(err);
    }
  });

  // Manual trigger: Reset Circuit Breaker to closed
  btnResetCircuit.addEventListener('click', async () => {
    try {
      await fetch('/api/toggle-circuit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ state: 'CLOSED' })
      });
      fetchStatus();
    } catch (err) {
      console.error(err);
    }
  });

  // Invalidate Redis Cache
  btnInvalidateCache.addEventListener('click', async () => {
    try {
      await fetch('/api/invalidate-cache', { method: 'POST' });
      fetchStatus();
    } catch (err) {
      console.error(err);
    }
  });

  // Reset PoC states
  btnResetPoc.addEventListener('click', async () => {
    if (!confirm('Apakah Anda yakin ingin me-reset seluruh keadaan database, cache, dan lock simulator?')) return;
    
    try {
      await fetch('/api/reset', { method: 'POST' });
      
      // Reset local hit rate variables
      totalRequests = 0;
      cacheHits = 0;
      hitRateChart.data.labels = ['Start'];
      hitRateChart.data.datasets[0].data = [0];
      hitRateChart.update();
      
      // Clear results displayer
      stressResultsBox.classList.add('hidden');
      
      appendTerminalLine('SYSTEM', 'DEV', '🔄 BERHASIL RESET: Keadaan database dikembalikan ke awal (Stok: 3). Cache kosong.');
      fetchStatus();
    } catch (err) {
      console.error(err);
    }
  });

  // Sync range slider and number input
  stressUsersSlider.addEventListener('input', () => {
    stressUsersInput.value = stressUsersSlider.value;
  });
  
  stressUsersInput.addEventListener('input', () => {
    let val = parseInt(stressUsersInput.value) || 100;
    if (val < 100) val = 100;
    if (val > 1000000) val = 1000000;
    stressUsersInput.value = val;
    stressUsersSlider.value = val;
  });

  // Trigger Mass Stress Test (100 - 1,000,000 users)
  btnRunStress.addEventListener('click', async () => {
    const userCount = parseInt(stressUsersInput.value) || 100000;
    const ratio = stressRatio.value;

    btnRunStress.disabled = true;
    btnRunStress.textContent = '🚀 MENJALANKAN STRESS TEST MASSAL... 💥';
    stressResultsBox.classList.add('hidden');

    appendTerminalLine('SYSTEM', 'DEV', `💥 MEMULAI STRESS TEST MASSAL: Menyemburkan ${userCount.toLocaleString('id-ID')} request secara bersamaan!`);

    // Flash Client and Gateway nodes visually on diagram
    nodeClient.classList.add('active-flash');
    nodeGateway.classList.add('active-flash');
    setTimeout(() => {
      nodeClient.classList.remove('active-flash');
      nodeGateway.classList.remove('active-flash');
    }, 1000);

    try {
      const res = await fetch('/api/simulate-stress-test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userCount, ratio })
      });
      
      const body = await res.json();
      
      if (res.ok && body.success) {
        const data = body.data;
        
        // Display visual report tiles
        resTotalLoad.textContent = `${data.userCount.toLocaleString('id-ID')} req`;
        resRedisAbsorb.textContent = `${data.redisHits.toLocaleString('id-ID')} req`;
        resRedisPercent.textContent = `(${data.cacheHitRate}%)`;
        resReplicaLoad.textContent = `${data.dbReplicaQueries.toLocaleString('id-ID')} req`;
        
        resReplicaStatus.textContent = data.dbReplicaQueries > 10000 ? 'BEBAN TINGGI ⚠️' : 'Aman ✓';
        resReplicaStatus.className = data.dbReplicaQueries > 10000 ? 'text-yellow font-semibold' : 'text-green';
        
        resCheckoutSuccess.textContent = `${data.successfulCheckouts} unit`;
        resCheckoutSuccess.className = data.successfulCheckouts > 0 ? 'text-green' : 'text-gray';
        resCheckoutStock.textContent = data.stockRemaining === 0 ? 'Stok Terjual Habis' : `Sisa Stok: ${data.stockRemaining}`;
        
        resCheckoutFiltered.textContent = `${data.rateLimitedCheckouts.toLocaleString('id-ID')} req`;
        resOversellStatus.textContent = data.overselling ? '⚠️ TERJADI OVERSELL' : 'TIDAK ADA ✓';
        resOversellStatus.className = data.overselling ? 'text-red font-bold' : 'text-green';

        // Reveal results container
        stressResultsBox.classList.remove('hidden');
        
        // Feed Chart.js hit rate diagram
        totalRequests += data.readCount;
        cacheHits += data.redisHits;
        updateHitRateChart();
      } else {
        appendTerminalLine('SYSTEM', 'DEV', `❌ Gagal menjalankan stress test: ${body.message || 'Error server'}`);
      }
    } catch (err) {
      appendTerminalLine('SYSTEM', 'DEV', `❌ Gagal koneksi saat stress test: ${err.message}`);
    } finally {
      btnRunStress.disabled = false;
      btnRunStress.textContent = '🚀 LUNCURKAN SIMULASI BEBAN MASSAL (STRESS TEST) 💥';
      fetchStatus();
    }
  });

  // Modal Closures
  btnCloseInvoice.addEventListener('click', () => {
    modalInvoice.classList.add('hidden');
  });

  btnCloseError.addEventListener('click', () => {
    modalError.classList.add('hidden');
  });

  btnClearTerm.addEventListener('click', () => {
    cliConsole.innerHTML = '<div class="term-line text-gray-dim">[SYSTEM] Terminal logs cleared.</div>';
  });

  // ==========================================================================
  // INITIALIZATIONS
  // ==========================================================================
  initEventSource();
  fetchStatus();

  // Start polling statuses every 500ms for smooth live counts / bar transitions
  statusPollInterval = setInterval(fetchStatus, 500);

  window.addEventListener('beforeunload', () => {
    if (sseSource) sseSource.close();
    clearInterval(statusPollInterval);
  });
});
