# 🔴 PoC Simulasi Flash Sale Harbolnas — Studi Kasus 7
### **Arsitektur Perangkat Lunak | Universitas Negeri Yogyakarta (UNY) 2025**

Proyek ini adalah *Proof of Concept* (PoC) interaktif berbasis **Node.js** dan **Web Dashboard** yang menyimulasikan solusi arsitektur microservice berkinerja tinggi untuk meredam lonjakan trafik ekstrim saat *Flash Sale Harbolnas* (misal: memperebutkan 100 unit iPhone seharga Rp10.000 oleh jutaan pengguna concurrent tepat pukul 00:00 WIB).

PoC ini memadukan **Colored Terminal Logging** dengan **Interactive Web Dashboard** bersistem dual-mode (**Mode Pengguna** dan **Mode Developer**) untuk mendemonstrasikan peredaman beban secara visual, *real-time*, dan asinkron.

---

## 🏗️ Topologi Arsitektur Sistem

Sistem memisahkan beban baca (*Read Heavy*) dengan operasi tulis transaksional (*Write/Checkout Isolation*):

```
                       [ 👤 Client Browser / CLI ]
                                    │
                                    ▼ (HTTP / API)
                            [ 🛡️ API Gateway ]
                                    │
          ┌─────────────────────────┴─────────────────────────┐
          │ (99% Traffic: GET Halaman Produk)                  │ (1% Traffic: POST Checkout)
          ▼                                                   ▼
  [ 📋 Catalog Service ]                             [ 💳 Transaction Service ]
          │                                                   │
          ├──────────────┐ (Cache Miss)                       ▼ (Template Method)
          ▼ (Cache Hit)  ▼                                [ 🔒 Redlock Mutex ]
   [ ⚡ Redis Cache ] [ 📊 DB Read Replica ]                   │ (Commit DB)
                                                              ▼
                                                      [ 🗄️ DB Primary SQL ]
                                                              │
                                                              ▼ (Async Event)
                                                      [ 📨 Kafka Broker ]
                                                              │
                                            ┌─────────────────┴─────────────────┐
                                            ▼ (Async Task)                      ▼ (Push Invalidate)
                                    [ ⚙️ Invoice Worker ]              [ 🗑️ Cache Invalidation ]
```

---

## ⚡ Fitur Utama & Mitigasi Concurrency

1. **Read/Write Segregation (Pemisahan Baca/Tulis)**:
   * GET/Membaca produk dilayani oleh **Catalog Service** mengarah ke **Redis Cache** dan jatuh ke **SQL DB Read Replica**.
   * POST/Checkout dilayani oleh **Transaction Service** mengarah ke **DB Primary SQL** dengan pengamanan ketat.
2. **Double-Checked Locking & Redlock (`lock/distributedLock.js`)**:
   * Mutex terdistribusi mencegah kondisi balapan (*race conditions* / *overselling*). Stok database terjamin aman dan tidak akan pernah bocor melebihi kapasitas unit yang disediakan.
3. **Mitigasi Cache Stampede (`cache/redisSimulator.js`)**:
   * Redis cache simulator dilengkapi **TTL Jitter** (`base TTL (30s) + random (0-10s)`) agar jutaan kunci produk tidak kedaluwarsa serentak yang dapat meruntuhkan database replica (*cashing stampede*).
4. **Asynchronous Kafka Pipeline (`broker/kafkaSimulator.js`)**:
   * Operasi berat seperti render PDF invoice, pengiriman surel, dan *push cache invalidation* didekopel secara asinkron lewat antrean broker FIFO Kafka untuk membebaskan thread utama pembeli.
5. **Gateway Rate Limiting & Circuit Breaker (`gateway/apiGateway.js`)**:
   * **Rate Limiter**: Menapis bot dan banjir request (Max 10 req/s per IP pada katalog, Max 1 req/s per User pada checkout).
   * **Circuit Breaker**: Membuka gerbang bypass (*OPEN*) saat layanan downstream down, menyajikan offline fallback lokal instan untuk mengamankan resource backend dari kejatuhan total.

---

## 📂 Struktur File Proyek

```
flash-sale-poc/
├── run.bat                    ← Script otomatis Windows (Double-click untuk run!)
├── package.json                ← Konfigurasi komponen dependensi & ESM
├── index.js                    ← Web Server HTTP, router API, & Terminal logger
├── eventBus.js                 ← Pipeline Log pusat penghubung Node.js & Web SSE
├── db/
│   └── inMemoryDB.js           ← Simulasi PostgreSQL (Primary vs Read Replica)
├── cache/
│   └── redisSimulator.js       ← Simulasi Redis Cache (Jitter TTL & Lazy Eviction)
├── broker/
│   └── kafkaSimulator.js       ← Simulasi Kafka Broker (Asynchronous consumer loop)
├── lock/
│   └── distributedLock.js      ← Simulasi Distributed Lock Redlock (Mutex)
├── patterns/
│   └── checkoutTemplate.js     ← Implementasi Template Method Pattern (GoPay vs Bank)
├── services/
│   ├── catalogService.js       ← Catalog Service (Implementasi Proxy Pattern)
│   └── transactionService.js   ← Transaction Service (Checkout orchestrator)
├── gateway/
│   └── apiGateway.js           ← API Gateway (Rate Limiter & Circuit Breaker)
└── public/                     ← Frontend Web Dashboard
    ├── index.html              ← Layout HTML dual-mode (User & Developer)
    ├── style.css               ← Desain dark mode glassmorphism
    └── client.js               ← Handler EventSource (SSE), Chart.js, & interaksi
```

---

## 🛠️ Pola Desain (Design Patterns) yang Didemonstrasikan

### 1. Template Method Pattern (`patterns/checkoutTemplate.js`)
Enkapsulasi algoritma transaksional yang kaku dan tidak boleh diubah urutannya:
1. `cekStok(productId)` ➡️ Wajib ada stok tersedia.
2. `kunciStok(productId)` ➡️ Mengunci kunci Redlock terdistribusi.
3. `prosesPayment(userId, amount)` ➡️ Hook abstrak yang di-override oleh subclass (`GoPayCheckout` dan `BankTransferCheckout`) untuk mensimulasikan latensi jaringan API gateway perbankan.
4. `terbitkanInvoice(...)` ➡️ Commit stok di Primary DB, kirim event Kafka, dan lepas lock.

### 2. Proxy Pattern (`services/catalogService.js`)
Mengontrol akses pembacaan katalog produk. Objek `CatalogService` bertindak sebagai Proxy penengah bagi Database Read Replica, mengecek `redisSimulator` terlebih dahulu untuk memberikan *Cache Hit* super cepat sebelum membebani database fisik (*Cache Miss*).

---

## 🚀 Cara Menjalankan Aplikasi (Windows)

Anda dapat menjalankan seluruh aplikasi beserta instalasi dependensinya hanya dengan **satu langkah mudah**:

### 🔹 Opsi A: Double-Click File Explorer (Sangat Direkomendasikan)
1. Buka folder proyek `flash-sale-poc` di File Explorer Windows Anda.
2. Cari berkas bernama **`run.bat`**.
3. **Double-click** (klik ganda) berkas `run.bat` tersebut.
4. Jendela Command Prompt akan terbuka, menginstal komponen dependensi (`npm install`), lalu langsung menjalankan server lokal.

### 🔹 Opsi B: Melalui Terminal (Command Prompt / PowerShell)
1. Buka terminal dan arahkan ke direktori proyek:
   ```cmd
   cd C:\Users\...\....\...\...\flash-sale-poc
   ```
2. Jalankan berkas batch:
   ```cmd
   .\run.bat
   ```

---

## 🖥️ Cara Melakukan Demonstrasi Interaktif

1. Buka browser internet Anda dan akses dashboard lokal:
   🔗 **[http://localhost:3000](http://localhost:3000)**
2. **Uji Mode Pengguna (🛒)**:
   * Masukkan nama pembeli, pilih metode pembayaran, dan klik tombol merah besar **"Beli Sekarang ⚡"**.
   * Lihat invoice belanjaan tercetak indah di layar Anda.
3. **Uji Mode Developer (💻) - Aktifkan di pojok kanan atas**:
   * Layar akan terbelah memperlihatkan diagram topologi mikroservis aktif, progress bar sisa waktu TTL Redis, dan lampu indikator LED Redlock.
   * **Simulasi Lonjakan Beban Ekstrim**: Geser slider Stress Test ke angka **100.000** atau **1.000.000** concurrent users.
   * Klik **"LUNCURKAN SIMULASI BEBAN MASSAL (STRESS TEST) 🚀"**.
   * Saksikan log asinkron bergulir cepat di konsol hitam-neon, lampu diagram topologi berkedip dinamis, grafik *Cache Hit Rate* ter-update di Chart.js, serta panel analisis ketahanan yang membuktikan **0% Overselling**!
