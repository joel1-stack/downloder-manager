const path = require('path');
const DatabaseManager = require(path.join(__dirname, 'database'));
const ResourceAnalyzer = require(path.join(__dirname, 'resourceAnalyzer'));
const RetryManager = require(path.join(__dirname, 'retryManager'));
const BandwidthGovernor = require(path.join(__dirname, 'bandwidthGovernor'));
const VerificationEngine = require(path.join(__dirname, 'verificationEngine'));
const Scheduler = require(path.join(__dirname, 'scheduler'));
const http = require('http');
const https = require('https');
const fs = require('fs');

class DownloadManager {
  constructor(opts = {}) {
    this.db = new DatabaseManager(opts.dbPath || './download_manager.db');
    this.analyzer = new ResourceAnalyzer();
    this.retry = new RetryManager({ maxRetries: opts.maxRetries || 5 });
    this.bandwidth = new BandwidthGovernor({ globalLimit: opts.bandwidthLimit || 0 });
    this.verification = new VerificationEngine();
    this.scheduler = new Scheduler({ maxConcurrent: opts.maxConcurrent || 4 });
    this.chunkSize = opts.chunkSize || 1024 * 1024;
    this.speedTracker = new Map();
    this._recover();
  }

  _recover() {
    const unfinished = this.db.getAllDownloads().filter(d => !['COMPLETED', 'CANCELLED'].includes(d.status));
    unfinished.forEach(d => {
      this.db.updateDownload(d.id, { status: 'PAUSED' });
      this.scheduler.enqueue(d.id, d.priority || 0);
    });
    if (unfinished.length) console.log(`[Recovery] ${unfinished.length} downloads recovered`);
  }

  // === PUBLIC API ===

  startDownload(url, opts = {}) {
    try { new URL(url); } catch(e) { throw new Error('Invalid URL'); }
    const filename = opts.filename || this._extractFilename(url);
    const priority = opts.priority || 0;
    const downloadId = this.db.createDownload(url, { filename, total_size: opts.total_size || 0, priority });
    this.db.addEvent(downloadId, 'DOWNLOAD_CREATED', { url, priority });
    this.scheduler.enqueue(downloadId, priority);
    this._analyzeAndStart(downloadId, url);
    return { id: downloadId, status: 'IDLE', message: 'Analyzing resource...' };
  }

  pauseDownload(id) {
    const dl = this.db.getDownload(id);
    if (!dl) throw new Error('Not found');
    this.db.updateDownload(id, { status: 'PAUSED' });
    this.scheduler.dequeue(id);
    this.retry.resetAll(id);
    return { id, status: 'PAUSED' };
  }

  resumeDownload(id) {
    const dl = this.db.getDownload(id);
    if (!dl) throw new Error('Not found');
    if (!['PAUSED', 'FAILED'].includes(dl.status)) throw new Error(`Cannot resume "${dl.status}"`);
    this.db.getChunks(id).forEach(c => {
      if (c.status !== 'COMPLETED') this.db.updateChunk(id, c.start_byte, c.end_byte, { status: 'PENDING', downloaded_bytes: 0 });
    });
    this.db.updateDownload(id, { status: 'DOWNLOADING' });
    this.scheduler.enqueue(id, dl.priority || 0);
    setTimeout(() => this._downloadChunks(id, dl.url), 100);
    return { id, status: 'DOWNLOADING' };
  }

  cancelDownload(id) {
    const dl = this.db.getDownload(id);
    if (!dl) throw new Error('Not found');
    this.db.updateDownload(id, { status: 'CANCELLED', completed_at: new Date().toISOString() });
    this.scheduler.dequeue(id);
    this.retry.resetAll(id);
    const part = path.join(process.cwd(), '.downloads', `dl-${id}.part`);
    if (fs.existsSync(part)) try { fs.unlinkSync(part); } catch(e) {}
    return { id, status: 'CANCELLED' };
  }

  removeDownload(id) {
    this.cancelDownload(id);
    this.db.db.prepare('DELETE FROM chunks WHERE download_id = ?').run(id);
    this.db.db.prepare('DELETE FROM events WHERE download_id = ?').run(id);
    this.db.db.prepare('DELETE FROM downloads WHERE id = ?').run(id);
    return { id, removed: true };
  }

  setBandwidthLimit(bytesPerSec) {
    this.bandwidth.setGlobalLimit(bytesPerSec);
  }

  setMaxConcurrent(max) {
    this.scheduler.setMaxConcurrent(max);
  }

  // === STATUS ===

  getDownload(id) {
    const dl = this.db.getDownload(id);
    if (!dl) return null;
    const chunks = this.db.getChunks(id);
    const done = chunks.filter(c => c.status === 'COMPLETED').length;
    const totalBytes = chunks.reduce((s, c) => s + (c.end_byte - c.start_byte + 1), 0);
    const downloaded = chunks.reduce((s, c) => s + (c.downloaded_bytes || 0), 0);
    const speed = this.speedTracker.get(id) || 0;
    const eta = speed > 0 && dl.total_size > 0 ? Math.ceil((dl.total_size - downloaded) / speed) : 0;
    return {
      ...dl,
      percentage: totalBytes > 0 ? Math.round((downloaded / totalBytes) * 100) : 0,
      downloaded_bytes: downloaded,
      speed,
      eta,
      chunks: { total: chunks.length, completed: done, pending: chunks.filter(c => c.status === 'PENDING').length, active: chunks.filter(c => c.status === 'ACTIVE').length }
    };
  }

  getAllDownloads() {
    return this.db.getAllDownloads().map(d => {
      const chunks = this.db.getChunks(d.id);
      const done = chunks.filter(c => c.status === 'COMPLETED').length;
      const downloaded = chunks.reduce((s, c) => s + (c.downloaded_bytes || 0), 0);
      const speed = this.speedTracker.get(d.id) || 0;
      return { ...d, percentage: chunks.length > 0 ? Math.round((done / chunks.length) * 100) : 0, downloaded_bytes: downloaded, speed };
    });
  }

  getEvents(id) { return this.db.getEvents(id); }

  getSystemStatus() {
    return {
      bandwidth: this.bandwidth.getStatus(),
      scheduler: this.scheduler.getStatus(),
      retry: { maxRetries: this.retry.maxRetries }
    };
  }

  queryHistory(filters = {}) {
    let all = this.db.getAllDownloads();
    if (filters.status) all = all.filter(d => d.status === filters.status);
    if (filters.filename) all = all.filter(d => (d.filename || '').toLowerCase().includes(filters.filename.toLowerCase()));
    return all;
  }

  // === INTERNAL ===

  _extractFilename(url) {
    try {
      const parts = new URL(url).pathname.split('/');
      return decodeURIComponent(parts[parts.length - 1]) || 'download';
    } catch { return 'download'; }
  }

  async _analyzeAndStart(downloadId, url) {
    let cap = { contentLength: null, supportsRange: false };
    try { cap = await this.analyzer.analyze(url); } catch (e) {}

    if (!cap.contentLength || cap.contentLength <= 0) {
      try { cap = await this._discoverSize(url); } catch (e) {}
    }

    const dl = this.db.getDownload(downloadId);
    if (!dl || dl.status === 'CANCELLED') return;

    if (cap.contentLength && cap.contentLength > 0) {
      this.db.updateDownload(downloadId, { total_size: cap.contentLength });
      const n = cap.supportsRange ? Math.min(8, Math.max(2, Math.ceil(cap.contentLength / this.chunkSize))) : 1;
      const sz = Math.ceil(cap.contentLength / n);
      for (let i = 0; i < n; i++) this.db.createChunk(downloadId, i * sz, Math.min(i * sz + sz - 1, cap.contentLength - 1));
    } else {
      this.db.createChunk(downloadId, 0, -1);
    }

    this.db.updateDownload(downloadId, { status: 'DOWNLOADING', started_at: new Date().toISOString() });
    this.db.addEvent(downloadId, 'DOWNLOAD_STARTED', { total_size: cap.contentLength });
    this.scheduler.markActive(downloadId);
    this._downloadChunks(downloadId, url);
  }

  _discoverSize(url) {
    return new Promise((resolve, reject) => {
      const client = url.startsWith('https:') ? https : http;
      const parsed = new URL(url);
      const req = client.request({
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search || ''),
        method: 'GET',
        timeout: 15000,
        headers: { 'Range': 'bytes=0-0', 'User-Agent': 'Mozilla/5.0' }
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return this._discoverSize(res.headers.location).then(resolve).catch(reject);
        }
        let total = null, range = false;
        if (res.headers['content-range']) { const m = res.headers['content-range'].match(/\/(\d+)/); if (m) total = parseInt(m[1]); range = true; }
        else if (res.headers['content-length']) { total = parseInt(res.headers['content-length']); }
        res.on('data', () => {});
        res.on('end', () => resolve({ contentLength: total, supportsRange: range }));
      });
      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
      req.end();
    });
  }

  _downloadChunks(downloadId, url) {
    const dl = this.db.getDownload(downloadId);
    if (!dl || dl.status !== 'DOWNLOADING') return;
    this.db.getChunks(downloadId).filter(c => c.status === 'PENDING').forEach(chunk => {
      this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'ACTIVE' });
      this._fetchChunk(downloadId, url, chunk);
    });
  }

  _fetchChunk(downloadId, url, chunk) {
    try {
      const dl = this.db.getDownload(downloadId);
      if (!dl || dl.status !== 'DOWNLOADING') return;

      // Bandwidth check
      const bw = this.bandwidth.canTransfer(downloadId, 0);
      if (!bw.allowed) {
        setTimeout(() => this._fetchChunk(downloadId, url, chunk), bw.delay);
        return;
      }

      let client;
      try { client = url.startsWith('https:') ? https : http; } catch(e) { return; }
      let parsed;
      try { parsed = new URL(url); } catch(e) { return; }

      const headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' };
      if (chunk.end_byte > chunk.start_byte && chunk.end_byte > 0) {
        const start = chunk.start_byte + (chunk.downloaded_bytes || 0);
        if (start < chunk.end_byte) headers['Range'] = `bytes=${start}-${chunk.end_byte}`;
      }

      const req = client.request({
        hostname: parsed.hostname, port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search || ''), method: 'GET', timeout: 30000, headers
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return this._fetchChunk(downloadId, res.headers.location, chunk);
        }

        if (res.statusCode !== 200 && res.statusCode !== 206) {
          return this._handleChunkError(downloadId, url, chunk, res.statusCode);
        }

        let downloaded = chunk.downloaded_bytes || 0;
        let lastSpeedUpdate = Date.now();
        let bytesSinceLastUpdate = 0;
        const dir = path.join(process.cwd(), '.downloads');
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        const filePath = path.join(dir, `dl-${downloadId}.part`);
        const ws = fs.createWriteStream(filePath, { flags: downloaded > 0 ? 'r+' : 'w', start: downloaded > 0 ? downloaded : undefined });

        res.on('data', (data) => {
          const d2 = this.db.getDownload(downloadId);
          if (!d2 || d2.status !== 'DOWNLOADING') { res.destroy(); ws.end(); return; }

          downloaded += data.length;
          bytesSinceLastUpdate += data.length;
          ws.write(data);

          // Track speed
          const now = Date.now();
          if (now - lastSpeedUpdate >= 1000) {
            const speed = bytesSinceLastUpdate / ((now - lastSpeedUpdate) / 1000);
            this.speedTracker.set(downloadId, speed);
            this.bandwidth.recordBytes(downloadId, bytesSinceLastUpdate);
            lastSpeedUpdate = now;
            bytesSinceLastUpdate = 0;
          }

          this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { downloaded_bytes: downloaded, status: 'ACTIVE' });
          this.db.updateDownload(downloadId, { downloaded_bytes: (d2.downloaded_bytes || 0) + data.length });
        });

        res.on('end', () => {
          ws.end();
          this.speedTracker.set(downloadId, 0);
          this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { downloaded_bytes: downloaded, status: 'COMPLETED' });
          this.db.addEvent(downloadId, 'CHUNK_COMPLETED', { start: chunk.start_byte, end: chunk.end_byte });
          this.retry.reset(downloadId, chunk);
          this._checkDone(downloadId);
        });

        res.on('error', () => {
          ws.end();
          this.speedTracker.set(downloadId, 0);
          this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { downloaded_bytes: downloaded, status: 'FAILED' });
          this._handleChunkError(downloadId, url, chunk, 0);
        });
      });

      req.on('error', () => {
        this.speedTracker.set(downloadId, 0);
        this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'FAILED' });
        this._handleChunkError(downloadId, url, chunk, 0);
      });
      req.on('timeout', () => req.destroy());
      req.end();
    } catch(e) {
      console.error(`[Fetch] Error: ${e.message}`);
      try { this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'FAILED' }); this._checkDone(downloadId); } catch(e2) {}
    }
  }

  _handleChunkError(downloadId, url, chunk, statusCode) {
    const attempt = this.retry.recordAttempt(downloadId, chunk);

    if (this.retry.shouldRetryError(statusCode) || this.retry.canRetry(downloadId, chunk)) {
      const delay = this.retry.getRetryDelay(downloadId, chunk);
      this.db.addEvent(downloadId, 'RETRY', { chunk: chunk.start_byte, attempt, delay });
      console.log(`[Retry] #${downloadId} chunk ${chunk.start_byte}, attempt ${attempt}, delay ${delay}ms`);
      setTimeout(() => {
        this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'PENDING' });
        this._fetchChunk(downloadId, url, chunk);
      }, delay);
    } else {
      this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'FAILED' });
      this.db.addEvent(downloadId, 'CHUNK_FAILED', { start: chunk.start_byte, error: `HTTP ${statusCode}` });
      this._checkDone(downloadId);
    }
  }

  _checkDone(downloadId) {
    const dl = this.db.getDownload(downloadId);
    if (!dl || dl.status !== 'DOWNLOADING') return;
    const chunks = this.db.getChunks(downloadId);
    if (!chunks.length || !chunks.every(c => c.status === 'COMPLETED' || c.status === 'FAILED')) return;

    const failed = chunks.some(c => c.status === 'FAILED');
    const totalBytes = chunks.reduce((s, c) => s + (c.downloaded_bytes || 0), 0);

    // Rename part file
    const part = path.join(process.cwd(), '.downloads', `dl-${downloadId}.part`);
    const dest = path.join(process.cwd(), '.downloads', dl.filename || `dl-${downloadId}`);
    if (fs.existsSync(part)) try { fs.renameSync(part, dest); } catch(e) {}

    this.db.updateDownload(downloadId, {
      status: failed ? 'FAILED' : 'COMPLETED',
      completed_at: new Date().toISOString(),
      downloaded_bytes: totalBytes
    });

    this.scheduler.markInactive(downloadId);
    this.scheduler.dequeue(downloadId);
    this.speedTracker.delete(downloadId);
    this.db.addEvent(downloadId, failed ? 'DOWNLOAD_FAILED' : 'DOWNLOAD_COMPLETED', { bytes: totalBytes });
  }

  close() { this.db.close(); }
}

module.exports = DownloadManager;