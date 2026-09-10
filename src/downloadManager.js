const path = require('path');
const DatabaseManager = require(path.join(__dirname, 'database'));
const http = require('http');
const https = require('https');
const fs = require('fs');

class ResourceAnalyzer {
  analyze(url) {
    return new Promise((resolve) => {
      const client = url.startsWith('https:') ? https : http;
      const parsed = new URL(url);
      const req = client.request({
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search || ''),
        method: 'HEAD',
        timeout: 10000,
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return this.analyze(res.headers.location).then(resolve);
        }
        const cap = {
          contentLength: res.headers['content-length'] ? parseInt(res.headers['content-length'], 10) : null,
          supportsRange: res.headers['accept-ranges'] === 'bytes'
        };
        resolve(cap);
      });
      req.on('error', () => resolve({ contentLength: null, supportsRange: false }));
      req.on('timeout', () => { req.destroy(); resolve({ contentLength: null, supportsRange: false }); });
      req.end();
    });
  }
}

class DownloadManager {
  constructor(opts = {}) {
    this.db = new DatabaseManager(opts.dbPath || './download_manager.db');
    this.analyzer = new ResourceAnalyzer();
    this.chunkSize = opts.chunkSize || 1024 * 1024; // 1MB chunks
    this._recover();
  }

  _recover() {
    const unfinished = this.db.getAllDownloads().filter(d => !['COMPLETED', 'CANCELLED'].includes(d.status));
    unfinished.forEach(d => this.db.updateDownload(d.id, { status: 'PAUSED' }));
    if (unfinished.length) console.log(`[Recovery] ${unfinished.length} downloads recovered`);
  }

  startDownload(url, opts = {}) {
    try { new URL(url); } catch(e) { throw new Error('Invalid URL'); }
    const filename = opts.filename || this._extractFilename(url);
    const downloadId = this.db.createDownload(url, { filename, total_size: opts.total_size || 0, priority: opts.priority || 0 });
    this.db.addEvent(downloadId, 'DOWNLOAD_CREATED', { url });
    this._analyzeAndStart(downloadId, url);
    return { id: downloadId, status: 'IDLE', message: 'Analyzing resource...' };
  }

  _extractFilename(url) {
    try {
      const parts = new URL(url).pathname.split('/');
      return decodeURIComponent(parts[parts.length - 1]) || 'download';
    } catch { return 'download'; }
  }

  async _analyzeAndStart(downloadId, url) {
    let cap = { contentLength: null, supportsRange: false };
    try {
      cap = await this.analyzer.analyze(url);
    } catch (e) {
      console.log(`[Analyzer] HEAD failed: ${e.message}`);
    }

    const dl = this.db.getDownload(downloadId);
    if (!dl || dl.status === 'CANCELLED') return;

    const update = { status: 'QUEUED' };
    if (cap.contentLength && cap.contentLength > 0) update.total_size = cap.contentLength;
    this.db.updateDownload(downloadId, update);

    // If HEAD failed to get size, try GET Range: bytes=0-0 to discover it
    if (!cap.contentLength || cap.contentLength <= 0) {
      try {
        const sizeCap = await this._discoverSize(url);
        if (sizeCap.contentLength && sizeCap.contentLength > 0) {
          cap.contentLength = sizeCap.contentLength;
          cap.supportsRange = sizeCap.supportsRange;
          this.db.updateDownload(downloadId, { total_size: sizeCap.contentLength });
        }
      } catch (e) {
        console.log(`[Analyzer] Size discovery failed: ${e.message}`);
      }
    }

    // Create chunks
    if (cap.contentLength && cap.contentLength > 0) {
      const n = cap.supportsRange ? Math.min(8, Math.max(2, Math.ceil(cap.contentLength / this.chunkSize))) : 1;
      const sz = Math.ceil(cap.contentLength / n);
      for (let i = 0; i < n; i++) {
        this.db.createChunk(downloadId, i * sz, Math.min(i * sz + sz - 1, cap.contentLength - 1));
      }
      console.log(`[Chunks] ${n} chunks for #${downloadId} (${cap.contentLength} bytes, range=${cap.supportsRange})`);
    } else {
      // Unknown size: single chunk, will stream the entire response
      this.db.createChunk(downloadId, 0, -1);
      console.log(`[Chunks] Unknown size for #${downloadId}, streaming`);
    }

    this.db.updateDownload(downloadId, { status: 'DOWNLOADING', started_at: new Date().toISOString() });
    this.db.addEvent(downloadId, 'DOWNLOAD_STARTED', { total_size: cap.contentLength });
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
        headers: { 'Range': 'bytes=0-0', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
      }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return this._discoverSize(res.headers.location).then(resolve).catch(reject);
        }
        let total = null;
        let range = false;
        if (res.headers['content-range']) {
          const m = res.headers['content-range'].match(/\/(\d+)/);
          if (m) total = parseInt(m[1], 10);
          range = true;
        } else if (res.headers['content-length']) {
          total = parseInt(res.headers['content-length'], 10);
          range = false;
        }
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

    let client;
    try { client = url.startsWith('https:') ? https : http; } catch(e) { console.log('[Fetch] Bad URL'); return; }
    let parsed;
    try { parsed = new URL(url); } catch(e) { console.log('[Fetch] Invalid URL:', url); return; }
    const headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' };

    // Only use Range header if we know the size and it's a valid range
    if (chunk.end_byte > chunk.start_byte && chunk.end_byte > 0) {
      const start = chunk.start_byte + (chunk.downloaded_bytes || 0);
      if (start < chunk.end_byte) {
        headers['Range'] = `bytes=${start}-${chunk.end_byte}`;
      }
    }

    const req = client.request({
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + (parsed.search || ''),
      method: 'GET',
      timeout: 30000,
      headers
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        this._fetchChunk(downloadId, res.headers.location, chunk);
        return;
      }

      if (res.statusCode !== 200 && res.statusCode !== 206) {
        this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'FAILED' });
        this._checkDone(downloadId);
        return;
      }

      let downloaded = chunk.downloaded_bytes || 0;
      const dir = path.join(process.cwd(), '.downloads');
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      const filePath = path.join(dir, `dl-${downloadId}.part`);
      const ws = fs.createWriteStream(filePath, { flags: downloaded > 0 ? 'r+' : 'w', start: downloaded > 0 ? downloaded : undefined });

      res.on('data', (data) => {
        const d2 = this.db.getDownload(downloadId);
        if (!d2 || d2.status !== 'DOWNLOADING') { res.destroy(); ws.end(); return; }
        downloaded += data.length;
        ws.write(data);
        this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { downloaded_bytes: downloaded, status: 'ACTIVE' });
        this.db.updateDownload(downloadId, { downloaded_bytes: (d2.downloaded_bytes || 0) + data.length });
      });

      res.on('end', () => {
        ws.end();
        this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { downloaded_bytes: downloaded, status: 'COMPLETED' });
        this._checkDone(downloadId);
      });

      res.on('error', () => { ws.end(); this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'FAILED' }); this._checkDone(downloadId); });
    });

    req.on('error', () => { try { this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'FAILED' }); this._checkDone(downloadId); } catch(e) {} });
    req.on('timeout', () => { req.destroy(); });
    req.end();
    } catch(e) { console.error('[Fetch] Fatal error:', e.message); try { this.db.updateChunk(downloadId, chunk.start_byte, chunk.end_byte, { status: 'FAILED' }); this._checkDone(downloadId); } catch(e2) {} }
  }

  _checkDone(downloadId) {
    const dl = this.db.getDownload(downloadId);
    if (!dl || dl.status !== 'DOWNLOADING') return;
    const chunks = this.db.getChunks(downloadId);
    if (!chunks.length || !chunks.every(c => c.status === 'COMPLETED' || c.status === 'FAILED')) return;

    const failed = chunks.some(c => c.status === 'FAILED');
    const totalBytes = chunks.reduce((s, c) => s + (c.downloaded_bytes || 0), 0);

    const part = path.join(process.cwd(), '.downloads', `dl-${downloadId}.part`);
    const dest = path.join(process.cwd(), '.downloads', dl.filename || `dl-${downloadId}`);
    if (fs.existsSync(part)) try { fs.renameSync(part, dest); } catch(e) {}

    this.db.updateDownload(downloadId, { status: failed ? 'FAILED' : 'COMPLETED', completed_at: new Date().toISOString(), downloaded_bytes: totalBytes });
  }

  pauseDownload(id) {
    const dl = this.db.getDownload(id);
    if (!dl) throw new Error('Not found');
    this.db.updateDownload(id, { status: 'PAUSED' });
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
    setTimeout(() => this._downloadChunks(id, dl.url), 100);
    return { id, status: 'DOWNLOADING' };
  }

  cancelDownload(id) {
    const dl = this.db.getDownload(id);
    if (!dl) throw new Error('Not found');
    this.db.updateDownload(id, { status: 'CANCELLED', completed_at: new Date().toISOString() });
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

  getDownload(id) {
    const dl = this.db.getDownload(id);
    if (!dl) return null;
    const chunks = this.db.getChunks(id);
    const done = chunks.filter(c => c.status === 'COMPLETED').length;
    return { ...dl, percentage: chunks.length > 0 ? Math.round((done / chunks.length) * 100) : 0 };
  }

  getAllDownloads() {
    return this.db.getAllDownloads().map(d => {
      const chunks = this.db.getChunks(d.id);
      const done = chunks.filter(c => c.status === 'COMPLETED').length;
      return { ...d, percentage: chunks.length > 0 ? Math.round((done / chunks.length) * 100) : 0 };
    });
  }

  getEvents(id) { return this.db.getEvents(id); }
  close() { this.db.close(); }
}

module.exports = DownloadManager;