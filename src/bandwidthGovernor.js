class BandwidthGovernor {
  constructor(opts = {}) {
    this.globalLimit = opts.globalLimit || 0; // 0 = unlimited, bytes/sec
    this.perDownloadLimit = opts.perDownloadLimit || 0;
    this.windowSize = opts.windowSize || 1000; // ms
    this.downloads = new Map(); // downloadId -> { bytes, timestamps }
    this.globalBytes = 0;
    this.globalTimestamps = [];
  }

  setGlobalLimit(bytesPerSec) {
    this.globalLimit = bytesPerSec;
  }

  setPerDownloadLimit(downloadId, bytesPerSec) {
    if (!this.perDownloadLimits) this.perDownloadLimits = new Map();
    this.perDownloadLimits.set(downloadId, bytesPerSec);
  }

  canTransfer(downloadId, bytes) {
    if (this.globalLimit <= 0 && (!this.perDownloadLimits || !this.perDownloadLimits.has(downloadId))) {
      return { allowed: true, delay: 0 };
    }

    const now = Date.now();

    // Check per-download limit
    if (this.perDownloadLimits && this.perDownloadLimits.has(downloadId)) {
      const limit = this.perDownloadLimits.get(downloadId);
      if (limit > 0) {
        const dl = this._getDownloadStats(downloadId, now);
        if (dl.bytes >= limit * (this.windowSize / 1000)) {
          const delay = this._getWaitTime(dl, limit);
          if (delay > 0) return { allowed: false, delay };
        }
      }
    }

    // Check global limit
    if (this.globalLimit > 0) {
      const global = this._getGlobalStats(now);
      if (global.bytes >= this.globalLimit * (this.windowSize / 1000)) {
        const delay = this._getWaitTime(global, this.globalLimit);
        if (delay > 0) return { allowed: false, delay };
      }
    }

    return { allowed: true, delay: 0 };
  }

  recordBytes(downloadId, bytes) {
    const now = Date.now();

    // Per-download tracking
    if (!this.downloads.has(downloadId)) {
      this.downloads.set(downloadId, { bytes: 0, timestamps: [] });
    }
    const dl = this.downloads.get(downloadId);
    dl.bytes += bytes;
    dl.timestamps.push({ time: now, bytes });
    this._cleanup(dl, now);

    // Global tracking
    this.globalBytes += bytes;
    this.globalTimestamps.push({ time: now, bytes });
    this._cleanupGlobal(now);
  }

  _getDownloadStats(downloadId, now) {
    const dl = this.downloads.get(downloadId) || { bytes: 0, timestamps: [] };
    const windowStart = now - this.windowSize;
    const recent = dl.timestamps.filter(t => t.time >= windowStart);
    const bytes = recent.reduce((sum, t) => sum + t.bytes, 0);
    return { bytes, timestamps: recent };
  }

  _getGlobalStats(now) {
    const windowStart = now - this.windowSize;
    const recent = this.globalTimestamps.filter(t => t.time >= windowStart);
    const bytes = recent.reduce((sum, t) => sum + t.bytes, 0);
    return { bytes, timestamps: recent };
  }

  _getWaitTime(stats, limit) {
    if (!stats.timestamps.length) return 0;
    const maxBytes = limit * (this.windowSize / 1000);
    const excess = stats.bytes - maxBytes;
    if (excess <= 0) return 0;
    return Math.ceil((excess / limit) * 1000);
  }

  _cleanup(dl, now) {
    const windowStart = now - this.windowSize * 2;
    dl.timestamps = dl.timestamps.filter(t => t.time >= windowStart);
  }

  _cleanupGlobal(now) {
    const windowStart = now - this.windowSize * 2;
    this.globalTimestamps = this.globalTimestamps.filter(t => t.time >= windowStart);
  }

  getSpeed(downloadId) {
    const now = Date.now();
    const dl = this._getDownloadStats(downloadId, now);
    const windowSec = this.windowSize / 1000;
    return dl.bytes / windowSec;
  }

  getGlobalSpeed() {
    const now = Date.now();
    const g = this._getGlobalStats(now);
    const windowSec = this.windowSize / 1000;
    return g.bytes / windowSec;
  }

  getStatus() {
    return {
      globalLimit: this.globalLimit,
      globalSpeed: this.getGlobalSpeed(),
      activeDownloads: this.downloads.size,
      limits: this.perDownloadLimits ? Object.fromEntries(this.perDownloadLimits) : {}
    };
  }
}

module.exports = BandwidthGovernor;