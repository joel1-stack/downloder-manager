class RetryManager {
  constructor(opts = {}) {
    this.maxRetries = opts.maxRetries || 5;
    this.baseDelay = opts.baseDelay || 1000;
    this.maxDelay = opts.maxDelay || 30000;
    this.retryCounts = new Map(); // downloadId-chunkKey -> count
  }

  getRetryKey(downloadId, chunk) {
    return `${downloadId}-${chunk.start_byte}-${chunk.end_byte}`;
  }

  canRetry(downloadId, chunk) {
    const key = this.getRetryKey(downloadId, chunk);
    const count = this.retryCounts.get(key) || 0;
    return count < this.maxRetries;
  }

  getRetryDelay(downloadId, chunk) {
    const key = this.getRetryKey(downloadId, chunk);
    const count = this.retryCounts.get(key) || 0;
    const delay = Math.min(this.baseDelay * Math.pow(2, count), this.maxDelay);
    const jitter = delay * 0.1 * Math.random();
    return Math.round(delay + jitter);
  }

  recordAttempt(downloadId, chunk) {
    const key = this.getRetryKey(downloadId, chunk);
    const count = this.retryCounts.get(key) || 0;
    this.retryCounts.set(key, count + 1);
    return count + 1;
  }

  reset(downloadId, chunk) {
    const key = this.getRetryKey(downloadId, chunk);
    this.retryCounts.delete(key);
  }

  resetAll(downloadId) {
    for (const key of this.retryCounts.keys()) {
      if (key.startsWith(`${downloadId}-`)) {
        this.retryCounts.delete(key);
      }
    }
  }

  shouldRetryError(statusCode) {
    const retryable = [408, 429, 500, 502, 503, 504];
    return retryable.includes(statusCode);
  }

  isNetworkError(err) {
    const codes = ['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'ECONNREFUSED', 'EAI_AGAIN'];
    return codes.includes(err.code);
  }

  getStats(downloadId) {
    let totalRetries = 0;
    for (const key of this.retryCounts.keys()) {
      if (key.startsWith(`${downloadId}-`)) {
        totalRetries += this.retryCounts.get(key);
      }
    }
    return { totalRetries };
  }
}

module.exports = RetryManager;