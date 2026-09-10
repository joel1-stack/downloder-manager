const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

/**
 * Core downloader module - the "muscles" of the system.
 * Handles HTTP(S) download with progress tracking and file writing.
 */

class Downloader {
  constructor(options = {}) {
    this.url = options.url;
    this.outputPath = options.outputPath;
    this.headers = options.headers || {};
    this.onProgress = options.onProgress || ((progress) => {});
    this.onComplete = options.onComplete || ((result) => {});
    this.onError = options.onError || ((err) => {});
    this.timeout = options.timeout || 30000;
    this.maxRetries = options.maxRetries || 3;
    this.retryCount = 0;
  }

  /**
   * Start the download
   */
  start() {
    if (!this.url) {
      this.onError(new Error('No URL provided'));
      return;
    }

    // Parse URL to determine protocol
    const url = new URL(this.url);
    const protocol = url.protocol; // 'http:' or 'https:'

    const client = protocol === 'https:' ? https : http;

    // Create write stream
    const filePath = this.outputPath || this._deriveFilename(url);
    const writeStream = fs.createWriteStream(filePath, { flags: 'w' });

    // Track downloaded bytes
    let downloaded = 0;
    let isPaused = false;
    let resumeByte = 0;

    // Handle range request for resume
    if (resumeByte > 0) {
      this.headers['Range'] = `bytes=${resumeByte}-`;
    }

    const request = client.get(this.url, { headers: this.headers }, (response) => {
      // Handle redirect
      if (response.statusCode === 301 || response.statusCode === 302 || response.statusCode === 303 || response.statusCode === 307 || response.statusCode === 308) {
        const location = response.headers.location;
        if (location) {
          this.onError(new Error(`Redirect not supported: ${location}`));
          request.destroy();
          return;
        }
      }

      // Get content length if available
      const contentLength = response.headers['content-length'];
      let totalSize = resumeByte;

      if (contentLength && !resumeByte) {
        totalSize = parseInt(contentLength, 10);
      }

      // Write data chunks
      response.on('data', (chunk) => {
        if (isPaused) return; // Skip data if paused

        downloaded += chunk.length;
        totalSize = contentLength ? parseInt(contentLength, 10) : (downloaded + resumeByte);
        
        // Emit progress
        this.onProgress({
          downloaded,
          total: totalSize,
          percentage: totalSize ? Math.round((downloaded / totalSize) * 100) : 0,
          speed: this._calculateSpeed(downloaded),
          eta: this._calculateETA(downloaded, totalSize)
        });

        // Write to file
        writeStream.write(chunk);
      });

      response.on('end', () => {
        writeStream.end();
        this.onComplete({
          success: true,
          path: filePath,
          size: downloaded + resumeByte
        });
      });
    });

    request.on('error', (err) => {
      if (err.code === 'EPIPE' || err.code === 'ERR_STREAM_PREMATURE_CLOSE') {
        // Connection reset, handle retry
        this.onError(err);
        return;
      }
      this.onError(err);
    });

    request.on('timeout', () => {
      request.destroy();
      this.onError(new Error('Request timeout'));
    });

    // Set timeout
    request.setTimeout(this.timeout);
  }

  /**
   * Pause the download
   */
  pause() {
    this.isPaused = true;
  }

  /**
   * Resume the download
   */
  resume() {
    this.isPaused = false;
  }

  /**
   * Derive filename from URL
   */
  _deriveFilename(url) {
    const match = url.pathname.match(/\/([^\/]+)$/);
    if (match && match[1]) {
      return match[1].replace(/[^\w\.-]/g, '_');
    }
    return 'download';
  }

  /**
   * Calculate download speed
   */
  _calculateSpeed(downloaded) {
    const now = Date.now();
    // Simple speed calculation - would be improved with timing tracking
    return downloaded / 1000; // bytes/sec (approximate)
  }

  /**
   * Calculate ETA
   */
  _calculateETA(downloaded, totalSize) {
    if (!totalSize || downloaded >= totalSize) return '0s';
    const speed = this._calculateSpeed(downloaded);
    if (speed <= 0) return '?';
    const remaining = totalSize - downloaded;
    const seconds = Math.max(0, Math.ceil(remaining / speed));
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}m ${secs}s`;
  }

  /**
   * Cancel the download
   */
  cancel() {
    this.isPaused = true;
    // Request will be garbage collected
  }
}

module.exports = Downloader;