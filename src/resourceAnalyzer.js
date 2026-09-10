const http = require('http');
const https = require('https');

class ResourceAnalyzer {
  constructor() {}

  /**
   * Analyze a URL resource capabilities
   * @param {string} url - The resource URL
   * @returns {Object} Resource capability object
   */
  analyze(url) {
    return new Promise((resolve, reject) => {
      const client = url.startsWith('https:') ? https : http;
      const parsed = new URL(url);

      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search ? '?' + parsed.search : ''),
        method: 'HEAD',
        timeout: 15000
      };

      const req = client.request(options, (response) => {
        const capability = {
          url,
          finalUrl: response.headers['x-forwarded-host'] || url,
          protocol: parsed.protocol,
          contentType: response.headers['content-type'] || 'application/octet-stream',
          contentLength: null,
          supportsRange: false,
          supportsResume: false,
          suggestedFilename: null,
          etag: response.headers['etag'],
          lastModified: response.headers['last-modified'],
          statusCode: response.statusCode,
          redirects: []
        };

        // Get content length
        if (response.headers['content-length']) {
          capability.contentLength = parseInt(response.headers['content-length'], 10);
        }

        // Check range support
        capability.supportsRange = response.headers['accept-ranges'] === 'bytes';

        // Get suggested filename from Content-Disposition
        if (response.headers['content-disposition']) {
          const cd = response.headers['content-disposition'];
          const filenameMatch = cd.match(/filename["']?\s*=\s*["']?([^"';]+)["']?/i);
          if (filenameMatch && filenameMatch[1]) {
            capability.suggestedFilename = filenameMatch[1].replace(/[^\w\.-]/g, '_');
          }
        }

        // Determine resumability
        capability.supportsResume = capability.supportsRange && capability.contentLength > 0;

        // Track redirects if they occurred
        if (response.statusCode >= 300 && response.statusCode < 400) {
          capability.redirects.push({
            status: response.statusCode,
            location: response.headers.location
          });
        }

        resolve(capability);
      });

      req.on('error', (err) => {
        reject(new Error(`Resource analysis failed: ${err.message}`));
      });

      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Resource analysis timeout'));
      });

      req.end();
    });
  }

  /**
   * Get file size using GET request (fallback when HEAD not available)
   * @param {string} url - The resource URL
   * @returns {Promise<Object>} Capability object with size
   */
  getFileSize(url) {
    return new Promise((resolve, reject) => {
      const client = url.startsWith('https:') ? https : http;
      const parsed = new URL(url);
      const options = {
        hostname: parsed.hostname,
        port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path: parsed.pathname + (parsed.search ? '?' + parsed.search : ''),
        method: 'GET',
        timeout: 15000,
        headers: { 'Range': 'bytes=0-0' }
      };

      const req = client.request(options, (response) => {
        const capability = {
          url,
          contentLength: null,
          supportsRange: false
        };

        if (response.headers['content-length']) {
          capability.contentLength = parseInt(response.headers['content-length'], 10);
        }

        // Check if range was satisfied
        capability.supportsRange = response.statusCode === 206; // Partial Content

        resolve(capability);
      });

      req.on('error', reject);
      req.on('timeout', () => { req.destroy(); reject(new Error('Size request timeout')); });
      req.end();
    });
  }
}

module.exports = ResourceAnalyzer;