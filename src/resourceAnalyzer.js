const http = require('http');
const https = require('https');

class ResourceAnalyzer {
  analyze(url) {
    return new Promise((resolve) => {
      let client;
      try { client = url.startsWith('https:') ? https : http; } catch(e) { resolve({ contentLength: null, supportsRange: false }); return; }
      let parsed;
      try { parsed = new URL(url); } catch(e) { resolve({ contentLength: null, supportsRange: false }); return; }

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

module.exports = ResourceAnalyzer;