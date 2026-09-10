const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

class VerificationEngine {
  constructor() {}

  /**
   * Verify file integrity using SHA-256
   * @param {string} filePath - Path to the file
   * @param {string} expectedHash - Expected SHA-256 hash
   * @returns {Promise<{valid: boolean, actual: string, expected: string}>}
   */
  verifyFile(filePath, expectedHash) {
    return new Promise((resolve, reject) => {
      if (!fs.existsSync(filePath)) {
        return reject(new Error('File not found'));
      }

      const hash = crypto.createHash('sha256');
      const stream = fs.createReadStream(filePath);

      stream.on('data', (data) => hash.update(data));
      stream.on('end', () => {
        const actual = hash.digest('hex');
        resolve({
          valid: actual.toLowerCase() === expectedHash.toLowerCase(),
          actual,
          expected: expectedHash
        });
      });
      stream.on('error', reject);
    });
  }

  /**
   * Calculate SHA-256 hash of a file
   */
  calculateHash(filePath) {
    return new Promise((resolve, reject) => {
      if (!fs.existsSync(filePath)) {
        return reject(new Error('File not found'));
      }

      const hash = crypto.createHash('sha256');
      const stream = fs.createReadStream(filePath);

      stream.on('data', (data) => hash.update(data));
      stream.on('end', () => resolve(hash.digest('hex')));
      stream.on('error', reject);
    });
  }

  /**
   * Verify file size matches expected
   */
  verifySize(filePath, expectedSize) {
    if (!fs.existsSync(filePath)) return false;
    const stats = fs.statSync(filePath);
    return stats.size === expectedSize;
  }

  /**
   * Get file info
   */
  getFileInfo(filePath) {
    if (!fs.existsSync(filePath)) return null;
    const stats = fs.statSync(filePath);
    return {
      size: stats.size,
      created: stats.birthtime,
      modified: stats.mtime,
      isFile: stats.isFile()
    };
  }

  /**
   * Calculate hash of a buffer
   */
  hashBuffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
  }

  /**
   * Verify downloaded chunks match expected ranges
   */
  verifyChunksIntegrity(downloadId, chunks, partFilePath) {
    if (!fs.existsSync(partFilePath)) {
      return { valid: false, error: 'Part file not found' };
    }

    const fileSize = fs.statSync(partFilePath).size;
    const expectedSize = chunks.reduce((sum, c) => sum + (c.end_byte - c.start_byte + 1), 0);

    return {
      valid: fileSize === expectedSize,
      actualSize: fileSize,
      expectedSize,
      chunksValid: chunks.every(c => c.status === 'COMPLETED')
    };
  }
}

module.exports = VerificationEngine;