const Downloader = require('./downloader');
const path = require('path');

/**
 * Segment Manager - Manages download segmentation and worker assignment.
 * Divides files into chunks for parallel downloading.
 */

class SegmentManager {
  constructor(options = {}) {
    this.defaultChunkSize = options.chunkSize || 25 * 1024 * 1024; // 25MB
    this.maxConcurrentWorkers = options.maxWorkers || 8;
    this.downloaderClass = Downloader;
  }

  /**
   * Plan chunks for a download based on file size and server capabilities
   */
  planChunks(downloadId, totalSize, existingChunks = []) {
    // If no total size or already have chunks, return existing
    if (!totalSize || totalSize <= 0) {
      return existingChunks || [];
    }

    // Determine optimal number of chunks
    // Adaptive: start with min(4, Math.max(2, Math.sqrt(totalSize / 1MB)))
    const sqrtFactor = Math.sqrt(totalSize / (1024 * 1024));
    const optimalChunks = Math.min(16, Math.max(2, Math.floor(sqrtFactor)));
    
    const chunkSize = Math.ceil(totalSize / optimalChunks);
    const chunks = [];

    // Filter out existing completed chunks
    const completedRanges = new Set(
      existingChunks
        .filter(c => c.status === 'COMPLETED')
        .map(c => `${c.start_byte}-${c.end_byte}`)
    );

    for (let i = 0; i < optimalChunks; i++) {
      const startByte = i * chunkSize;
      const endByte = Math.min(startByte + chunkSize - 1, totalSize - 1);
      const rangeKey = `${startByte}-${endByte}`;

      // Skip if this range is already completed
      if (completedRanges.has(rangeKey)) {
        // Find existing chunk and keep it
        const existing = existingChunks.find(
          c => c.start_byte === startByte && c.end_byte === endByte
        );
        if (existing) {
          chunks.push({
            id: existing.id,
            download_id: downloadId,
            start_byte: startByte,
            end_byte: endByte,
            status: 'COMPLETED'
          });
        }
        continue;
      }

      chunks.push({
        id: `chunk-${downloadId}-${i}`,
        download_id: downloadId,
        start_byte: startByte,
        end_byte: endByte,
        status: 'PENDING'
      });
    }

    return chunks;
  }

  /**
   * Start a download segment/chunk
   * @param {number} downloadId - Download database ID
   * @param {Object} chunk - Chunk definition {start_byte, end_byte}
   * @param {Object} options - Download options
   * @returns {Downloader} - Started downloader instance
   */
  startSegment(downloadId, chunk, options) {
    const { url, filename, destination, headers } = options;
    const chunkUrl = this._buildRangeUrl(url, chunk.start_byte, chunk.end_byte);

    const downloader = new this.downloaderClass({
      url: chunkUrl,
      outputPath: this._getChunkPath(downloadId, chunk),
      headers: {
        ...headers,
        Range: `bytes=${chunk.start_byte}-${chunk.end_byte}`
      },
      onProgress: (progress) => {
        // Update chunk progress in persistence
        this._onChunkProgress(downloadId, chunk, progress.downloaded, progress.total);
        if (options.onProgress) options.onProgress(progress);
      },
      onComplete: () => {
        this._onChunkCompleted(downloadId, chunk);
        if (options.onComplete) options.onComplete();
      },
      onError: (err) => {
        this._onChunkError(downloadId, chunk, err);
        if (options.onError) options.onError(err);
      }
    });

    downloader.start();
    return downloader;
  }

  /**
   * Build URL with Range header value
   */
  _buildRangeUrl(url, startByte, endByte) {
    const parsed = new URL(url);
    parsed.pathname += (parsed.pathname.includes('?') ? '&' : '?') + `start=${startByte}&end=${endByte}`;
    return parsed.toString();
  }

  /**
   * Get path for a chunk file
   */
  _getChunkPath(downloadId, chunk) {
    const baseDir = path.dirname(process.cwd());
    const chunkDir = path.join(baseDir, '.downloads', `download-${downloadId}`);
    
    // Ensure directory exists
    const fs = require('fs');
    if (!fs.existsSync(chunkDir)) {
      fs.mkdirSync(chunkDir, { recursive: true });
    }
    
    return path.join(chunkDir, `chunk-${chunk.start_byte}-${chunk.end_byte}`);
  }

  /**
   * Handle chunk progress update
   */
  _onChunkProgress(downloadId, chunk, downloaded, total) {
    // Update chunk in database
    this.persistence.updateChunkProgress(
      downloadId,
      chunk.start_byte,
      chunk.end_byte,
      downloaded
    );
  }

  /**
   * Handle chunk completion
   */
  _onChunkCompleted(downloadId, chunk) {
    // Mark chunk as completed in database
    this.persistence.updateChunkProgress(
      downloadId,
      chunk.start_byte,
      chunk.end_byte,
      chunk.end_byte - chunk.start_byte + 1,
      'COMPLETED'
    );
  }

  /**
   * Handle chunk error
   */
  _onChunkError(downloadId, chunk, error) {
    // Could trigger retry logic here
    console.error(`Chunk error: ${error.message}`);
  }

  /**
   * Initialize persistence reference
   */
  setPersistence(persistence) {
    this.persistence = persistence;
  }
}

module.exports = SegmentManager;