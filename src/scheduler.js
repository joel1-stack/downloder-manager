class Scheduler {
  constructor(opts = {}) {
    this.maxConcurrent = opts.maxConcurrent || 4;
    this.priorityQueue = [];
    this.activeDownloads = new Set();
    this.pausedByScheduler = new Set();
  }

  /**
   * Add a download to the scheduler queue
   */
  enqueue(downloadId, priority = 0) {
    this.priorityQueue.push({ downloadId, priority, addedAt: Date.now() });
    this.priorityQueue.sort((a, b) => b.priority - a.priority || a.addedAt - b.addedAt);
  }

  /**
   * Remove a download from the scheduler
   */
  dequeue(downloadId) {
    this.priorityQueue = this.priorityQueue.filter(q => q.downloadId !== downloadId);
    this.activeDownloads.delete(downloadId);
  }

  /**
   * Get next downloads to run (respecting concurrency limit)
   */
  getNextDownloads() {
    const available = this.maxConcurrent - this.activeDownloads.size;
    if (available <= 0) return [];

    return this.priorityQueue
      .filter(q => !this.activeDownloads.has(q.downloadId))
      .slice(0, available)
      .map(q => q.downloadId);
  }

  /**
   * Mark a download as active (started)
   */
  markActive(downloadId) {
    this.activeDownloads.add(downloadId);
    this.pausedByScheduler.delete(downloadId);
  }

  /**
   * Mark a download as inactive (paused/completed/cancelled)
   */
  markInactive(downloadId) {
    this.activeDownloads.delete(downloadId);
  }

  /**
   * Pause lowest priority download to make room
   */
  pauseLowestPriority() {
    if (this.activeDownloads.size <= this.maxConcurrent) return null;

    let lowestId = null;
    let lowestPriority = Infinity;
    let oldestTime = Infinity;

    for (const id of this.activeDownloads) {
      const entry = this.priorityQueue.find(q => q.downloadId === id);
      if (entry && (entry.priority < lowestPriority || 
          (entry.priority === lowestPriority && entry.addedAt < oldestTime))) {
        lowestPriority = entry.priority;
        oldestTime = entry.addedAt;
        lowestId = id;
      }
    }

    if (lowestId) {
      this.activeDownloads.delete(lowestId);
      this.pausedByScheduler.add(lowestId);
    }

    return lowestId;
  }

  /**
   * Check if scheduler paused a download
   */
  wasPausedByScheduler(downloadId) {
    return this.pausedByScheduler.has(downloadId);
  }

  /**
   * Get scheduler status
   */
  getStatus() {
    return {
      maxConcurrent: this.maxConcurrent,
      activeCount: this.activeDownloads.size,
      queuedCount: this.priorityQueue.length,
      active: Array.from(this.activeDownloads),
      queued: this.priorityQueue.map(q => q.downloadId)
    };
  }

  /**
   * Set max concurrent downloads
   */
  setMaxConcurrent(max) {
    this.maxConcurrent = max;
  }
}

module.exports = Scheduler;