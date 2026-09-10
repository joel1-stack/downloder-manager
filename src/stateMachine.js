const PersistenceLayer = require('./persistence');

/**
 * Download State Machine - The "brain" orchestration layer.
 * Manages lifecycle transitions and enforces valid state changes.
 */

class DownloadStateMachine {
  constructor(persistence) {
    this.persistence = persistence;
    
    // Define valid state transitions
    this.transitions = {
      IDLE: ['QUEUED', 'CANCELLED'],
      QUEUED: ['STARTING', 'CANCELLED'],
      STARTING: ['DOWNLOADING', 'PAUSED', 'CANCELLED'],
      DOWNLOADING: ['PAUSED', 'COMPLETED', 'FAILED', 'RETRY', 'CANCELLED'],
      PAUSED: ['STARTING', 'CANCELLED', 'FAILED'],
      COMPLETED: [], // Terminal state
      FAILED: ['RETRY', 'CANCELLED'],
      RETRY: ['DOWNLOADING', 'CANCELLED'],
      CANCELLED: [] // Terminal state
    };

    // Track state-specific data
    this.stateDescriptions = {
      IDLE: 'Download created, waiting to start',
      QUEUED: 'Queued for download, waiting for scheduler',
      STARTING: 'Initializing download - establishing connections',
      DOWNLOADING: 'Active download in progress',
      PAUSED: 'Paused by user or system',
      COMPLETED: 'Download finished successfully',
      FAILED: 'Download failed and not retrying',
      RETRY: 'Retrying after transient failure',
      CANCELLED: 'Download cancelled by user'
    };

    // Error categorization for retry decisions
    this.retryableErrors = [
      'ECONNRESET',
      'ETIMEDOUT',
      'EPIPE',
      'ENOTFOUND',
      'ECONNREFUSED'
    ];

    this.nonRetryableErrors = [
      '403',
      '404',
      '400',
      'AuthenticationError',
      'AccessDenied'
    ];
  }

  /**
   * Transition download to a new state
   */
  transition(downloadId, newState, eventData = {}) {
    const download = this.persistence.getDownload(downloadId);
    if (!download) {
      throw new Error(`Download ${downloadId} not found`);
    }

    const currentState = download.status;
    const allowedNext = this.transitions[currentState] || [];

    if (!allowedNext.includes(newState)) {
      this._logInvalidTransition(downloadId, currentState, newState);
      return false;
    }

    // Handle special transition logic
    switch (newState) {
      case 'QUEUED':
        this._onQueued(downloadId);
        break;
      case 'STARTING':
        this._onStarting(downloadId);
        break;
      case 'DOWNLOADING':
        this._onStartingDownload(downloadId);
        break;
      case 'PAUSED':
        this._onPaused(downloadId);
        break;
      case 'COMPLETED':
        this._onCompleted(downloadId);
        break;
      case 'FAILED':
        this._onFailed(downloadId, eventData);
        break;
      case 'CANCELLED':
        this._onCancelled(downloadId);
        break;
      case 'RETRY':
        this._onRetry(downloadId);
        break;
    }

    // Update persistence
    this.persistence.updateDownloadState(downloadId, newState, eventData);

    // Emit state change event
    this._emitStateEvent(downloadId, currentState, newState);

    return true;
  }

  /**
   * Handle IDLE -> QUEUED transition
   */
  _onQueued(downloadId) {
    this._emitStateEvent(downloadId, 'IDLE', 'QUEUED');
  }

  /**
   * Handle IDLE/QUEUED -> STARTING transition
   */
  _onStarting(downloadId) {
    // Mark download as started, create chunks if needed
    this.persistence.updateDownloadState(downloadId, 'STARTING', { started_at: new Date().toISOString() });
  }

  /**
   * Handle -> DOWNLOADING transition (actually starting the engine)
   */
  _onStartingDownload(downloadId) {
    // Create chunks if not already exist
    this._ensureChunks(downloadId);
  }

  /**
   * Handle -> PAUSED transition
   */
  _onPaused(downloadId) {
    // No special cleanup needed - state is preserved
    this._emitStateEvent(downloadId, 'DOWNLOADING', 'PAUSED');
  }

  /**
   * Handle -> COMPLETED transition
   */
  _onCompleted(downloadId) {
    this.persistence.updateDownloadState(downloadId, 'COMPLETED', {
      completed_at: new Date().toISOString()
    });
    this._emitStateEvent(downloadId, 'DOWNLOADING', 'COMPLETED');
  }

  /**
   * Handle -> FAILED transition
   */
  _onFailed(downloadId, eventData) {
    // Determine if retry is appropriate
    const error = eventData.error;
    const canRetry = error 
      ? this.retryableErrors.some(e => error.message && error.message.includes(e))
      : false;

    if (canRetry) {
      // Transition to RETRY state, will go back to DOWNLOADING
      this.persistence.updateDownloadState(downloadId, 'RETRY', { error: error });
    } else {
      this.persistence.updateDownloadState(downloadId, 'FAILED', { error: eventData.error });
    }
    this._emitStateEvent(downloadId, 'DOWNLOADING', 'FAILED');
  }

  /**
   * Handle -> RETRY -> DOWNLOADING transition
   */
  _onRetry(downloadId) {
    this.persistence.updateDownloadState(downloadId, 'DOWNLOADING', { error: null });
    this._emitStateEvent(downloadId, 'RETRY', 'DOWNLOADING');
  }

  /**
   * Handle -> CANCELLED transition
   */
  _onCancelled(downloadId) {
    this.persistence.updateDownloadState(downloadId, 'CANCELLED', {
      completed_at: new Date().toISOString()
    });
    this._emitStateEvent(downloadId, 'any', 'CANCELLED');
  }

  /**
   * Ensure chunks exist for a download
   */
  _ensureChunks(downloadId) {
    const download = this.persistence.getDownload(downloadId);
    if (!download) return;

    // If total_size is known and chunks don't exist, create them
    if (download.total_size && download.total_size > 0) {
      const existingChunks = this.persistence.getChunks(downloadId);
      
      if (existingChunks.length === 0) {
        const chunkSize = 25 * 1024 * 1024; // 25MB default
        const numChunks = Math.max(1, Math.ceil(download.total_size / chunkSize));
        const actualChunkSize = Math.ceil(download.total_size / numChunks);

        for (let i = 0; i < numChunks; i++) {
          const startByte = i * actualChunkSize;
          const endByte = Math.min(startByte + actualChunkSize - 1, download.total_size - 1);
          
          this.persistence.createChunk(downloadId, startByte, endByte);
        }
      }
    }
  }

  /**
   * Emit state change event
   */
  _emitStateEvent(downloadId, fromState, toState) {
    this.persistence._emitEvent(downloadId, 'STATE_CHANGE', {
      from: fromState,
      to: toState
    });
  }

  /**
   * Log invalid transition
   */
  _logInvalidTransition(downloadId, from, to) {
    this.persistence._emitEvent(downloadId, 'INVALID_TRANSITION', {
      from,
      to
    });
  }

  /**
   * Check if a state transition is valid
   */
  isValidTransition(currentState, newState) {
    return (this.transitions[currentState] || []).includes(newState);
  }

  /**
   * Get current state description
   */
  getStateDescription(state) {
    return this.stateDescriptions[state] || 'Unknown state';
  }

  /**
   * Get allowed next states
   */
  getAllowedNextStates(currentState) {
    return this.transitions[currentState] || [];
  }
}

module.exports = DownloadStateMachine;