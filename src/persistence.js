const Database = require('better-sqlite3');

class PersistenceLayer {
  constructor(dbPath = './downloads.db') {
    this.dbPath = dbPath;
    this.db = this._initDatabase();
  }

  _initDatabase() {
    const db = new Database(this.dbPath, { readonly: false });

    // Enable WAL mode for better concurrency
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');

    // Create downloads table
    db.exec(`
      CREATE TABLE IF NOT EXISTS downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL,
        filename TEXT,
        destination TEXT,
        total_size INTEGER DEFAULT 0,
        downloaded_size INTEGER DEFAULT 0,
        status TEXT DEFAULT 'IDLE',
        priority INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        started_at TEXT,
        completed_at TEXT,
        checksum TEXT,
        metadata TEXT DEFAULT '{}'
      )
    `);

    // Create chunks table - for resumable downloads
    db.exec(`
      CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        download_id INTEGER NOT NULL,
        start_byte INTEGER DEFAULT 0,
        end_byte INTEGER DEFAULT 0,
        current_byte INTEGER DEFAULT 0,
        status TEXT DEFAULT 'PENDING',
        worker_id TEXT,
        retry_count INTEGER DEFAULT 0,
        FOREIGN KEY (download_id) REFERENCES downloads(id) ON DELETE CASCADE
      )
    `);

    // Create events table - for observability
    db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        download_id INTEGER,
        event_type TEXT NOT NULL,
        timestamp TEXT DEFAULT (datetime('now')),
        metadata TEXT DEFAULT '{}',
        FOREIGN KEY (download_id) REFERENCES downloads(id) ON DELETE SET NULL
      )
    `);

    // Create configuration table
    db.exec(`
      CREATE TABLE IF NOT EXISTS configuration (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);

    // Insert default config
    const existing = db.prepare('SELECT key FROM configuration WHERE key = ?').get('bandwidth_limit');
    if (!existing) {
      db.prepare('INSERT INTO configuration (key, value) VALUES (?, ?)').run('bandwidth_limit', '0');
    }

    return db;
  }

  /**
   * Create a new download entry
   */
  createDownload(url, options = {}) {
    const stmt = this.db.prepare(`
      INSERT INTO downloads (url, filename, destination, total_size, status, priority)
      VALUES (?, ?, ?, ?, 'IDLE', ?)
    `);

    const result = stmt.run(url, options.filename, options.destination, options.total_size || 0, options.priority || 0);
    const id = result.lastInsertRowid;

    // Emit creation event
    this._emitEvent(id, 'DOWNLOAD_CREATED', { url, filename: options.filename });

    return { id, ...options };
  }

  /**
   * Get download by ID
   */
  getDownload(id) {
    const stmt = this.db.prepare('SELECT * FROM downloads WHERE id = ?');
    return stmt.get(id);
  }

  /**
   * Update download state
   */
  updateDownloadState(id, state, options = {}) {
    const { downloaded_size, total_size, started_at, completed_at, checksum } = options;
    
    const fields = [];
    const values = [];

    if (downloaded_size !== undefined) {
      fields.push('downloaded_size = ?');
      values.push(downloaded_size);
    }
    if (total_size !== undefined) {
      fields.push('total_size = ?');
      values.push(total_size);
    }
    if (started_at) {
      fields.push('started_at = ?');
      values.push(started_at);
    }
    if (completed_at) {
      fields.push('completed_at = ?');
      values.push(completed_at);
    }
    if (checksum) {
      fields.push('checksum = ?');
      values.push(checksum);
    }
    fields.push('status = ?');
    values.push(state);

    values.push(id);

    const sql = `UPDATE downloads SET ${fields.join(', ')} WHERE id = ?`;
    const stmt = this.db.prepare(sql);
    stmt.run(...values);

    this._emitEvent(id, `DOWNLOAD_${state.toUpperCase()}`, {});
  }

  /**
   * Get all downloads
   */
  getAllDownloads() {
    const stmt = this.db.prepare('SELECT * FROM downloads ORDER BY priority DESC, created_at ASC');
    return stmt.all();
  }

  /**
   * Create a chunk record
   */
  createChunk(downloadId, startByte, endByte) {
    const stmt = this.db.prepare(`
      INSERT INTO chunks (download_id, start_byte, end_byte, status)
      VALUES (?, ?, ?, 'PENDING')
    `);
    stmt.run(downloadId, startByte, endByte);
    return this.getChunkByDownloadAndRange(downloadId, startByte, endByte);
  }

  /**
   * Get chunk by download ID and byte range
   */
  getChunkByDownloadAndRange(downloadId, startByte, endByte) {
    const stmt = this.db.prepare(
      'SELECT * FROM chunks WHERE download_id = ? AND start_byte = ? AND end_byte = ?'
    );
    return stmt.get(downloadId, startByte, endByte);
  }

  /**
   * Update chunk progress
   */
  updateChunkProgress(downloadId, startByte, endByte, currentByte, status) {
    const stmt = this.db.prepare(`
      UPDATE chunks 
      SET current_byte = ?, status = ?
      WHERE download_id = ? AND start_byte = ? AND end_byte = ?
    `);
    stmt.run(currentByte, status, downloadId, startByte, endByte);

    // Emit chunk completed event
    this._emitEvent(downloadId, 'CHUNK_COMPLETED', { 
      startByte, 
      endByte, 
      currentByte 
    });
  }

  /**
   * Get all chunks for a download
   */
  getChunks(downloadId) {
    const stmt = this.db.prepare('SELECT * FROM chunks WHERE download_id = ? ORDER BY start_byte');
    return stmt.all(downloadId);
  }

  /**
   * Get completed chunks count
   */
  getCompletedChunks(downloadId) {
    const stmt = this.db.prepare(
      'SELECT COUNT(*) as count FROM chunks WHERE download_id = ? AND status = ?'
    );
    return stmt.get(downloadId, 'COMPLETED').count;
  }

  /**
   * Get pending chunks count
   */
  getPendingChunks(downloadId) {
    const stmt = this.db.prepare(
      'SELECT COUNT(*) as count FROM chunks WHERE download_id = ? AND status = ?'
    );
    return stmt.get(downloadId, 'PENDING').count;
  }

  /**
   * Emit event to the database
   */
  _emitEvent(downloadId, eventType, metadata) {
    const stmt = this.db.prepare(`
      INSERT INTO events (download_id, event_type, metadata)
      VALUES (?, ?, ?)
    `);
    stmt.run(downloadId, eventType, JSON.stringify(metadata));
  }

  /**
   * Close the database
   */
  close() {
    this.db.close();
  }
}

module.exports = PersistenceLayer;