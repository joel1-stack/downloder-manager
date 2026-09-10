const Database = require('better-sqlite3');

class DatabaseManager {
  constructor(dbPath = './download_manager.db') {
    this.dbPath = dbPath;
    this.db = this._initDatabase();
  }

  _initDatabase() {
    const db = new Database(this.dbPath, { readonly: false });
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('foreign_keys = ON');

    db.exec(`
      CREATE TABLE IF NOT EXISTS downloads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL,
        filename TEXT DEFAULT 'unknown',
        total_size INTEGER DEFAULT 0,
        downloaded_bytes INTEGER DEFAULT 0,
        status TEXT DEFAULT 'IDLE',
        priority INTEGER DEFAULT 0,
        destination TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now')),
        started_at TEXT,
        completed_at TEXT,
        checksum TEXT
      );
      CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        download_id INTEGER NOT NULL,
        start_byte INTEGER DEFAULT 0,
        end_byte INTEGER DEFAULT 0,
        downloaded_bytes INTEGER DEFAULT 0,
        status TEXT DEFAULT 'PENDING',
        FOREIGN KEY (download_id) REFERENCES downloads(id) ON DELETE CASCADE
      );
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        download_id INTEGER,
        event_type TEXT NOT NULL,
        timestamp TEXT DEFAULT (datetime('now')),
        metadata TEXT DEFAULT '{}'
      );
    `);

    return db;
  }

  createDownload(url, opts = {}) {
    const r = this.db.prepare(
      `INSERT INTO downloads (url, filename, total_size, status, priority, destination) VALUES (?, ?, ?, 'IDLE', ?, ?)`
    ).run(url, opts.filename || 'unknown', opts.total_size || 0, opts.priority || 0, opts.destination || '');
    return r.lastInsertRowid;
  }

  getDownload(id) { return this.db.prepare('SELECT * FROM downloads WHERE id = ?').get(id); }
  getAllDownloads() { return this.db.prepare('SELECT * FROM downloads ORDER BY id DESC').all(); }

  updateDownload(id, fields) {
    const keys = Object.keys(fields);
    if (!keys.length) return;
    const sets = keys.map(k => `${k} = ?`).join(', ');
    const vals = keys.map(k => fields[k]);
    vals.push(id);
    this.db.prepare(`UPDATE downloads SET ${sets} WHERE id = ?`).run(...vals);
  }

  createChunk(downloadId, startByte, endByte) {
    this.db.prepare(
      `INSERT INTO chunks (download_id, start_byte, end_byte, status) VALUES (?, ?, ?, 'PENDING')`
    ).run(downloadId, startByte, endByte);
  }

  getChunks(downloadId) { return this.db.prepare('SELECT * FROM chunks WHERE download_id = ? ORDER BY start_byte').all(downloadId); }
  getPendingChunks(downloadId) { return this.db.prepare('SELECT * FROM chunks WHERE download_id = ? AND status = ?').all(downloadId, 'PENDING'); }

  updateChunk(downloadId, startByte, endByte, fields) {
    const keys = Object.keys(fields);
    if (!keys.length) return;
    const sets = keys.map(k => `${k} = ?`).join(', ');
    const vals = keys.map(k => fields[k]);
    vals.push(downloadId, startByte, endByte);
    this.db.prepare(`UPDATE chunks SET ${sets} WHERE download_id = ? AND start_byte = ? AND end_byte = ?`).run(...vals);
  }

  addEvent(downloadId, eventType, metadata = {}) {
    this.db.prepare(`INSERT INTO events (download_id, event_type, metadata) VALUES (?, ?, ?)`).run(downloadId, eventType, JSON.stringify(metadata));
  }

  getEvents(downloadId, limit = 50) {
    return this.db.prepare('SELECT * FROM events WHERE download_id = ? ORDER BY id DESC LIMIT ?').all(downloadId, limit);
  }

  close() { this.db.close(); }
}

module.exports = DatabaseManager;