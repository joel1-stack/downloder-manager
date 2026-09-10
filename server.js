const express = require('express');
const path = require('path');
const DownloadManager = require('./src/downloadManager');

const app = express();
const PORT = 3000;
const manager = new DownloadManager({ dbPath: './download_manager.db' });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Download CRUD
app.get('/api/downloads', (req, res) => res.json(manager.getAllDownloads()));
app.get('/api/downloads/:id', (req, res) => {
  const dl = manager.getDownload(parseInt(req.params.id));
  if (!dl) return res.status(404).json({ error: 'Not found' });
  res.json(dl);
});
app.post('/api/downloads', (req, res) => {
  const { url, filename, priority } = req.body;
  if (!url) return res.status(400).json({ error: 'URL required' });
  try { res.json(manager.startDownload(url, { filename, priority })); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
app.post('/api/downloads/:id/pause', (req, res) => {
  try { res.json(manager.pauseDownload(parseInt(req.params.id))); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
app.post('/api/downloads/:id/resume', (req, res) => {
  try { res.json(manager.resumeDownload(parseInt(req.params.id))); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
app.post('/api/downloads/:id/cancel', (req, res) => {
  try { res.json(manager.cancelDownload(parseInt(req.params.id))); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
app.delete('/api/downloads/:id', (req, res) => {
  try { res.json(manager.removeDownload(parseInt(req.params.id))); }
  catch (e) { res.status(400).json({ error: e.message }); }
});

// Events
app.get('/api/downloads/:id/events', (req, res) => res.json(manager.getEvents(parseInt(req.params.id))));

// History query
app.get('/api/history', (req, res) => {
  const { status, filename } = req.query;
  res.json(manager.queryHistory({ status, filename }));
});

// System
app.get('/api/system', (req, res) => res.json(manager.getSystemStatus()));
app.post('/api/system/bandwidth', (req, res) => {
  const { limit } = req.body;
  manager.setBandwidthLimit(limit || 0);
  res.json({ success: true, limit: limit || 0 });
});
app.post('/api/system/concurrent', (req, res) => {
  const { max } = req.body;
  manager.setMaxConcurrent(max || 4);
  res.json({ success: true, max: max || 4 });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n  Download Manager running at http://localhost:${PORT}\n`);
});

process.on('SIGINT', () => { manager.close(); process.exit(0); });
process.on('SIGTERM', () => { manager.close(); process.exit(0); });
process.on('uncaughtException', (err) => console.error('[ERROR]', err.message));
process.on('unhandledRejection', (err) => console.error('[ERROR]', err));