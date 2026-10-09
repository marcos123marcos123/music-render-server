require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { Pool } = require('pg');
const path = require('path');

// ============================================================
// CONFIGURACIÓN
// ============================================================
const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;

const app = express();
app.use(express.json({ limit: '1mb' }));

// ============================================================
// ESTADO EN MEMORIA (RAM - se pierde al dormir Render)
// ============================================================
const state = {
  // device_id → { ws, device_id, name, queue, settings, last_seen }
  devices: new Map(),
  // WebSockets de administradores activos
  adminSockets: new Set(),
  // Timestamp de última actividad del admin
  adminActiveUntil: 0,
  // Buffer de eventos pendientes
  events: [],
};

// ============================================================
// NEON (solo para persistencia cuando hay cambios)
// ============================================================
let pool = null;
if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 3,
    idleTimeoutMillis: 30000,
  });

  pool.on('error', (err) => {
    console.error('[Neon] Error:', err.message);
  });

  console.log('[Neon] Pool configurado');
} else {
  console.warn('[Neon] DATABASE_URL no configurada — sin persistencia');
}

// Inicializar tablas
async function initDb() {
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS devices (
        device_id TEXT PRIMARY KEY,
        name TEXT,
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        settings_json JSONB
      );
      CREATE TABLE IF NOT EXISTS queue (
        id SERIAL PRIMARY KEY,
        device_id TEXT,
        video_id TEXT,
        title TEXT,
        added_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS global_settings (
        key TEXT PRIMARY KEY,
        value JSONB
      );
      CREATE TABLE IF NOT EXISTS auth (
        key TEXT PRIMARY KEY,
        value TEXT
      );
    `);
    console.log('[Neon] Tablas verificadas');
  } catch (e) {
    console.error('[Neon] Error inicializando tablas:', e);
  }
}

// ============================================================
// HTTP ENDPOINTS
// ============================================================

// Health check (para cron-job.org)
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    ts: Date.now(),
    devices: state.devices.size,
    admins: state.adminSockets.size,
    admin_active: state.adminActiveUntil > Date.now(),
  });
});

// Presencia — el APK pregunta si hay admin activo
app.get('/api/presence', (req, res) => {
  const active = state.adminActiveUntil > Date.now();
  res.json({
    admin: active,
    until: state.adminActiveUntil,
    ts: Date.now(),
  });
});

// Devices list
app.get('/api/devices', (req, res) => {
  const now = Date.now();
  const list = [];
  for (const [id, dev] of state.devices.entries()) {
    list.push({
      device_id: id,
      name: dev.name || id,
      connected: dev.ws && dev.ws.readyState === 1,
      last_seen: dev.last_seen || now,
    });
  }
  list.sort((a, b) => (b.last_seen || 0) - (a.last_seen || 0));
  res.json(list);
});

// State — devuelve el estado del dispositivo (o de todos)
app.get('/api/state', (req, res) => {
  const target = req.query.device;
  const now = Date.now();
  const states = [];
  for (const [id, dev] of state.devices.entries()) {
    if (target && id !== target) continue;
    if (!dev.ws || dev.ws.readyState !== 1) continue;
    if (dev.current) {
      states.push({ ...dev.current, device_id: id });
    }
  }
  res.json(states);
});

// Now playing
app.get('/api/public/now-playing', (req, res) => {
  for (const [id, dev] of state.devices.entries()) {
    if (dev.current && dev.current.playing) {
      return res.json({ ...dev.current, device_id: id });
    }
  }
  res.json(null);
});

// Queue
app.get('/api/queue', (req, res) => {
  const target = req.query.device;
  const queues = [];
  for (const [id, dev] of state.devices.entries()) {
    if (target && id !== target) continue;
    if (dev.queue) {
      queues.push(...dev.queue.map(q => ({ ...q, device_id: id })));
    }
  }
  res.json(queues);
});

// Settings
app.get('/api/settings', (req, res) => {
  res.json({
    pro_enabled: true,
    search_enabled: true,
    queue_enabled: true,
    repeat_infinite: false,
    volume: 80,
  });
});

// Command — enviar comando al APK
app.post('/api/command', (req, res) => {
  const { command, video_id, device } = req.body;
  if (!command) return res.status(400).json({ error: 'falta command' });

  const target = device || Array.from(state.devices.keys())[0];
  if (!target) return res.status(503).json({ error: 'sin dispositivos' });

  const dev = state.devices.get(target);
  if (!dev || !dev.ws || dev.ws.readyState !== 1) {
    return res.status(503).json({ error: 'dispositivo no conectado' });
  }

  try {
    dev.ws.send(JSON.stringify({ type: 'command', command, video_id }));
    res.json({ ok: true, sent_to: target });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Queue add
app.post('/api/queue', (req, res) => {
  const { video_id, title, device } = req.body;
  if (!video_id) return res.status(400).json({ error: 'falta video_id' });

  const target = device || Array.from(state.devices.keys())[0];
  if (!target) return res.status(503).json({ error: 'sin dispositivos' });

  const dev = state.devices.get(target);
  if (!dev || !dev.ws || dev.ws.readyState !== 1) {
    return res.status(503).json({ error: 'dispositivo no conectado' });
  }

  try {
    dev.ws.send(JSON.stringify({ type: 'queue_add', video_id, title }));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Search proxy (a YouTube via Worker)
app.get('/api/search', async (req, res) => {
  const q = req.query.q || '';
  if (!q) return res.json([]);

  try {
    const url = `https://www.youtube.com/youtubei/v1/search?prettyPrint=false`;
    const body = {
      context: { client: { clientName: 'WEB', clientVersion: '2.20240101.00.00' } },
      query: q,
    };
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await r.json();
    const results = [];
    const sections = data?.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents || [];
    for (const section of sections) {
      const items = section?.itemSectionRenderer?.contents || [];
      for (const item of items) {
        const v = item?.videoRenderer;
        if (!v) continue;
        const videoId = v.videoId || '';
        const title = v.title?.runs?.[0]?.text || '';
        const thumb = v.thumbnail?.thumbnails?.[0]?.url || '';
        if (videoId) results.push({ video_id: videoId, title, thumb });
      }
    }
    res.json(results.slice(0, 10));
  } catch (e) {
    res.json([]);
  }
});

// Login
app.post('/api/login', (req, res) => {
  const { token } = req.body;
  // TODO: validar contra Neon
  res.json({ ok: true });
});

// ============================================================
// SERVIR HTML
// ============================================================
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// ============================================================
// SERVIDOR HTTP + WEBSOCKET
// ============================================================
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const pathname = url.pathname;

  if (pathname === '/apk') {
    // WebSocket del APK
    wss.handleUpgrade(request, socket, head, (ws) => {
      const deviceId = url.searchParams.get('device') || 'default';
      const deviceName = url.searchParams.get('name') || deviceId;
      handleApkConnection(ws, deviceId, deviceName);
    });
  } else if (pathname === '/ws/admin') {
    // WebSocket del admin (para presencia)
    wss.handleUpgrade(request, socket, head, (ws) => {
      handleAdminConnection(ws);
    });
  } else {
    socket.destroy();
  }
});

// ============================================================
// HANDLER APK
// ============================================================
function handleApkConnection(ws, deviceId, deviceName) {
  console.log(`[APK] Conectado: ${deviceId} (${deviceName})`);

  const dev = state.devices.get(deviceId) || { device_id: deviceId, name: deviceName };
  dev.ws = ws;
  dev.name = deviceName;
  dev.last_seen = Date.now();
  state.devices.set(deviceId, dev);

  // Avisar al admin
  notifyAdmins({ type: 'device_connected', device_id: deviceId, name: deviceName });

  // Ping cada 25s para mantener la conexión viva
  const pingInterval = setInterval(() => {
    if (ws.readyState === 1) {
      try { ws.send(JSON.stringify({ type: 'ping', ts: Date.now() })); } catch (e) {}
    } else {
      clearInterval(pingInterval);
    }
  }, 25000);

  ws.on('message', (data) => {
    dev.last_seen = Date.now();
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === 'pong') return;
      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
        return;
      }

      // Estado actual de reproducción
      if (msg.type === 'state') {
        dev.current = {
          title: msg.title || '',
          video_id: msg.video_id || '',
          position: msg.position || 0,
          duration: msg.duration || 0,
          playing: !!msg.playing,
        };
        notifyAdmins({ type: 'state', device_id: deviceId, state: dev.current });
        return;
      }

      // Cola
      if (msg.type === 'queue') {
        dev.queue = msg.queue || [];
        notifyAdmins({ type: 'queue', device_id: deviceId, queue: dev.queue });
        return;
      }

      // Settings
      if (msg.type === 'settings') {
        dev.settings = msg.settings || {};
        notifyAdmins({ type: 'settings', device_id: deviceId, settings: dev.settings });
        return;
      }

      // ACK de comandos
      if (msg.type === 'ack') {
        notifyAdmins({ type: 'ack', device_id: deviceId, command: msg.command, ok: msg.ok });
        return;
      }

    } catch (e) {
      console.error('[APK] Error parseando:', e.message);
    }
  });

  ws.on('close', () => {
    console.log(`[APK] Desconectado: ${deviceId}`);
    clearInterval(pingInterval);
    if (dev.ws === ws) dev.ws = null;
    notifyAdmins({ type: 'device_disconnected', device_id: deviceId });
  });

  ws.on('error', (e) => {
    console.error(`[APK] Error ${deviceId}:`, e.message);
  });
}

// ============================================================
// HANDLER ADMIN
// ============================================================
function handleAdminConnection(ws) {
  console.log('[Admin] Conectado');
  state.adminSockets.add(ws);
  state.adminActiveUntil = Date.now() + 5 * 60 * 1000; // 5 min

  ws.send(JSON.stringify({
    type: 'welcome',
    devices: Array.from(state.devices.keys()),
  }));

  ws.on('message', (data) => {
    state.adminActiveUntil = Date.now() + 5 * 60 * 1000;
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
      // Reenviar comandos al APK
      if (msg.type === 'command' && msg.device_id) {
        const dev = state.devices.get(msg.device_id);
        if (dev && dev.ws && dev.ws.readyState === 1) {
          dev.ws.send(JSON.stringify(msg));
        }
      }
    } catch (e) {}
  });

  ws.on('close', () => {
    console.log('[Admin] Desconectado');
    state.adminSockets.delete(ws);
    if (state.adminSockets.size === 0) {
      state.adminActiveUntil = Date.now() + 60 * 1000; // 1 min de gracia
    }
  });
}

function notifyAdmins(msg) {
  const data = JSON.stringify(msg);
  for (const ws of state.adminSockets) {
    if (ws.readyState === 1) {
      try { ws.send(data); } catch (e) {}
    }
  }
}

// ============================================================
// ARRANQUE
// ============================================================
// ============================================================
// NUEVOS ENDPOINTS — ping adaptativo
// ============================================================

// POST /api/presence — el navegador avisa que está abierto
app.post('/api/presence', (req, res) => {
  state.adminActiveUntil = Date.now() + 5 * 60 * 1000;
  res.json({ ok: true, until: state.adminActiveUntil });
});

// GET /api/ping — ping ligero del APK
app.get('/api/ping', (req, res) => {
  const active = state.adminActiveUntil > Date.now();
  res.json({
    admin_active: active,
    next_ping_ms: active ? 1000 : 7200000,
    ts: Date.now()
  });
});

// POST /api/events — eventos del APK (song change, queue, etc.)
app.post('/api/events', (req, res) => {
  const body = req.body || {};
  const device_id = body.device_id;
  const events = body.events || [];
  const snapshot = body.snapshot;

  if (device_id) {
    const dev = state.devices.get(device_id) || { device_id: device_id };
    dev.last_seen = Date.now();
    if (snapshot) dev.current = snapshot;
    state.devices.set(device_id, dev);
  }

  res.json({ ok: true, received: events.length });
});



// ============================================================
// POST /api/disconnect — forzar desconexión del APK
// ============================================================
app.post('/api/disconnect', (req, res) => {
  const target = (req.body && req.body.device) || 
                 (Array.from(state.devices.keys())[0]);
  if (!target) return res.status(503).json({ error: 'sin dispositivos' });

  const dev = state.devices.get(target);
  if (!dev || !dev.ws || dev.ws.readyState !== 1) {
    return res.status(503).json({ error: 'dispositivo no conectado' });
  }

  try {
    dev.ws.send(JSON.stringify({
      type: 'disconnect',
      reason: 'user_request',
      ts: Date.now()
    }));
    res.json({ ok: true, sent_to: target });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


// ============================================================
// POST /api/reconnect — forzar reconexión del APK
// ============================================================
app.post('/api/reconnect', (req, res) => {
  const target = (req.body && req.body.device) ||
                 (Array.from(state.devices.keys())[0]);
  if (!target) return res.status(503).json({ error: 'sin dispositivos' });

  const dev = state.devices.get(target);
  if (!dev || !dev.ws || dev.ws.readyState !== 1) {
    return res.status(503).json({ error: 'dispositivo no conectado' });
  }

  try {
    dev.ws.send(JSON.stringify({
      type: 'reconnect',
      reason: 'user_request',
      ts: Date.now()
    }));
    res.json({ ok: true, sent_to: target });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

server.listen(PORT, '0.0.0.0', async () => {
  console.log(`[Server] Escuchando en 0.0.0.0:${PORT}`);
  await initDb();
});

process.on('SIGTERM', () => {
  console.log('[Server] SIGTERM recibido, cerrando...');
  server.close(() => {
    if (pool) pool.end();
    process.exit(0);
  });
});
