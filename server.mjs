import http from 'node:http';

const PORT = Number(process.env.PORT || 10000);
const TIMEOUT = Number(process.env.UPSTREAM_TIMEOUT || 20000);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type',
  'Access-Control-Max-Age': '86400',
};

// ── 多家「公益免费 / 免 key / 免登录」上游，融合成一个 OpenAI 兼容端点 ──
// 每条通道有自己的请求体格式与响应解析方式（见 buildRequest / extractContent）。
const CH = {
  poll: { label: 'pollinations', kind: 'oai',     endpoint: 'https://text.pollinations.ai/openai' },
  nw:   { label: 'netwrck',      kind: 'netwrck', endpoint: 'https://netwrck.com/api/chatpred_or' },
  nwfb: { label: 'netwrck',      kind: 'netwrck', endpoint: 'https://netwrck.com/api/chatpred' },
};

// 对外 model id → 通道 + 上游真实模型名。想加新公益通道，往这里加一行即可。
const DEFS = [
  { id: 'openai-fast',          ch: 'poll', up: 'openai-fast' },
  { id: 'gpt-oss-20b',          ch: 'poll', up: 'gpt-oss-20b' },
  { id: 'openai',               ch: 'poll', up: 'openai' },
  { id: 'netwrck/ling-flash',   ch: 'nw',   up: 'inclusionai/ling-3.0-flash' },
  { id: 'netwrck/skyfall-36b',  ch: 'nw',   up: 'thedrummer/skyfall-36b-v2' },
  { id: 'netwrck/mythomax-13b', ch: 'nw',   up: 'gryphe/mythomax-l2-13b' },
  { id: 'netwrck/default',      ch: 'nwfb', up: 'thedrummer/skyfall-36b-v2' },
];

let POOL = DEFS.map(d => ({
  id: d.id, provider: CH[d.ch].label, kind: CH[d.ch].kind,
  endpoint: CH[d.ch].endpoint, upstreamModel: d.up,
  dead: false, lastOk: 0, lastFailWhy: '',
}));

let LAST_TEST = 0, TESTING = false;
const confirmed = () => POOL.filter(c => !c.dead && c.lastOk > 0).sort((a, b) => b.lastOk - a.lastOk);
const oaiNow = () => Math.floor(Date.now() / 1000);

function jsonRes(res, code, obj, extra) {
  res.writeHead(code, { ...CORS, 'Content-Type': 'application/json; charset=utf-8', ...(extra || {}) });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise(resolve => {
    let b = ''; req.on('data', c => { b += c; });
    req.on('end', () => { try { resolve(JSON.parse(b || '{}')); } catch { resolve({}); } });
  });
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

// OpenAI 格式响应壳（netwrck 等非 OpenAI 原生响应统一包成标准格式）
function wrapOAI(model, content) {
  return {
    id: 'chatcmpl-' + Date.now(), object: 'chat.completion', created: oaiNow(), model,
    choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}
// 从各种响应体里抠出真实文本；限流占位/假 OK 一律返回空（不误判为可用）
function extractContent(txt) {
  let d;
  try { d = JSON.parse(txt); }
  catch { const t = (txt || '').trim(); return (t && t.length < 500 && !/^OK$/i.test(t)) ? t : ''; }
  if (typeof d === 'string') return /^OK$/i.test(d.trim()) ? '' : d.trim();
  if (Array.isArray(d) || !d || typeof d !== 'object') return '';
  if (d.choices) return String(d.choices[0]?.message?.content ?? d.choices[0]?.text ?? '').trim();
  if (typeof d.data === 'string') return d.data.trim();
  if (d.data && typeof d.data === 'object') return String(d.data.content ?? d.data.text ?? JSON.stringify(d.data)).trim();
  for (const k of ['content', 'response', 'text', 'answer', 'result', 'output', 'completion', 'message']) {
    if (typeof d[k] === 'string' && d[k].trim()) return d[k].trim();
  }
  return '';
}
function toQuery(messages) {
  const u = [...messages].reverse().find(m => m.role === 'user');
  return u ? String(u.content) : 'hi';
}
function toContext(messages) {
  return messages.filter(m => m.role === 'system').map(m => String(m.content)).join('\n');
}
function buildRequest(c, messages, maxTokens) {
  if (c.kind === 'netwrck') {
    return {
      url: c.endpoint,
      headers: { 'Content-Type': 'application/json', 'origin': 'https://netwrck.com', 'referer': 'https://netwrck.com/' },
      body: { query: toQuery(messages), context: toContext(messages), model: c.upstreamModel, max_tokens: maxTokens || 16 },
    };
  }
  return {
    url: c.endpoint,
    headers: { 'Content-Type': 'application/json' },
    body: { model: c.upstreamModel, messages, max_tokens: maxTokens || 16, stream: false },
  };
}
function isDead(status, txt) {
  if (status === 404 || status === 401 || status === 403) return true;
  return /NOT_FOUND|Model not found|invalid.?model|unknown model|missing API key|Unauthorized/i.test(txt || '');
}
async function callUpstream(c, messages, maxTokens) {
  const { url, headers, body } = buildRequest(c, messages, maxTokens);
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), TIMEOUT);
  try {
    const r = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body), signal: ctl.signal });
    clearTimeout(t);
    const txt = await r.text();
    if (!r.ok) return { kind: isDead(r.status, txt) ? 'dead' : 'temp', why: 'HTTP ' + r.status + ' ' + txt.slice(0, 60) };
    const content = extractContent(txt);
    if (!content || /^OK$/i.test(content.trim())) return { kind: 'temp', why: 'limit/placeholder: ' + txt.slice(0, 30) };
    // 原生 OpenAI 响应直接透传；其它包装成标准格式
    let data; try { data = JSON.parse(txt); } catch { data = null; }
    const native = data && data.choices && data.choices[0];
    return { kind: 'ok', data: native ? data : wrapOAI(c.id, content), content };
  } catch (e) {
    clearTimeout(t);
    return { kind: 'temp', why: String(e.message || e).slice(0, 50) };
  }
}

// 串行、温和体检：逐个模型真发一句话，能回话才确认；真失效(404/401)才废除；限流仅标记、不废除
async function selftest(force) {
  if (TESTING) return confirmed();
  if (!force && confirmed().length && Date.now() - LAST_TEST < 20000) return confirmed();
  TESTING = true;
  try {
    for (const c of POOL) {
      if (c.dead) continue;
      const r = await callUpstream(c, [{ role: 'user', content: 'Reply in one short sentence: what is 2+2?' }], 24);
      if (r.kind === 'ok') { c.lastOk = Date.now(); c.lastFailWhy = ''; }
      else if (r.kind === 'dead') { c.dead = true; console.log('[abolish] ' + c.id); }
      else { c.lastFailWhy = r.why; console.log('[temp] ' + c.id + ' -> ' + r.why); }
      await sleep(450);   // 温和间隔，绝不并发猛打
    }
    LAST_TEST = Date.now();
    console.log('[selftest] confirmed=' + confirmed().length + ' dead=' + POOL.filter(c => c.dead).length
      + ' [' + confirmed().map(a => a.id).join(',') + ']');
  } finally { TESTING = false; }
  return confirmed();
}

// 跨通道轮转排序：保证 failover 优先尝试「不同通道」，一条限流自动绕到另一家
function pickOrder(wantedId) {
  const conf = confirmed();
  const byProv = {};
  for (const c of conf) (byProv[c.provider] ||= []).push(c);
  const merged = [];
  let go = true;
  while (go) { go = false; for (const p of Object.keys(byProv)) { const a = byProv[p]; if (a.length) { merged.push(a.shift()); go = true; } } }
  for (const c of POOL) if (!c.dead && c.lastOk === 0 && !merged.includes(c)) merged.push(c);
  if (wantedId) { const w = merged.find(c => c.id === wantedId); if (w) { merged.splice(merged.indexOf(w), 1); merged.unshift(w); } }
  return merged;
}

const server = http.createServer(async (req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, {
      ok: true, confirmed: confirmed().length, dead: POOL.filter(c => c.dead).length,
      channels: [...new Set(POOL.filter(c => !c.dead && c.lastOk > 0).map(c => c.provider))],
      lastTest: LAST_TEST,
    });

  if (u.pathname === '/v1/selftest') {
    const ok = await selftest(true);
    return jsonRes(res, 200, {
      count: ok.length,
      channels: [...new Set(ok.map(a => a.provider))],
      abolished: POOL.filter(c => c.dead).map(c => c.id),
      available: ok.map(a => a.id),
      note: '多家公益免费API融合，只列真回过话的模型；限流/占位的标记但不废除',
    });
  }

  if (u.pathname === '/v1/models') {
    let ok = confirmed();
    if (!ok.length) ok = await selftest(false);
    if (!ok.length) ok = POOL.filter(c => !c.dead && c.lastOk > 0);
    const list = ok.length ? ok : POOL.filter(c => !c.dead);
    return jsonRes(res, 200, {
      object: 'list',
      data: list.map(a => ({
        id: a.id, object: 'model', owned_by: a.provider,
        context_length: a.kind === 'netwrck' ? 32000 : 128000,
        capabilities: { tool_calling: true },
      })),
    });
  }

  if (u.pathname === '/v1/chat/completions' && req.method === 'POST') {
    const body = await readBody(req);
    const auth = req.headers.authorization || '';
    if (!/^Bearer\s+sk-\S{6,}/i.test(auth))
      return jsonRes(res, 401, { error: { message: '需要 API Key：Authorization: Bearer sk-...', code: 'auth_required' } });

    const messages = body.messages || [{ role: 'user', content: 'hi' }];
    const maxTokens = body.max_tokens || 512;
    let order = pickOrder(body.model);
    // 用户没指定模型时，从头试；最多试 4 个且跨通道，杜绝"越失败越打"
    order = order.slice(0, 4);

    let lastWhy = 'unknown';
    for (const c of order) {
      const r = await callUpstream(c, messages, maxTokens);
      if (r.kind === 'ok') {
        c.lastOk = Date.now(); c.lastFailWhy = '';
        if (body.stream) {
          res.writeHead(200, { ...CORS, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
          const base = { id: 'chatcmpl-' + Date.now(), object: 'chat.completion.chunk', created: oaiNow(), model: body.model || c.id };
          res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: { role: 'assistant', content: r.content }, finish_reason: null }] }) + '\n\n');
          res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] }) + '\n\n');
          res.write('data: [DONE]\n\n'); res.end(); return;
        }
        return jsonRes(res, 200, r.data);
      }
      lastWhy = r.why; c.lastFailWhy = r.why;
      if (r.kind === 'dead') { c.dead = true; console.log('[abolish-live] ' + c.id); }
      // temp(限流)：继续尝试下一个不同通道，不阻断
    }
    return jsonRes(res, 429, {
      error: { message: '所有公益通道此刻均限流/不可用，请稍等约 30-45 秒再试。最后错误：' + lastWhy, type: 'rate_limited', code: 'upstream_busy' },
    }, { 'Retry-After': '30' });
  }

  jsonRes(res, 200, {
    name: 'freeai-proxy', confirmed: confirmed().length,
    channels: [...new Set(POOL.map(c => c.provider))],
    endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'],
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT);
  selftest(true);   // 启动时温和体检一次，把真能回话的公益通道融合进列表
});
