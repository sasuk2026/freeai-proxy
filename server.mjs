import http from 'node:http';

const PORT = Number(process.env.PORT || 10000);
const TIMEOUT = Number(process.env.UPSTREAM_TIMEOUT || 20000);
const COOLDOWN_MS = Number(process.env.COOLDOWN_MS || 45000);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type',
  'Access-Control-Max-Age': '86400',
};

// 免费上游：Pollinations 匿名通道（实测能回中文）。
// 'dead' = 真不存在/永久失效 → 废除；'temp' = 限流/超时/占位 → 冷却，不废除。
const POLL_EP = 'https://text.pollinations.ai/openai';
const MODELS = ['openai-fast', 'gpt-oss-20b', 'openai', 'mistral', 'qwen-coder'];

let POOL = MODELS.map(m => ({
  id: m, provider: 'pollinations', upstreamModel: m, endpoint: POLL_EP,
  dead: false, lastOk: 0, lastFailWhy: '',
}));

let LAST_TEST = 0;
let TESTING = false;
let COOLDOWN_UNTIL = 0;
const inCooldown = () => Date.now() < COOLDOWN_UNTIL;
const tripCooldown = () => { COOLDOWN_UNTIL = Date.now() + COOLDOWN_MS; };
const confirmed = () => POOL.filter(c => !c.dead && c.lastOk > 0).sort((a, b) => b.lastOk - a.lastOk);

function jsonRes(res, code, obj, extra) {
  res.writeHead(code, { ...CORS, 'Content-Type': 'application/json; charset=utf-8', ...(extra || {}) });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise(resolve => {
    let b = '';
    req.on('data', c => { b += c; });
    req.on('end', () => { try { resolve(JSON.parse(b || '{}')); } catch { resolve({}); } });
  });
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
function isDead(status, txt) {
  if (status === 404 || status === 401 || status === 403) return true;
  return /NOT_FOUND|invalid.?model|unknown model/i.test(txt || '');
}
async function callUpstream(c, messages, maxTokens) {
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), TIMEOUT);
  try {
    const r = await fetch(c.endpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: c.upstreamModel, messages, max_tokens: maxTokens || 16, stream: false }),
      signal: ctl.signal,
    });
    clearTimeout(t);
    const txt = await r.text();
    if (!r.ok) return { kind: isDead(r.status, txt) ? 'dead' : 'temp', why: 'HTTP ' + r.status + ' ' + txt.slice(0, 60) };
    let d; try { d = JSON.parse(txt); } catch { return { kind: 'temp', why: 'limit-placeholder: ' + txt.slice(0, 30) }; }
    const content = d?.choices?.[0]?.message?.content ?? d?.choices?.[0]?.text;
    if (!content || !String(content).trim()) return { kind: 'temp', why: 'empty content' };
    return { kind: 'ok', data: d, content: String(content) };
  } catch (e) {
    clearTimeout(t);
    return { kind: 'temp', why: String(e.message || e).slice(0, 50) };
  }
}
// 温和体检：串行、最多几个、遇限流即冷却并停止，绝不把额度打爆
async function selftest() {
  if (TESTING) return confirmed();
  if (inCooldown()) return confirmed();
  TESTING = true;
  try {
    const order = [...confirmed(), ...POOL.filter(c => !c.dead && c.lastOk === 0)].slice(0, 4);
    for (const c of order) {
      const r = await callUpstream(c, [{ role: 'user', content: 'Reply with exactly: OK' }], 8);
      if (r.kind === 'ok') { c.lastOk = Date.now(); c.lastFailWhy = ''; COOLDOWN_UNTIL = 0; }
      else if (r.kind === 'dead') { c.dead = true; console.log('[abolish] ' + c.id); }
      else { c.lastFailWhy = r.why; tripCooldown(); console.log('[cooldown] ' + c.id + ' -> ' + r.why); break; }
      await sleep(500);
    }
    LAST_TEST = Date.now();
  } finally { TESTING = false; }
  return confirmed();
}

const server = http.createServer(async (req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, { ok: true, confirmed: confirmed().length, dead: POOL.filter(c => c.dead).length, cooldown: inCooldown(), lastTest: LAST_TEST });

  if (u.pathname === '/v1/selftest') {
    const ok = await selftest();
    return jsonRes(res, 200, {
      count: ok.length, cooldown: inCooldown(),
      abolished: POOL.filter(c => c.dead).map(c => c.id),
      available: ok.map(a => a.id),
      note: '仅列真回过话的模型；限流/占位的进入冷却，稍等自动恢复，不废除',
    });
  }

  if (u.pathname === '/v1/models') {
    let ok = confirmed();
    if (!ok.length && !inCooldown()) ok = await selftest();
    // 冷却期且有历史确认：把历史确认的也列出（别让列表变空）
    if (!ok.length) ok = POOL.filter(c => !c.dead && c.lastOk > 0);
    return jsonRes(res, 200, { object: 'list', data: (ok.length ? ok : POOL.filter(c => !c.dead)).map(a => ({
      id: a.id, object: 'model', owned_by: a.provider, context_length: 128000, capabilities: { tool_calling: true },
    })) });
  }

  if (u.pathname === '/v1/chat/completions' && req.method === 'POST') {
    const body = await readBody(req);
    const auth = req.headers.authorization || '';
    if (!/^Bearer\s+sk-\S{6,}/i.test(auth))
      return jsonRes(res, 401, { error: { message: '需要 API Key：Authorization: Bearer sk-...', code: 'auth_required' } });

    if (inCooldown())
      return jsonRes(res, 429, {
        error: { message: '免费额度临时限流，冷却中，约 ' + Math.ceil((COOLDOWN_UNTIL - Date.now()) / 1000) + ' 秒后自动恢复，请稍后再试（未废除任何模型）', type: 'rate_limited', code: 'cooldown' },
      }, { 'Retry-After': String(Math.ceil((COOLDOWN_UNTIL - Date.now()) / 1000)) });

    const messages = body.messages || [{ role: 'user', content: 'hi' }];
    const maxTokens = body.max_tokens || 512;
    const conf = confirmed();
    let order = [...conf, ...POOL.filter(c => !c.dead && c.lastOk === 0)];
    const wanted = order.find(c => c.id === body.model);
    if (wanted) order = [wanted, ...order.filter(c => c !== wanted)];
    order = order.slice(0, 3);   // 最多试 3 个，杜绝"越失败越打"

    let lastWhy = 'unknown';
    for (const c of order) {
      const r = await callUpstream(c, messages, maxTokens);
      if (r.kind === 'ok') {
        c.lastOk = Date.now(); c.lastFailWhy = ''; COOLDOWN_UNTIL = 0;
        const d = r.data;
        if (body.stream) {
          res.writeHead(200, { ...CORS, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
          const base = { id: 'chatcmpl-' + Date.now(), object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model || c.id };
          res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: { role: 'assistant', content: r.content }, finish_reason: null }] }) + '\n\n');
          res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] }) + '\n\n');
          res.write('data: [DONE]\n\n'); res.end(); return;
        }
        return jsonRes(res, 200, d);
      }
      lastWhy = r.why; c.lastFailWhy = r.why;
      if (r.kind === 'dead') { c.dead = true; console.log('[abolish-live] ' + c.id); }
      else { tripCooldown(); break; }   // 限流 → 立即冷却，不再打其它通道
    }
    return jsonRes(res, 429, {
      error: { message: '免费额度临时限流，已进入冷却，约 ' + Math.ceil(COOLDOWN_MS / 1000) + ' 秒后自动恢复。最后错误：' + lastWhy, type: 'rate_limited', code: 'cooldown' },
    }, { 'Retry-After': String(Math.ceil(COOLDOWN_MS / 1000)) });
  }

  jsonRes(res, 200, { name: 'freeai-proxy', confirmed: confirmed().length, cooldown: inCooldown(), endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'] });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT);
  selftest();   // 启动时温和体检一次；不再定时打，避免周期性耗尽额度
});
