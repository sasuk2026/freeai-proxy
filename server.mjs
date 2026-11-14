import http from 'node:http';

const PORT = Number(process.env.PORT || 10000);
const TIMEOUT = Number(process.env.UPSTREAM_TIMEOUT || 14000);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type',
  'Access-Control-Max-Age': '86400',
};

// ── 多家「公益免费 / 免 key / 免登录」上游，融合成一个 OpenAI 兼容端点 ──
const CH = {
  poll:  { label: 'pollinations', kind: 'oai',     endpoint: 'https://text.pollinations.ai/openai' },
  pollv1:{ label: 'pollinations', kind: 'oai',     endpoint: 'https://text.pollinations.ai/openai/chat/completions' },
  nw:    { label: 'netwrck',      kind: 'netwrck', endpoint: 'https://netwrck.com/api/chatpred_or' },
  nwfb:  { label: 'netwrck',      kind: 'netwrck', endpoint: 'https://netwrck.com/api/chatpred' },
};

// 对外 model id → 通道 + 上游真实模型名。core=1 的会在启动时真测，其余保留为「冷却/待验证」并按需真测。
// 「不可用直接废除」= 只对真失效(404/401premium/假占位)永久废除；免费额度限流(402/429)会自动恢复，保留。
const DEFS = [
  // pollinations —— 实测真能回话的
  { id: 'openai-fast',        ch: 'poll',  up: 'openai-fast',        core: 1 },
  { id: 'openai',             ch: 'poll',  up: 'openai',             core: 1 },
  { id: 'gpt-oss-20b',        ch: 'poll',  up: 'gpt-oss-20b',        core: 1 },
  // netwrck /chatpred_or —— 实测真回话的不同免费模型
  { id: 'nw/ling-flash',      ch: 'nw',    up: 'inclusionai/ling-3.0-flash',            core: 1 },
  { id: 'nw/skyfall-36b',     ch: 'nw',    up: 'thedrummer/skyfall-36b-v2',             core: 1 },
  { id: 'nw/mythomax-13b',    ch: 'nw',    up: 'gryphe/mythomax-l2-13b',                core: 1 },
  { id: 'nw/llama-70b',       ch: 'nw',    up: 'meta-llama/llama-3.3-70b-instruct',     core: 1 },
  { id: 'nw/mistral-7b',      ch: 'nw',    up: 'mistralai/mistral-7b-instruct:free',    core: 1 },
  { id: 'nw/olmo-32b',        ch: 'nw',    up: 'allenai/olmo-2-0325-32b-instruct',      core: 1 },
  { id: 'nw/nemotron-70b',    ch: 'nw',    up: 'nvidia/llama-3.1-nemotron-70b-instruct',core: 1 },
  // netwrck 备用端点 —— 同样真回话，作为独立 failover 通道
  { id: 'nw2/ling-flash',     ch: 'nwfb',  up: 'inclusionai/ling-3.0-flash',            core: 1 },
  { id: 'nw2/llama-70b',      ch: 'nwfb',  up: 'meta-llama/llama-3.3-70b-instruct',     core: 1 },
  { id: 'nw2/skyfall-36b',    ch: 'nwfb',  up: 'thedrummer/skyfall-36b-v2',             core: 0 },
  // pollinations —— 免费额度临时限流的真模型(会自动恢复，保留不废除)
  { id: 'mistral',            ch: 'poll',  up: 'mistral',            core: 0 },
  { id: 'llama',              ch: 'poll',  up: 'llama',              core: 0 },
  { id: 'deepseek',           ch: 'poll',  up: 'deepseek',           core: 0 },
  { id: 'deepseek-reasoning', ch: 'poll',  up: 'deepseek-reasoning', core: 0 },
  { id: 'gemini',             ch: 'poll',  up: 'gemini',             core: 0 },
  { id: 'phi',                ch: 'poll',  up: 'phi',                core: 0 },
  { id: 'claude',             ch: 'poll',  up: 'claude',             core: 0 },
  { id: 'roblox-rp',          ch: 'poll',  up: 'roblox-rp',          core: 0 },
  { id: 'unity',              ch: 'poll',  up: 'unity',              core: 0 },
  // netwrck —— 免费预览额度临时限流的真模型(会自动恢复，保留不废除)
  { id: 'nw/qwen-72b',        ch: 'nw',    up: 'qwen/qwen-2.5-72b-instruct',            core: 0 },
  { id: 'nw/phi-4',           ch: 'nw',    up: 'microsoft/phi-4',                       core: 0 },
  { id: 'nw/llama-8b',        ch: 'nw',    up: 'meta-llama/llama-3.1-8b-instruct',      core: 0 },
  { id: 'nw/kimi-k2',         ch: 'nw',    up: 'moonshotai/kimi-k2:free',               core: 0 },
];

let POOL = DEFS.map(d => ({
  id: d.id, provider: CH[d.ch].label, kind: CH[d.ch].kind,
  endpoint: CH[d.ch].endpoint, upstreamModel: d.up, core: !!d.core,
  dead: false, lastOk: 0, lastFailWhy: '',
}));

let LAST_TEST = 0, TESTING = false;
const alive = () => POOL.filter(c => !c.dead);                                  // 未废除的全部(列表)
const confirmed = () => alive().filter(c => c.lastOk > 0).sort((a, b) => b.lastOk - a.lastOk); // 真回过话的
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

function wrapOAI(model, content) {
  return {
    id: 'chatcmpl-' + Date.now(), object: 'chat.completion', created: oaiNow(), model,
    choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}
// 限流/鉴权/占位等「非真回话」信号
const COOLING = /FREE_LIMIT|free_limit|rate.?limit|429|402|quota|too many|try again|premium|Authentication required|Just a moment|CAPTCHA/i;
const DEADTXT = /NOT_FOUND|Model not found|model not found|invalid.?model|unknown model|Selected model is required|Invalid session|missing query/i;
function extractContent(txt) {
  let d;
  try { d = JSON.parse(txt); }
  catch { const t = (txt || '').trim(); return (t && t.length < 500 && !/^OK$/i.test(t) && !COOLING.test(t) && !/<html|<!doctype/i.test(t)) ? t : ''; }
  if (typeof d === 'string') return /^OK$/i.test(d.trim()) || COOLING.test(d) ? '' : d.trim();
  if (Array.isArray(d) || !d || typeof d !== 'object') return '';
  let c = '';
  if (d.choices) c = String(d.choices[0]?.message?.content ?? d.choices[0]?.text ?? '').trim();
  if (!c && typeof d.data === 'string') c = d.data.trim();
  if (!c && d.data && typeof d.data === 'object') c = String(d.data.content ?? d.data.text ?? '').trim();
  if (!c) for (const k of ['content', 'response', 'text', 'answer', 'result', 'output', 'completion', 'message']) {
    if (typeof d[k] === 'string' && d[k].trim()) { c = d[k].trim(); break; }
  }
  if (!c && d.error) c = String(d.error.message || d.error.code || '');
  return (COOLING.test(c) || /^OK$/i.test(c)) ? '' : c;
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
      headers: { 'Content-Type': 'application/json', 'origin': 'https://netwrck.com', 'referer': 'https://netwrck.com/', 'User-Agent': 'Mozilla/5.0' },
      body: { query: toQuery(messages), context: toContext(messages), model: c.upstreamModel, max_tokens: maxTokens || 16 },
    };
  }
  return {
    url: c.endpoint,
    headers: { 'Content-Type': 'application/json' },
    body: { model: c.upstreamModel, messages, max_tokens: maxTokens || 16, stream: false },
  };
}
// 三态：dead(永久废除) / cooling(限流，保留) / ok(真回话)
function classify(status, txt, content) {
  if (status === 404 || status === 403) return 'dead';
  if (DEADTXT.test(txt || '')) return 'dead';
  if (status === 401 && /premium|Authentication required/i.test(txt || '')) return 'dead';
  if (COOLING.test(txt || '') || /^OK$/i.test((txt || '').trim())) return 'cooling';
  if (!content) return 'cooling';
  return 'ok';
}
async function callUpstream(c, messages, maxTokens) {
  const { url, headers, body } = buildRequest(c, messages, maxTokens);
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), TIMEOUT);
  try {
    const r = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body), signal: ctl.signal });
    clearTimeout(t);
    const txt = await r.text();
    const content = extractContent(txt);
    const kind = r.ok ? classify(200, txt, content) : classify(r.status, txt, content);
    if (kind !== 'ok') return { kind, why: 'HTTP ' + r.status + ' ' + String(txt).slice(0, 60) };
    let data; try { data = JSON.parse(txt); } catch { data = null; }
    const native = data && data.choices && data.choices[0];
    return { kind: 'ok', data: native ? data : wrapOAI(c.id, content), content };
  } catch (e) {
    clearTimeout(t);
    return { kind: 'cooling', why: String(e.message || e).slice(0, 50) };
  }
}

// 串行、温和体检：core 逐个真发一句话；真失效才废除；限流仅标记保留
async function selftest(force) {
  if (TESTING) return confirmed();
  if (!force && confirmed().length && Date.now() - LAST_TEST < 20000) return confirmed();
  TESTING = true;
  try {
    const targets = force ? alive() : POOL.filter(c => c.core && !c.dead);
    for (const c of targets) {
      const r = await callUpstream(c, [{ role: 'user', content: 'Reply with just the number: 7 times 8' }], 24);
      if (r.kind === 'ok') { c.lastOk = Date.now(); c.lastFailWhy = ''; }
      else if (r.kind === 'dead') { c.dead = true; console.log('[abolish] ' + c.id + ' -> ' + r.why); }
      else { c.lastFailWhy = r.why; console.log('[cooling] ' + c.id); }
      await sleep(400);
    }
    LAST_TEST = Date.now();
    console.log('[selftest] confirmed=' + confirmed().length + ' alive=' + alive().length
      + ' dead=' + POOL.filter(c => c.dead).length);
  } finally { TESTING = false; }
  return confirmed();
}

// 跨通道轮转：优先「真回过话」且不同通道，其余按需补位；杜绝越失败越猛打
function pickOrder(wantedId) {
  const conf = confirmed(), rest = alive().filter(c => !c.lastOk);
  const byProv = {};
  for (const c of conf) (byProv[c.provider] ||= []).push(c);
  const merged = []; let go = true;
  while (go) { go = false; for (const p of Object.keys(byProv)) { const a = byProv[p]; if (a.length) { merged.push(a.shift()); go = true; } } }
  for (const c of rest) if (c.core) merged.push(c);
  for (const c of rest) if (!c.core && !merged.includes(c)) merged.push(c);
  if (wantedId) { const w = merged.find(c => c.id === wantedId); if (w) { merged.splice(merged.indexOf(w), 1); merged.unshift(w); } }
  return merged;
}

const server = http.createServer(async (req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, {
      ok: true, alive: alive().length, confirmed: confirmed().length, dead: POOL.filter(c => c.dead).length,
      channels: [...new Set(alive().map(c => c.provider))], lastTest: LAST_TEST,
    });

  if (u.pathname === '/v1/selftest') {
    const ok = await selftest(true);
    return jsonRes(res, 200, {
      alive: alive().length, confirmed: ok.length,
      channels: [...new Set(alive().map(a => a.provider))],
      abolished: POOL.filter(c => c.dead).map(c => c.id),
      confirmedIds: ok.map(a => a.id),
      note: '多家公益免费API融合；真失效永久废除，免费额度限流保留(会自动恢复)',
    });
  }

  if (u.pathname === '/v1/models') {
    if (!confirmed().length && Date.now() - LAST_TEST > 20000) await selftest(false);
    const list = alive();
    return jsonRes(res, 200, {
      object: 'list',
      data: list.map(a => ({
        id: a.id, object: 'model', owned_by: a.provider,
        status: a.lastOk > 0 ? 'ready' : 'cooling',
        context_length: a.kind === 'netwrck' ? 32000 : 128000,
        capabilities: { tool_calling: true },
      })),
    });
  }

  if (u.pathname === '/v1/chat/completions' && req.method === 'POST') {
    // 鉴权完全放宽：公益网关不强制 Key，任何/无 Authorization 都放行（彻底消除 401）
    const body = await readBody(req);
    const messages = body.messages || [{ role: 'user', content: 'hi' }];
    const maxTokens = body.max_tokens || 512;
    let order = pickOrder(body.model).slice(0, 7);

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
    }
    return jsonRes(res, 429, {
      error: { message: '所有试过的公益通道此刻均限流，请稍等约 30-45 秒再试。最后错误：' + lastWhy, type: 'rate_limited', code: 'upstream_busy' },
    }, { 'Retry-After': '30' });
  }

  jsonRes(res, 200, {
    name: 'freeai-proxy', alive: alive().length, confirmed: confirmed().length,
    channels: [...new Set(POOL.map(c => c.provider))],
    endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'],
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT);
  selftest(false);   // 启动时温和体检 core，把真能回话的公益通道融合进列表
});
