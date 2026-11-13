import http from 'node:http';

const PAT = process.env.GITHUB_PAT || '';
const PORT = Number(process.env.PORT || 10000);
const TIMEOUT = Number(process.env.UPSTREAM_TIMEOUT || 25000);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type',
  'Access-Control-Max-Age': '86400',
};

// 候选上游。实测：pollinations 匿名可用的是 openai-fast 系列(gpt-oss-20b)；
// 多数名字是同一免费额度的别名/受限，402=临时限流(不废除、可 failover)，404=真不存在(废除)。
const POLL_EP = 'https://text.pollinations.ai/openai';
const POLL_MODELS = [
  'openai-fast', 'gpt-oss-20b', 'openai', 'gpt-oss', 'ovh-reasoning',
  'mistral', 'llama', 'qwen-coder', 'deepseek', 'gemini', 'phi',
];
const GHM_MODELS = ['openai/gpt-4o-mini', 'openai/gpt-4.1-mini', 'microsoft/Phi-4'];

let POOL = [];          // 全部候选（含被限流但有效的）
for (const m of POLL_MODELS) POOL.push({
  id: (m === 'gpt-oss-20b' ? 'gpt-oss-20b' : m), provider: 'pollinations',
  upstreamModel: m, endpoint: POLL_EP, bearer: '',
  dead: false, lastOk: 0, lastFailWhy: '',
});
if (PAT) for (const m of GHM_MODELS) POOL.push({
  id: m, provider: 'github-models', upstreamModel: m,
  endpoint: 'https://models.github.ai/inference/chat/completions', bearer: PAT,
  dead: false, lastOk: 0, lastFailWhy: '',
});

let AVAILABLE = [];     // 最近一次体检真回过话的（用于 /v1/models 展示）
let LAST_TEST = 0;
let TESTING = false;

function jsonRes(res, code, obj) {
  res.writeHead(code, { ...CORS, 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise(resolve => {
    let b = '';
    req.on('data', c => { b += c; });
    req.on('end', () => { try { resolve(JSON.parse(b || '{}')); } catch { resolve({}); } });
  });
}
// 404/401/403/非JSON = 永久失效(废除)；402/429/5xx/超时 = 临时(保留，仅跳过)
function isPermanent(status, txt) {
  if (status === 404 || status === 401 || status === 403) return true;
  if (/NOT_FOUND|invalid.*model|unknown model/i.test(txt || '')) return true;
  return false;
}
async function callUpstream(c, messages, maxTokens) {
  const headers = { 'Content-Type': 'application/json' };
  if (c.bearer) headers['Authorization'] = 'Bearer ' + c.bearer;
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), TIMEOUT);
  try {
    const r = await fetch(c.endpoint, {
      method: 'POST', headers,
      body: JSON.stringify({ model: c.upstreamModel, messages, max_tokens: maxTokens || 16, stream: false }),
      signal: ctl.signal,
    });
    clearTimeout(t);
    const txt = await r.text();
    if (!r.ok) return { ok: false, status: r.status, why: 'HTTP ' + r.status + ' ' + txt.slice(0, 80), permanent: isPermanent(r.status, txt) };
    let d; try { d = JSON.parse(txt); } catch { return { ok: false, status: 502, why: 'non-json: ' + txt.slice(0, 50), permanent: false }; }
    const content = d?.choices?.[0]?.message?.content ?? d?.choices?.[0]?.text;
    if (!content || !String(content).trim()) return { ok: false, status: 502, why: 'empty content', permanent: false };
    return { ok: true, data: d, content: String(content) };
  } catch (e) {
    clearTimeout(t);
    return { ok: false, status: 504, why: String(e.message || e).slice(0, 60), permanent: false };
  }
}
// 低并发体检（2 路），避免把上游打到限流
async function selftest() {
  if (TESTING) return AVAILABLE;
  TESTING = true;
  const fresh = [];
  try {
    const alive = POOL.filter(c => !c.dead);
    let idx = 0;
    const worker = async () => {
      while (idx < alive.length) {
        const c = alive[idx++];
        const r = await callUpstream(c, [{ role: 'user', content: 'Reply with exactly: OK' }], 8);
        if (r.ok) { c.lastOk = Date.now(); c.lastFailWhy = ''; fresh.push(c); }
        else {
          c.lastFailWhy = r.why;
          if (r.permanent) { c.dead = true; console.log('[abolish] ' + c.id + ' -> ' + r.why); }
          else console.log('[temp-skip] ' + c.id + ' -> ' + r.why);
        }
      }
    };
    await Promise.all([worker(), worker()]);
    // 活着(未被永久废除)的都进可调用列表；本次真回过话的排前面。
    // 临时限流(402/429/超时)不废除——否则一刷新就空列表，且额度恢复后找不回来。
    AVAILABLE = POOL.filter(c => !c.dead).sort((a, b) => b.lastOk - a.lastOk);
    LAST_TEST = Date.now();
    console.log('[selftest] listed=' + AVAILABLE.length + ' confirmedOk=' + AVAILABLE.filter(c => c.lastOk > 0).length + ' dead=' + POOL.filter(c => c.dead).length);
  } finally { TESTING = false; }
  return AVAILABLE;
}
function abolish(c) { c.dead = true; AVAILABLE = AVAILABLE.filter(a => a.id !== c.id); }

const server = http.createServer(async (req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, { ok: true, available: AVAILABLE.length, dead: POOL.filter(c => c.dead).length, lastTest: LAST_TEST });

  if (u.pathname === '/v1/selftest') {
    await selftest();
    return jsonRes(res, 200, {
      count: AVAILABLE.length,
      abolished: POOL.filter(c => c.dead).map(c => c.id),
      available: AVAILABLE.map(a => ({ id: a.id, provider: a.provider })),
    });
  }

  if (u.pathname === '/v1/models') {
    if (!AVAILABLE.length) await selftest();
    return jsonRes(res, 200, {
      object: 'list',
      data: AVAILABLE.map(a => ({
        id: a.id, object: 'model', owned_by: a.provider,
        context_length: 128000, capabilities: { tool_calling: true },
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
    // 顺序：用户指定模型优先 → 其余活着的候选（限流自动 failover）
    let order = POOL.filter(c => !c.dead);
    const wanted = order.find(c => c.id === body.model);
    if (wanted) order = [wanted, ...order.filter(c => c !== wanted)];

    let lastErr = null;
    for (const c of order) {
      const r = await callUpstream(c, messages, maxTokens);
      if (r.ok) {
        c.lastOk = Date.now(); c.lastFailWhy = '';
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
      lastErr = r;
      c.lastFailWhy = r.why;
      if (r.permanent) abolish(c);   // 真不存在/失效 → 当场废除，不再进任何调用
      // 临时限流/超时 → 保留，继续试下一个
    }
    return jsonRes(res, lastErr?.status || 503, {
      error: {
        message: '所有可用通道这次都没回话（多为免费额度临时限流，稍等会自动恢复）。最后错误：' + (lastErr?.why || 'unknown'),
        type: 'upstream_error', code: 'all_failed',
      },
    });
  }

  jsonRes(res, 200, { name: 'freeai-proxy', available: AVAILABLE.length, endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'] });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT);
  selftest();
  setInterval(selftest, 10 * 60 * 1000).unref();
});
