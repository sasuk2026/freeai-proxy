import http from 'node:http';

const PAT = process.env.GITHUB_PAT || '';
const PORT = Number(process.env.PORT || 10000);
const TIMEOUT = Number(process.env.UPSTREAM_TIMEOUT || 20000);

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type',
  'Access-Control-Max-Age': '86400',
};

// 免费上游候选：GitHub Models(用你的 PAT) + Pollinations(免 Key)
const GHM = [
  'openai/gpt-4o-mini','openai/gpt-4o','openai/gpt-4.1-mini','openai/gpt-4.1',
  'openai/o3-mini','openai/o4-mini','meta/Llama-3.3-70B-Instruct',
  'mistral-ai/Mistral-small','deepseek/DeepSeek-R1','microsoft/Phi-4',
  'google/gemma-3-27b-it','xai/grok-3-mini',
];
const POL = ['openai','openai-large','mistral','llama','qwen-coder','deepseek','gemini'];

const CANDIDATES = [];
for (const m of GHM) CANDIDATES.push({
  id: m, provider: 'github-models', upstreamModel: m,
  endpoint: 'https://models.github.ai/inference/chat/completions', bearer: PAT,
});
for (const m of POL) CANDIDATES.push({
  id: m, provider: 'pollinations', upstreamModel: m,
  endpoint: 'https://text.pollinations.ai/openai', bearer: '',
});

let AVAILABLE = [];
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
async function mapLimit(items, limit, fn) {
  const out = []; let i = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) { const idx = i++; out[idx] = await fn(items[idx]); }
  }));
  return out;
}
async function testOne(c) {
  const headers = { 'Content-Type': 'application/json' };
  if (c.bearer) headers['Authorization'] = 'Bearer ' + c.bearer;
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), TIMEOUT);
  try {
    const r = await fetch(c.endpoint, {
      method: 'POST', headers,
      body: JSON.stringify({
        model: c.upstreamModel,
        messages: [{ role: 'user', content: 'Reply with exactly: OK' }],
        max_tokens: 16, stream: false,
      }),
      signal: ctl.signal,
    });
    clearTimeout(t);
    const txt = await r.text();
    if (!r.ok) return { id: c.id, ok: false, why: 'HTTP ' + r.status + ' ' + txt.slice(0, 70) };
    let d; try { d = JSON.parse(txt); } catch { return { id: c.id, ok: false, why: 'non-json: ' + txt.slice(0, 50) }; }
    const content = d?.choices?.[0]?.message?.content ?? d?.choices?.[0]?.text;
    if (!content || !String(content).trim()) return { id: c.id, ok: false, why: 'empty content' };
    return { id: c.id, ok: true, c, sample: String(content).trim().slice(0, 30) };
  } catch (e) {
    clearTimeout(t);
    return { id: c.id, ok: false, why: String(e.message || e).slice(0, 70) };
  }
}
async function selftest() {
  if (TESTING) return AVAILABLE;
  TESTING = true;
  try {
    const rs = await mapLimit(CANDIDATES, 5, testOne);
    AVAILABLE = rs.filter(x => x.ok).map(x => ({
      id: x.id, provider: x.c.provider, upstreamModel: x.c.upstreamModel,
      endpoint: x.c.endpoint, bearer: x.c.bearer, sample: x.sample,
    }));
    LAST_TEST = Date.now();
    console.log('[selftest] available ' + AVAILABLE.length + '/' + CANDIDATES.length, AVAILABLE.map(a => a.id).join(','));
    if (!AVAILABLE.length) console.log('[selftest] fails', rs.filter(x => !x.ok).slice(0, 8).map(x => x.id + ':' + x.why));
  } finally { TESTING = false; }
  return AVAILABLE;
}
function drop(id) { AVAILABLE = AVAILABLE.filter(a => a.id !== id); }

const server = http.createServer(async (req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, { ok: true, available: AVAILABLE.length, lastTest: LAST_TEST });

  if (u.pathname === '/v1/selftest') {
    await selftest();
    return jsonRes(res, 200, {
      count: AVAILABLE.length,
      available: AVAILABLE.map(a => ({ id: a.id, provider: a.provider, sample: a.sample })),
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
    let item = AVAILABLE.find(a => a.id === body.model);
    if (!item) { await selftest(); item = AVAILABLE.find(a => a.id === body.model) || AVAILABLE[0]; }
    if (!item) return jsonRes(res, 503, {
      error: { message: '当前没有可用上游（自动测活全部未通过），点 /v1/selftest 查看详情', type: 'server_error', code: 'no_upstream' },
    });

    // 调用层鉴权：只放行 sk- 开头的 Key（PAT 只在服务端，不下发前端）
    const auth = req.headers.authorization || '';
    if (!/^Bearer\s+sk-\S{6,}/i.test(auth))
      return jsonRes(res, 401, { error: { message: '需要 API Key：Authorization: Bearer sk-...', code: 'auth_required' } });

    const headers = { 'Content-Type': 'application/json' };
    if (item.bearer) headers['Authorization'] = 'Bearer ' + item.bearer;
    const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), TIMEOUT + 10000);
    try {
      const up = await fetch(item.endpoint, {
        method: 'POST', headers,
        body: JSON.stringify({ ...body, model: item.upstreamModel, stream: false }),
        signal: ctl.signal,
      });
      clearTimeout(t);
      const txt = await up.text();
      if (!up.ok) { drop(item.id); return jsonRes(res, up.status, { error: { message: '上游不可用，已把该模型从可用列表废除：' + txt.slice(0, 90), type: 'upstream_error' } }); }
      let d; try { d = JSON.parse(txt); } catch { drop(item.id); return jsonRes(res, 502, { error: { message: '上游返回非 JSON，已废除该模型', type: 'upstream_error' } }); }
      const content = d?.choices?.[0]?.message?.content ?? d?.choices?.[0]?.text;
      if (!content || !String(content).trim()) { drop(item.id); return jsonRes(res, 502, { error: { message: '上游无回话内容，已废除该模型', type: 'upstream_error' } }); }

      if (body.stream) {
        res.writeHead(200, { ...CORS, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
        const base = { id: 'chatcmpl-' + Date.now(), object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model };
        res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: { role: 'assistant', content: String(content) }, finish_reason: null }] }) + '\n\n');
        res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] }) + '\n\n');
        res.write('data: [DONE]\n\n'); res.end(); return;
      }
      return jsonRes(res, 200, d);
    } catch (e) {
      clearTimeout(t); drop(item.id);
      return jsonRes(res, 502, { error: { message: '上游调用失败，已废除该模型：' + String(e.message || e), type: 'upstream_error' } });
    }
  }

  jsonRes(res, 200, { name: 'freeai-proxy', available: AVAILABLE.length, endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'] });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT);
  selftest();
  setInterval(selftest, 10 * 60 * 1000).unref();
});
