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

// 免费上游：Pollinations 匿名通道（实测能回中文）。
// 402/429/超时/"OK"占位 = 临时限流(不废除，会自己恢复)；404/401/403/NOT_FOUND = 永久失效(废除)。
const POLL_EP = 'https://text.pollinations.ai/openai';
const POLL_MODELS = [
  'openai-fast', 'gpt-oss-20b', 'openai', 'gpt-oss', 'ovh-reasoning',
  'mistral', 'llama', 'qwen-coder', 'deepseek', 'gemini', 'phi',
];

let POOL = [];
for (const m of POLL_MODELS) POOL.push({
  id: m, provider: 'pollinations', upstreamModel: m, endpoint: POLL_EP, bearer: '',
  dead: false, lastOk: 0, lastFailWhy: '',
});
if (PAT) for (const m of ['openai/gpt-4o-mini', 'openai/gpt-4.1-mini']) POOL.push({
  id: m, provider: 'github-models', upstreamModel: m,
  endpoint: 'https://models.github.ai/inference/chat/completions', bearer: PAT,
  dead: false, lastOk: 0, lastFailWhy: '',
});

let LAST_TEST = 0;
let TESTING = false;
// 只列"真回过话"且"未被永久废除"的 —— 从没回过话的、限流占位的都不进调用列表
const confirmed = () => POOL.filter(c => !c.dead && c.lastOk > 0).sort((a, b) => b.lastOk - a.lastOk);

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
function isPermanent(status, txt) {
  if (status === 404 || status === 401 || status === 403) return true;
  if (/NOT_FOUND|invalid.?model|unknown model/i.test(txt || '')) return true;
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
    if (!r.ok) return { ok: false, status: r.status, why: 'HTTP ' + r.status + ' ' + txt.slice(0, 70), permanent: isPermanent(r.status, txt) };
    let d; try { d = JSON.parse(txt); } catch { return { ok: false, status: 502, why: 'non-json: ' + txt.slice(0, 40), permanent: false }; }
    const content = d?.choices?.[0]?.message?.content ?? d?.choices?.[0]?.text;
    if (!content || !String(content).trim()) return { ok: false, status: 502, why: 'empty content', permanent: false };
    return { ok: true, data: d, content: String(content) };
  } catch (e) {
    clearTimeout(t);
    return { ok: false, status: 504, why: String(e.message || e).slice(0, 50), permanent: false };
  }
}
// 低并发体检；一旦连续吃到限流，快速跳过剩余候选，避免把免费额度打爆
async function selftest() {
  if (TESTING) return confirmed();
  TESTING = true;
  try {
    const alive = POOL.filter(c => !c.dead);
    let idx = 0, limitedStreak = 0;
    const worker = async () => {
      while (idx < alive.length && limitedStreak < 3) {
        const c = alive[idx++];
        const r = await callUpstream(c, [{ role: 'user', content: 'Reply with exactly: OK' }], 8);
        if (r.ok) { c.lastOk = Date.now(); c.lastFailWhy = ''; limitedStreak = 0; }
        else {
          c.lastFailWhy = r.why;
          if (r.permanent) { c.dead = true; console.log('[abolish] ' + c.id + ' -> ' + r.why); }
          else { limitedStreak++; console.log('[temp] ' + c.id + ' -> ' + r.why); }
        }
      }
    };
    await Promise.all([worker(), worker()]);
    LAST_TEST = Date.now();
    const ok = confirmed();
    console.log('[selftest] confirmed=' + ok.length + ' dead=' + POOL.filter(c => c.dead).length);
  } finally { TESTING = false; }
  return confirmed();
}

const server = http.createServer(async (req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, { ok: true, confirmed: confirmed().length, dead: POOL.filter(c => c.dead).length, lastTest: LAST_TEST });

  if (u.pathname === '/v1/selftest') {
    const ok = await selftest();
    return jsonRes(res, 200, {
      count: ok.length,
      abolished: POOL.filter(c => c.dead).map(c => c.id),
      available: ok.map(a => ({ id: a.id, provider: a.provider })),
      note: '仅列出本次真回过话的模型；临时限流的保留在池内、额度恢复后自动回来',
    });
  }

  if (u.pathname === '/v1/models') {
    let ok = confirmed();
    if (!ok.length) ok = await selftest();
    return jsonRes(res, 200, {
      object: 'list',
      data: ok.map(a => ({
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
    const conf = confirmed();
    // 顺序：用户指定 → 已确认可用的(按最近成功) → 其余活着的兜底(限流恢复能救回来)
    let order = [...conf, ...POOL.filter(c => !c.dead && c.lastOk === 0)];
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
      lastErr = r; c.lastFailWhy = r.why;
      if (r.permanent) { c.dead = true; console.log('[abolish-live] ' + c.id + ' -> ' + r.why); }
    }
    return jsonRes(res, lastErr?.status || 503, {
      error: {
        message: '所有通道这次都没回话，多为免费额度临时限流，稍等 1-2 分钟会自动恢复。最后错误：' + (lastErr?.why || 'unknown'),
        type: 'upstream_error', code: 'all_failed',
      },
    });
  }

  jsonRes(res, 200, { name: 'freeai-proxy', confirmed: confirmed().length, endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'] });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT);
  selftest();
  setInterval(selftest, 10 * 60 * 1000).unref();
});
