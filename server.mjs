import http from 'node:http';

const PORT = Number(process.env.PORT || 10000);
const TIMEOUT = Number(process.env.UPSTREAM_TIMEOUT || 12000);
const GH_TOKEN = process.env.GITHUB_PAT || '';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type,X-Upstream-Key',
  'Access-Control-Expose-Headers': 'X-FreeAI-Model,X-FreeAI-Provider',
  'Access-Control-Max-Age': '86400',
};

// 通道：免费匿名公益档 + GitHub Models（服务端 PAT 免费档）+ BYOK 聚合档
const CH = {
  poll:   { label: 'pollinations',  kind: 'oai',     endpoint: 'https://text.pollinations.ai/openai' },
  pollv1: { label: 'pollinations',  kind: 'oai',     endpoint: 'https://text.pollinations.ai/openai/chat/completions' },
  nw:     { label: 'netwrck',       kind: 'netwrck', endpoint: 'https://netwrck.com/api/chatpred_or' },
  nwfb:   { label: 'netwrck',       kind: 'netwrck', endpoint: 'https://netwrck.com/api/chatpred' },
  gh:     { label: 'github-models', kind: 'oai',     endpoint: 'https://models.github.ai/inference/chat/completions', auth: 'env' },
  or:     { label: 'openrouter',    kind: 'oai',     endpoint: 'https://openrouter.ai/api/v1/chat/completions',      auth: 'byok' },
  af:     { label: 'api-airforce',  kind: 'oai',     endpoint: 'https://api.airforce/v1/chat/completions',          auth: 'byok' },
};

// tier: flagship(旗舰/超大杯) > pro(标准) > light(轻量)
// needsKey=1 表示属于 BYOK 档，需要用户填一个免费 Key；免费档模型永远无需 Key。
const DEFS = [
  // ── GitHub Models 免费旗舰档：用服务端已有的 GitHub PAT，用户不用填任何 Key ──
  { id: 'gh/gpt-4o',          ch: 'gh', up: 'openai/gpt-4o',                       tier: 'flagship', label: 'GPT-4o',            params: 'OpenAI 旗舰 · 多模态',   ctx: 128000, core: 1 },
  { id: 'gh/o3-mini',         ch: 'gh', up: 'openai/o3-mini',                      tier: 'flagship', label: 'o3-mini',           params: 'OpenAI 推理模型',       ctx: 128000, core: 1 },
  { id: 'gh/o1',              ch: 'gh', up: 'openai/o1',                           tier: 'flagship', label: 'o1',                params: 'OpenAI 深度推理',       ctx: 200000, core: 0 },
  { id: 'gh/deepseek-r1',     ch: 'gh', up: 'deepseek/DeepSeek-R1',                tier: 'flagship', label: 'DeepSeek R1',       params: '671B MoE · 推理',       ctx: 128000, core: 1 },
  { id: 'gh/llama-3.1-405b',  ch: 'gh', up: 'meta/Llama-3.1-405B-Instruct',       tier: 'flagship', label: 'Llama 3.1 405B',    params: '405B 超大杯',           ctx: 128000, core: 1 },
  { id: 'gh/llama-3.3-70b',   ch: 'gh', up: 'meta/Llama-3.3-70B-Instruct',        tier: 'flagship', label: 'Llama 3.3 70B',     params: '70B 开源旗舰',          ctx: 128000, core: 1 },
  { id: 'gh/mistral-large',   ch: 'gh', up: 'mistralai/Mistral-large-2411',        tier: 'flagship', label: 'Mistral Large',     params: '123B 旗舰',             ctx: 128000, core: 1 },
  { id: 'gh/command-r-plus',  ch: 'gh', up: 'cohere/command-r-plus-08-2024',       tier: 'flagship', label: 'Command R+',        params: '104B · 长文 / RAG',     ctx: 128000, core: 0 },
  { id: 'gh/grok-3-mini',     ch: 'gh', up: 'xai/grok-3-mini',                     tier: 'flagship', label: 'Grok 3 mini',       params: 'xAI 推理模型',          ctx: 131072, core: 0 },
  { id: 'gh/gemma-3-27b',     ch: 'gh', up: 'google/gemma-3-27b-it',               tier: 'pro',      label: 'Gemma 3 27B',       params: '27B · 多模态',          ctx: 128000, core: 0 },
  { id: 'gh/phi-4',           ch: 'gh', up: 'microsoft/Phi-4',                     tier: 'pro',      label: 'Phi-4',             params: '14B · 数学推理',        ctx: 16384,  core: 0 },

  // ── 免费匿名公益档（无需 Key）：保留真·大模型，剔除玩具/7B 小模型 ──
  { id: 'nw/llama-70b',       ch: 'nw', up: 'meta-llama/llama-3.3-70b-instruct',       tier: 'flagship', label: 'Llama 3.3 70B',     params: '70B · 匿名预览',        ctx: 32000, core: 1 },
  { id: 'nw/qwen-72b',        ch: 'nw', up: 'qwen/qwen-2.5-72b-instruct',              tier: 'flagship', label: 'Qwen2.5 72B',       params: '72B · 匿名预览',        ctx: 32000, core: 0 },
  { id: 'nw/nemotron-70b',    ch: 'nw', up: 'nvidia/llama-3.1-nemotron-70b-instruct',  tier: 'flagship', label: 'Nemotron 70B',      params: '70B · 匿名预览',        ctx: 32000, core: 0 },
  { id: 'nw/kimi-k2',         ch: 'nw', up: 'moonshotai/kimi-k2:free',                  tier: 'flagship', label: 'Kimi K2',           params: '1T MoE · 匿名预览',     ctx: 32000, core: 0 },
  { id: 'nw/qwen3-235b',      ch: 'nw', up: 'qwen/qwen3-235b-a22b:free',                tier: 'flagship', label: 'Qwen3 235B',        params: '235B MoE · 匿名预览',   ctx: 32000, core: 0 },
  { id: 'nw/glm-4.5-air',     ch: 'nw', up: 'z-ai/glm-4.5-air:free',                    tier: 'flagship', label: 'GLM-4.5-Air',       params: 'MoE · 匿名预览',        ctx: 32000, core: 0 },
  { id: 'nw/llama-4-scout',   ch: 'nw', up: 'meta-llama/llama-4-scout:free',            tier: 'flagship', label: 'Llama 4 Scout',     params: '109B MoE · 匿名预览',   ctx: 32000, core: 0 },
  { id: 'nw2/llama-70b',      ch: 'nwfb', up: 'meta-llama/llama-3.3-70b-instruct',     tier: 'flagship', label: 'Llama 3.3 70B',     params: '70B · 备用通道',        ctx: 32000, core: 0 },
  { id: 'openai-fast',        ch: 'poll',  up: 'openai-fast',                            tier: 'pro',      label: 'GPT-OSS 20B',       params: '20B · 匿名可用',        ctx: 128000, core: 1 },
  { id: 'openai',             ch: 'poll',  up: 'openai',                                 tier: 'pro',      label: 'GPT-OSS 20B',       params: '20B · 匿名可用(别名)',  ctx: 128000, core: 1 },
  { id: 'nw/skyfall-36b',     ch: 'nw',    up: 'thedrummer/skyfall-36b-v2',             tier: 'pro',      label: 'Skyfall 36B',       params: '36B',                   ctx: 32000, core: 0 },
  { id: 'nw/olmo-32b',        ch: 'nw',    up: 'allenai/olmo-2-0325-32b-instruct',      tier: 'pro',      label: 'OLMo 2 32B',        params: '32B',                   ctx: 32000, core: 0 },
  { id: 'nw/phi-4',           ch: 'nw',    up: 'microsoft/phi-4',                        tier: 'pro',      label: 'Phi-4',             params: '14B',                   ctx: 32000, core: 0 },
  { id: 'deepseek',           ch: 'poll',  up: 'deepseek',                               tier: 'pro',      label: 'DeepSeek',          params: '匿名额度恢复后可用',    ctx: 128000, core: 0 },
  { id: 'deepseek-reasoning', ch: 'poll',  up: 'deepseek-reasoning',                     tier: 'pro',      label: 'DeepSeek 推理',     params: '匿名额度恢复后可用',    ctx: 128000, core: 0 },
  { id: 'gemini',             ch: 'poll',  up: 'gemini',                                 tier: 'pro',      label: 'Gemini',            params: '匿名额度恢复后可用',    ctx: 128000, core: 0 },
  { id: 'claude',             ch: 'poll',  up: 'claude',                                 tier: 'pro',      label: 'Claude',            params: '匿名额度恢复后可用',    ctx: 128000, core: 0 },

  // ── BYOK 旗舰档：填一个免费注册的 Key（OpenRouter / api.airforce，均无需绑卡）即解锁 ──
  { id: 'or/claude-sonnet-4.5', ch: 'or', up: 'anthropic/claude-sonnet-4.5',           tier: 'flagship', label: 'Claude Sonnet 4.5', params: 'Anthropic 旗舰',        ctx: 200000, needsKey: 1 },
  { id: 'or/gpt-5',             ch: 'or', up: 'openai/gpt-5',                           tier: 'flagship', label: 'GPT-5',             params: 'OpenAI 旗舰',          ctx: 128000, needsKey: 1 },
  { id: 'or/gemini-2.5-pro',    ch: 'or', up: 'google/gemini-2.5-pro',                  tier: 'flagship', label: 'Gemini 2.5 Pro',    params: 'Google 旗舰',          ctx: 1000000,needsKey: 1 },
  { id: 'or/deepseek-r1',       ch: 'or', up: 'deepseek/deepseek-r1',                   tier: 'flagship', label: 'DeepSeek R1',       params: '671B MoE',             ctx: 128000, needsKey: 1 },
  { id: 'or/llama-3.1-405b',    ch: 'or', up: 'meta-llama/llama-3.1-405b-instruct',    tier: 'flagship', label: 'Llama 3.1 405B',    params: '405B 超大杯',          ctx: 128000, needsKey: 1 },
  { id: 'or/nemotron-550b',     ch: 'or', up: 'nvidia/nemotron-3-ultra-550b-a55b:free',tier: 'flagship', label: 'Nemotron 3 Ultra',  params: '550B · OR 免费档',     ctx: 128000, needsKey: 1 },
  { id: 'af/gpt-5.4-pro',       ch: 'af', up: 'gpt-5.4-pro',                            tier: 'flagship', label: 'GPT-5.4 Pro',       params: 'OpenAI 旗舰',          ctx: 128000, needsKey: 1 },
  { id: 'af/claude-sonnet-4',   ch: 'af', up: 'claude-sonnet-4-20250514',               tier: 'flagship', label: 'Claude Sonnet 4',   params: 'Anthropic 旗舰',       ctx: 8192,   needsKey: 1 },
  { id: 'af/deepseek-reasoner', ch: 'af', up: 'deepseek-reasoner',                      tier: 'flagship', label: 'DeepSeek Reasoner', params: '推理模型',              ctx: 8192,   needsKey: 1 },
  { id: 'af/glm-5.3',           ch: 'af', up: 'glm-5.3',                                tier: 'flagship', label: 'GLM-5.3',           params: '智谱旗舰 · 200K',      ctx: 200000, needsKey: 1 },
  { id: 'af/mistral-large',     ch: 'af', up: 'mistral-large-2512',                     tier: 'flagship', label: 'Mistral Large 2512',params: '123B · AF 免费档',     ctx: 4096,   needsKey: 1 },
  { id: 'af/kimi-k2.7-code',    ch: 'af', up: 'kimi-k2.7-code',                         tier: 'flagship', label: 'Kimi K2.7 Code',    params: 'MoE · AF 免费档',      ctx: 4096,   needsKey: 1 },
];

const TIER = { flagship: 0, pro: 1, light: 2 };
const POOL = DEFS.map(d => ({
  id: d.id, ch: d.ch, provider: CH[d.ch].label, kind: CH[d.ch].kind,
  endpoint: CH[d.ch].endpoint, auth: CH[d.ch].auth || '', upstreamModel: d.up,
  tier: d.tier, label: d.label, params: d.params, ctx: d.ctx,
  core: !!d.core, needsKey: !!d.needsKey,
  dead: false, lastOk: 0, lastFailWhy: '',
}));

const channelOff = {};            // 通道级熔断：占位/不可达时整批跳过，不逐个超时
let LAST_TEST = 0, TESTING = false;
const alive = () => POOL.filter(c => !c.dead);
const confirmed = () => alive().filter(c => c.lastOk > 0).sort((a, b) => b.lastOk - a.lastOk);
const oaiNow = () => Math.floor(Date.now() / 1000);
const byTier = (a, b) => (TIER[a.tier] - TIER[b.tier]) || ((b.lastOk > 0 ? 1 : 0) - (a.lastOk > 0 ? 1 : 0));

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
function buildRequest(c, messages, maxTokens, byokKey) {
  if (c.kind === 'netwrck') {
    return {
      url: c.endpoint,
      headers: { 'Content-Type': 'application/json', 'origin': 'https://netwrck.com', 'referer': 'https://netwrck.com/', 'User-Agent': 'Mozilla/5.0' },
      body: { query: toQuery(messages), context: toContext(messages), model: c.upstreamModel, max_tokens: maxTokens || 16 },
    };
  }
  const headers = { 'Content-Type': 'application/json', 'User-Agent': 'freeai-proxy/2.0' };
  if (c.auth === 'env') {
    headers['Authorization'] = 'Bearer ' + GH_TOKEN;
    headers['Accept'] = 'application/vnd.github+json';
    headers['X-GitHub-Api-Version'] = '2022-11-28';
  } else if (c.auth === 'byok') {
    headers['Authorization'] = 'Bearer ' + (byokKey || '');
  }
  return {
    url: c.endpoint, headers,
    body: { model: c.upstreamModel, messages, max_tokens: maxTokens || 16, stream: false },
  };
}
function classify(status, txt, content) {
  if (status === 404 || status === 403) return 'dead';
  if (DEADTXT.test(txt || '')) return 'dead';
  if (status === 401 && /premium|Authentication required/i.test(txt || '')) return 'dead';
  if (status >= 400) return 'cooling';   // 任何 4xx/5xx 的 JSON 错误体都绝不算「真回话」
  if (COOLING.test(txt || '') || /^OK$/i.test((txt || '').trim())) return 'cooling';
  if (!content) return 'cooling';
  return 'ok';
}
async function callUpstream(c, messages, maxTokens, byokKey, timeout) {
  if (c.needsKey && !byokKey) return { kind: 'needs_key', why: '需要免费 Key' };
  if (channelOff[c.ch]) {
    if (Date.now() - channelOff[c.ch] < 300000) return { kind: 'cooling', why: '通道暂不可用：' + c.ch };
    delete channelOff[c.ch];   // 5 分钟后自动重试恢复
  }
  const { url, headers, body } = buildRequest(c, messages, maxTokens, byokKey);
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), timeout || TIMEOUT);
  try {
    const r = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body), signal: ctl.signal });
    clearTimeout(t);
    const txt = await r.text();
    const content = extractContent(txt);
    const placeholder = /^OK$/i.test((txt || '').trim());   // 上游被代理劫持成假 OK
    const kind = r.ok ? classify(200, txt, content) : classify(r.status, txt, content);
    if (kind !== 'ok') return { kind, placeholder, why: 'HTTP ' + r.status + ' ' + String(txt).slice(0, 70) };
    let data; try { data = JSON.parse(txt); } catch { data = null; }
    const native = data && data.choices && data.choices[0];
    return { kind: 'ok', placeholder, data: native ? data : wrapOAI(c.id, content), content };
  } catch (e) {
    clearTimeout(t);
    return { kind: 'cooling', placeholder: false, why: String(e.message || e).slice(0, 60) };
  }
}
function applyResult(c, r) {
  if (r.kind === 'ok') { c.lastOk = Date.now(); c.lastFailWhy = ''; }
  else if (r.kind === 'dead') { c.dead = true; console.log('[abolish] ' + c.id + ' -> ' + r.why); }
  else { c.lastFailWhy = r.why; }
}

// 温和体检：按通道先探首个；若该通道被假占位/网络不可达，整批熔断，避免逐个超时卡死
async function selftest(force) {
  if (TESTING) return confirmed();
  if (!force && confirmed().length && Date.now() - LAST_TEST < 20000) return confirmed();
  TESTING = true;
  try {
    if (force) for (const k of Object.keys(channelOff)) delete channelOff[k];
    const targets = (force ? alive() : POOL.filter(c => c.core && !c.dead)).filter(c => !c.needsKey && !c.dead);
    const byCh = {};
    for (const c of targets) (byCh[c.ch] ||= []).push(c);
    for (const [ch, list] of Object.entries(byCh)) {
      const probe = list[0];
      const r = await callUpstream(probe, [{ role: 'user', content: 'Reply with just the number: 7 times 8' }], 24, '', 9000);
      const netBad = /fetch failed|ENOTFOUND|EAI_AGAIN|aborted|network/i.test(r.why || '');
      if (r.placeholder || (netBad && r.kind !== 'ok')) {
        channelOff[ch] = Date.now();
        for (const c of list) c.lastFailWhy = r.placeholder ? '上游返回假 OK 占位' : r.why;
        console.log('[channel-off] ' + ch + ' (' + list.length + ' models)');
        continue;
      }
      delete channelOff[ch];
      applyResult(probe, r);
      await sleep(300);
      for (const c of list.slice(1)) {
        const rr = await callUpstream(c, [{ role: 'user', content: 'Reply with just: 56' }], 24, '', 9000);
        applyResult(c, rr);
        await sleep(300);
      }
    }
    LAST_TEST = Date.now();
    console.log('[selftest] confirmed=' + confirmed().length + ' alive=' + alive().length
      + ' dead=' + POOL.filter(c => c.dead).length + ' off=' + Object.keys(channelOff).join(','));
  } finally { TESTING = false; }
  return confirmed();
}

// 旗舰优先：同级别里「真回过话」的排前面；BYOK 无 Key 自动跳过；通道熔断自动跳过
function pickOrder(wantedId, byokKey) {
  const usable = alive().filter(c => !channelOff[c.ch] && (!c.needsKey || byokKey)).sort(byTier);
  let order = usable.slice(0, 7);
  if (wantedId) {
    const w = usable.find(c => c.id === wantedId);
    if (w) order = [w, ...order.filter(c => c !== w)].slice(0, 7);
  }
  return order;
}
function modelView(c) {
  return {
    id: c.id, object: 'model', owned_by: c.provider,
    tier: c.tier, display_name: c.label, params: c.params,
    status: c.needsKey ? 'needs_key' : (c.lastOk > 0 ? 'ready' : 'cooling'),
    needs_key: c.needsKey, upstream: c.upstreamModel,
    context_length: c.ctx, capabilities: { tool_calling: true },
  };
}

const server = http.createServer(async (req, res) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, {
      ok: true, alive: alive().length, confirmed: confirmed().length,
      flagship: alive().filter(c => c.tier === 'flagship').length,
      byok: alive().filter(c => c.needsKey).length,
      dead: POOL.filter(c => c.dead).length,
      channels: [...new Set(alive().map(c => c.provider))],
      channelsOff: Object.keys(channelOff), githubToken: !!GH_TOKEN, lastTest: LAST_TEST,
    });

  if (u.pathname === '/v1/selftest') {
    const ok = await selftest(true);
    return jsonRes(res, 200, {
      alive: alive().length, confirmed: ok.length,
      flagship: alive().filter(c => c.tier === 'flagship').length,
      channels: [...new Set(alive().map(a => a.provider))],
      channelsOff: Object.keys(channelOff),
      abolished: POOL.filter(c => c.dead).map(c => c.id),
      confirmedIds: ok.map(a => a.id),
      note: '免费匿名档 + GitHub Models 免费旗舰档(服务端 PAT) + BYOK 旗舰档(需免费 Key)；真失效永久废除，限流保留',
    });
  }

  if (u.pathname === '/v1/models') {
    if (!confirmed().length && Date.now() - LAST_TEST > 20000) await selftest(false);
    return jsonRes(res, 200, { object: 'list', data: alive().sort(byTier).map(modelView) });
  }

  if (u.pathname === '/v1/chat/completions' && req.method === 'POST') {
    // 免费档永远不校验 Key；BYOK 档用 X-Upstream-Key 透传，绝不静默降级到小模型代答
    const byokKey = String(req.headers['x-upstream-key'] || '').trim();
    const body = await readBody(req);
    const messages = body.messages || [{ role: 'user', content: 'hi' }];
    const maxTokens = body.max_tokens || 512;
    const wanted = POOL.find(c => c.id === body.model);
    if (wanted && wanted.needsKey && !byokKey && !wanted.dead) {
      return jsonRes(res, 402, {
        error: {
          message: '「' + wanted.label + '」属于高级 BYOK 档：请在控制台填入一个免费 Key（OpenRouter 或 api.airforce 免费注册，无需绑卡），请求头带 X-Upstream-Key 即可。不带 Key 的免费模型现在就能直接调用。',
          type: 'needs_key', code: 'upstream_key_required', model: wanted.id,
        },
      });
    }

    const order = pickOrder(body.model, byokKey || undefined);
    let lastWhy = 'unknown';
    for (const c of order) {
      const r = await callUpstream(c, messages, maxTokens, byokKey || '', TIMEOUT);
      if (r.kind === 'ok') {
        c.lastOk = Date.now(); c.lastFailWhy = '';
        const extra = { 'X-FreeAI-Model': c.id, 'X-FreeAI-Provider': c.provider };
        if (body.stream) {
          res.writeHead(200, { ...CORS, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', ...extra });
          const base = { id: 'chatcmpl-' + Date.now(), object: 'chat.completion.chunk', created: oaiNow(), model: body.model || c.id };
          res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: { role: 'assistant', content: r.content }, finish_reason: null }] }) + '\n\n');
          res.write('data: ' + JSON.stringify({ ...base, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] }) + '\n\n');
          res.write('data: [DONE]\n\n'); res.end(); return;
        }
        return jsonRes(res, 200, r.data, extra);
      }
      lastWhy = r.why; c.lastFailWhy = r.why;
      if (r.kind === 'dead') { c.dead = true; console.log('[abolish-live] ' + c.id); }
    }
    return jsonRes(res, 429, {
      error: { message: '所有可用通道此刻均限流，请稍等约 30-45 秒再试。最后错误：' + lastWhy, type: 'rate_limited', code: 'upstream_busy' },
    }, { 'Retry-After': '30' });
  }

  jsonRes(res, 200, {
    name: 'freeai-proxy', version: '2.0', alive: alive().length, confirmed: confirmed().length,
    flagship: alive().filter(c => c.tier === 'flagship').length,
    channels: [...new Set(POOL.map(c => c.provider))],
    endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'],
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT + ' (githubToken=' + !!GH_TOKEN + ')');
  selftest(false);
});
