import http from 'node:http';
import crypto from 'node:crypto';

const PORT = Number(process.env.PORT || 10000);
const TIMEOUT = Number(process.env.UPSTREAM_TIMEOUT || 12000);      // 体检/单槽位硬超时
const GH_TOKEN = process.env.GITHUB_PAT || '';
// ── 低延迟竞速：一次请求同时打多家不同渠道，谁先真回话就用谁；绝不串行挨个等超时 ──
const RACE_TIMEOUT = Number(process.env.RACE_TIMEOUT || 7500);       // 竞速单槽位最多 7.5 秒（Cloudflare WS 常需 7~10s）
const RACE_N = 4;                                                     // 每批 4 个不同渠道并发
const RACE_BATCHES = 4;                                               // 4 批兜底，降低「全渠道限流」概率
const REQ_DEADLINE_MS = Number(process.env.REQ_DEADLINE_MS || 22000); // 单次请求硬上限 22 秒（流式首包秒发，不体感为转圈）

// 放宽 CORS：第三方套壳 App 常带自定义头（X-Title / HTTP-Referer / x-stainless-* 等），
// 预检被拒会直接 fetch reject，客户端把错误对象 String() 就显示成 [object Object]。
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS,HEAD',
  'Access-Control-Allow-Headers': '*',
  'Access-Control-Expose-Headers': '*',
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
  cf:     { label: 'cloudflare',    kind: 'ws',      endpoint: 'wss://playground.ai.cloudflare.com' },
  llm7:   { label: 'llm7',          kind: 'oai',     endpoint: 'https://api.llm7.io/v1/chat/completions' },
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

  // ── Cloudflare Workers AI：51 个零凭证模型，WebSocket 独立会话，每请求新建 agent 天然摊额度 ──
  { id: 'cf/gpt-oss-120b',    ch: 'cf', up: '@cf/openai/gpt-oss-120b',                        tier: 'flagship', label: 'GPT-OSS 120B',       params: '120B · CF 免Key',   ctx: 128000, core: 1 },
  { id: 'cf/llama-3.3-70b',   ch: 'cf', up: '@cf/meta/llama-3.3-70b-instruct-fp8-fast',      tier: 'flagship', label: 'Llama 3.3 70B',      params: '70B · CF 免Key',    ctx: 128000, core: 1 },
  { id: 'cf/nemotron-3-120b', ch: 'cf', up: '@cf/nvidia/nemotron-3-120b-a12b',               tier: 'flagship', label: 'Nemotron 3 120B',    params: '120B MoE · CF 免Key',ctx: 128000, core: 0 },
  { id: 'cf/kimi-k2.7-code',  ch: 'cf', up: '@cf/moonshotai/kimi-k2.7-code',                 tier: 'flagship', label: 'Kimi K2.7 Code',     params: 'MoE · CF 免Key',    ctx: 128000, core: 0 },
  { id: 'cf/kimi-k2.6',       ch: 'cf', up: '@cf/moonshotai/kimi-k2.6',                      tier: 'flagship', label: 'Kimi K2.6',          params: 'MoE · CF 免Key',    ctx: 128000, core: 0 },
  { id: 'cf/glm-5.2',         ch: 'cf', up: '@cf/zai-org/glm-5.2',                           tier: 'flagship', label: 'GLM-5.2',            params: '智谱旗舰 · CF 免Key',ctx: 200000, core: 0 },
  { id: 'cf/glm-4.7-flash',   ch: 'cf', up: '@cf/zai-org/glm-4.7-flash',                     tier: 'flagship', label: 'GLM-4.7 Flash',      params: 'MoE · CF 免Key',    ctx: 128000, core: 0 },
  { id: 'cf/llama-4-scout',   ch: 'cf', up: '@cf/meta/llama-4-scout-17b-16e-instruct',       tier: 'flagship', label: 'Llama 4 Scout',      params: '109B MoE · CF 免Key',ctx: 128000, core: 0 },
  { id: 'cf/qwen3-30b',       ch: 'cf', up: '@cf/qwen/qwen3-30b-a3b-fp8',                    tier: 'flagship', label: 'Qwen3 30B A3B',      params: '30B MoE · CF 免Key',ctx: 128000, core: 0 },
  { id: 'cf/qwen2.5-coder32', ch: 'cf', up: '@cf/qwen/qwen2.5-coder-32b-instruct',           tier: 'flagship', label: 'Qwen2.5 Coder 32B',  params: '32B 代码 · CF 免Key',ctx: 32000,  core: 0 },
  { id: 'cf/qwq-32b',         ch: 'cf', up: '@cf/qwen/qwq-32b',                              tier: 'flagship', label: 'QwQ 32B',            params: '32B 推理 · CF 免Key',ctx: 32000,  core: 0 },
  { id: 'cf/deepseek-r1-32b', ch: 'cf', up: '@cf/deepseek-ai/deepseek-r1-distill-qwen-32b',  tier: 'flagship', label: 'DeepSeek R1 32B',    params: '32B 推理 · CF 免Key',ctx: 32000,  core: 0 },
  { id: 'cf/mistral-small24', ch: 'cf', up: '@cf/mistralai/mistral-small-3.1-24b-instruct',  tier: 'pro',      label: 'Mistral Small 24B',  params: '24B · CF 免Key',    ctx: 32000,  core: 0 },
  { id: 'cf/gemma-4-26b',     ch: 'cf', up: '@cf/google/gemma-4-26b-a4b-it',                 tier: 'pro',      label: 'Gemma 4 26B',        params: '26B MoE · CF 免Key',ctx: 32000,  core: 0 },
  { id: 'cf/gpt-oss-20b',     ch: 'cf', up: '@cf/openai/gpt-oss-20b',                        tier: 'pro',      label: 'GPT-OSS 20B',        params: '20B · CF 免Key',    ctx: 128000, core: 0 },
  { id: 'cf/gemma-3-12b',     ch: 'cf', up: '@cf/google/gemma-3-12b-it',                     tier: 'pro',      label: 'Gemma 3 12B',        params: '12B · CF 免Key',    ctx: 32000,  core: 0 },

  // ── LLM7.io：turbo 档零凭证，OpenAI 兼容 ──
  { id: 'llm7/deepseek-v4',   ch: 'llm7', up: 'DeepSeek-V4-Flash-0731',                     tier: 'flagship', label: 'DeepSeek V4 Flash',  params: 'LLM7 免Key',        ctx: 64000, core: 1 },
  { id: 'llm7/glm-5.3-flash', ch: 'llm7', up: 'GLM-5.3-Flash',                             tier: 'flagship', label: 'GLM-5.3 Flash',      params: 'LLM7 免Key',        ctx: 64000, core: 0 },
  { id: 'llm7/codestral',     ch: 'llm7', up: 'codestral-latest',                           tier: 'pro',      label: 'Codestral',          params: '代码 · LLM7 免Key', ctx: 32000, core: 0 },
  { id: 'llm7/gpt-oss-20b',   ch: 'llm7', up: 'gpt-oss:20b',                               tier: 'pro',      label: 'GPT-OSS 20B',        params: 'LLM7 免Key',        ctx: 32000, core: 0 },
  { id: 'llm7/minimax-m2.7',  ch: 'llm7', up: 'minimax-m2.7',                               tier: 'flagship', label: 'MiniMax M2.7',       params: 'LLM7 免Key',        ctx: 64000, core: 0 },
  { id: 'llm7/mistral-nemo',  ch: 'llm7', up: 'mistral-Nemo-Instruct-2407',                 tier: 'pro',      label: 'Mistral Nemo',       params: 'LLM7 免Key',        ctx: 32000, core: 0 },

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

const channelOff = {};            // 通道级暂避：占位/不可达时短暂停用，到点自动复活
const CHANNEL_COOL = 30000;       // 30 秒后自动复活重新参与轮转，不做一锤子打死
let LAST_TEST = 0, TESTING = false, RR = 0;   // RR：全局轮转指针，把额度均匀摊到所有槽位
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

// 把任何值强制收敛成「纯字符串」：上游偶尔给对象/数组/多模态 content，
// 直接 String() 会变成 "[object Object]" —— 这正是套壳 App 显示 ❌[object Object] 的根因之一。
function asText(v) {
  if (v == null) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  if (Array.isArray(v)) return v.map(x => asText(x && typeof x === 'object' ? (x.text ?? x.content ?? '') : x)).filter(Boolean).join('');
  if (typeof v === 'object') {
    for (const k of ['text', 'content', 'value', 'answer', 'output', 'response', 'message']) {
      if (v[k] != null) { const s = asText(v[k]); if (s) return s; }
    }
    return '';   // 宁可空，也绝不吐出 [object Object]
  }
  return String(v);
}
function wrapOAI(model, content) {
  return {
    id: 'chatcmpl-' + Date.now(), object: 'chat.completion', created: oaiNow(), model,
    choices: [{ index: 0, message: { role: 'assistant', content: asText(content) }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}
// 标准 OpenAI 流式 chunk：delta 与 message 都放同一份字符串，
// 兼容读 delta.content 与读 message.content 的两类套壳客户端；绝不含注释行 / 裸 error 帧。
function oaiChunk(id, model, content, finish) {
  const text = asText(content);
  const choice = { index: 0, finish_reason: finish ? 'stop' : null };
  if (finish) { choice.delta = { content: '' }; choice.message = { role: 'assistant', content: '' }; }
  else {
    choice.delta = { role: 'assistant', content: text };
    choice.message = { role: 'assistant', content: text };
  }
  return { id, object: 'chat.completion.chunk', created: oaiNow(), model, choices: [choice] };
}
function sse(res, obj) { res.write('data: ' + JSON.stringify(obj) + '\n\n'); }
const COOLING = /FREE_LIMIT|free_limit|rate.?limit|429|402|quota|too many|try again|premium|Authentication required|Just a moment|CAPTCHA/i;
const DEADTXT = /NOT_FOUND|Model not found|model not found|invalid.?model|unknown model|Selected model is required|Invalid session|missing query/i;
function extractContent(txt) {
  let d;
  try { d = JSON.parse(txt); }
  catch { const t = (txt || '').trim(); return (t && t.length < 500 && !/^OK$/i.test(t) && !COOLING.test(t) && !/<html|<!doctype/i.test(t)) ? t : ''; }
  if (typeof d === 'string') return /^OK$/i.test(d.trim()) || COOLING.test(d) ? '' : d.trim();
  if (Array.isArray(d) || !d || typeof d !== 'object') return '';
  let c = '';
  if (d.choices) c = asText(d.choices[0]?.message?.content ?? d.choices[0]?.delta?.content ?? d.choices[0]?.text ?? '').trim();
  if (!c && typeof d.data === 'string') c = d.data.trim();
  if (!c && d.data && typeof d.data === 'object') c = asText(d.data.content ?? d.data.text ?? '').trim();
  if (!c) for (const k of ['content', 'response', 'text', 'answer', 'result', 'output', 'completion', 'message']) {
    if (typeof d[k] === 'string' && d[k].trim()) { c = d[k].trim(); break; }
  }
  if (!c && d.error) c = asText(d.error.message || d.error.code || '');
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
// Cloudflare Workers AI Playground：零凭证 WebSocket，每次新建独立 agent（天然摊额度）
function callCloudflare(c, messages, maxTokens, timeout, signal) {
  return new Promise(resolve => {
    const agentId = 'playground-' + Math.random().toString(36).substring(2, 15);
    const pk = crypto.randomUUID();
    const model = c.upstreamModel;
    const url = 'wss://playground.ai.cloudflare.com/agents/playground/' + agentId
      + '?_pk=' + pk + '&model=' + encodeURIComponent(model);
    let ws, done = false, out = '', idle;
    const finish = (kind, why) => {
      if (done) return; done = true;
      clearTimeout(timer); clearTimeout(idle);
      try { ws && ws.close(); } catch {}
      resolve(kind === 'ok'
        ? { kind: 'ok', placeholder: false, data: wrapOAI(c.id, out.trim()), content: out.trim() }
        : { kind, placeholder: false, why });
    };
    const timer = setTimeout(() => finish(out.trim() ? 'ok' : 'cooling', 'ws timeout'), timeout || 14000);
    try { ws = new WebSocket(url); } catch (e) { return finish('cooling', String(e)); }
    // 竞速输了就被中止：立刻关掉 WS，不继续占用免费额度
    if (signal) {
      if (signal.aborted) return finish('cooling', 'raced');
      signal.addEventListener('abort', () => finish('cooling', 'raced'), { once: true });
    }
    const send = o => { try { ws.send(JSON.stringify(o)); } catch {} };
    ws.addEventListener('open', () => {
      send({ type: 'cf_agent_stream_resume_request' });
      send({ name: agentId, agent: 'playground', type: 'cf_agent_identity' });
      send({ state: { model, temperature: 1, stream: true, system: 'You are a helpful assistant.', useExternalProvider: false, externalProvider: 'openai', generativeUI: false }, type: 'cf_agent_state' });
      send({ mcp: { prompts: [], resources: [], servers: {}, tools: [] }, type: 'cf_agent_mcp_servers' });
      send({ type: 'cf_agent_stream_resume_none' });
      setTimeout(() => {
        const cfMessages = messages.map((m, i) => ({ role: m.role, parts: [{ type: 'text', text: String(m.content) }], id: 'msg_' + i }));
        send({ id: 'req_' + Math.random().toString(36).substring(2, 10), init: { method: 'POST', body: JSON.stringify({ messages: cfMessages, trigger: 'submit-message' }) }, type: 'cf_agent_use_chat_request' });
      }, 900);
    });
    ws.addEventListener('message', ev => {
      let d; try { d = JSON.parse(String(ev.data)); } catch { return; }
      if (d.type !== 'cf_agent_use_chat_response') return;
      let b; try { b = JSON.parse(d.body || '{}'); } catch { return; }
      if (b.type === 'text-delta' && typeof b.delta === 'string') {
        out += b.delta;
        clearTimeout(idle);
        idle = setTimeout(() => finish(out.trim() ? 'ok' : 'cooling', 'ws idle done'), 1200);
      }
      if (d.done === true || b.type === 'finish-step' || b.type === 'finish' || b.type === 'text-end')
        finish(out.trim() ? 'ok' : 'cooling', 'ws done');
    });
    ws.addEventListener('error', () => finish('cooling', 'ws error'));
    ws.addEventListener('close', () => finish(out.trim() ? 'ok' : 'cooling', 'ws closed'));
  });
}

async function callUpstream(c, messages, maxTokens, byokKey, timeout, signal) {
  if (c.needsKey && !byokKey) return { kind: 'needs_key', why: '需要免费 Key' };
  if (channelOff[c.ch]) {
    if (Date.now() - channelOff[c.ch] < CHANNEL_COOL) return { kind: 'cooling', why: '通道冷却中：' + c.ch };
    delete channelOff[c.ch];   // 到点自动复活，重新进入无限轮转池
  }
  if (c.kind === 'ws') return callCloudflare(c, messages, maxTokens, timeout || TIMEOUT, signal);
  const { url, headers, body } = buildRequest(c, messages, maxTokens, byokKey);
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), timeout || TIMEOUT);
  const onAbort = () => ctl.abort();
  if (signal) {
    if (signal.aborted) ctl.abort();
    else signal.addEventListener('abort', onAbort, { once: true });
  }
  try {
    const r = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body), signal: ctl.signal });
    clearTimeout(t);
    if (signal) signal.removeEventListener('abort', onAbort);
    const txt = await r.text();
    const content = extractContent(txt);
    const placeholder = /^OK$/i.test((txt || '').trim());   // 上游被代理劫持成假 OK
    const kind = r.ok ? classify(200, txt, content) : classify(r.status, txt, content);
    if (kind !== 'ok') return { kind, placeholder, why: 'HTTP ' + r.status + ' ' + String(txt).slice(0, 70) };
    // 不再原样透传上游 JSON：上游的 content/字段形态不可控，一律重包成干净的纯字符串结构
    return { kind: 'ok', placeholder, data: wrapOAI(c.id, content), content: asText(content) };
  } catch (e) {
    clearTimeout(t);
    if (signal) signal.removeEventListener('abort', onAbort);
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

// 「无限额度池」：所有免费槽位 + BYOK模型×每个Key 展开成独立槽位。
// 每家公益上游都有限额，但把额度摊到几十个槽位上、全局轮转、限流自动跳、冷却自动复活，
// 单次请求一路重试到真回话为止 —— 用户侧感知就是「打不完」。
function buildSlots(keys) {
  const slots = [];
  for (const c of alive()) {
    // 冷却通道的槽位仍留在池里（排后面），轮到时由 callUpstream 判断到点自动复活 —— 否则永远没机会恢复
    if (c.needsKey) {
      for (const k of keys) slots.push({ c, key: k, id: c.id });
    } else slots.push({ c, key: '', id: c.id });
  }
  slots.sort((a, b) => {
    const off = (channelOff[a.c.ch] ? 1 : 0) - (channelOff[b.c.ch] ? 1 : 0);
    return off || byTier(a.c, b.c) || ((b.c.lastOk > 0 ? 1 : 0) - (a.c.lastOk > 0 ? 1 : 0));
  });
  return slots;
}
function pickOrder(wantedId, keys) {
  const slots = buildSlots(keys);
  if (!slots.length) return [];
  const n = slots.length, start = RR % n;
  const rotated = [...slots.slice(start), ...slots.slice(0, start)];
  RR = (start + 1) % n;                    // 每次请求前移一格，下次自动换下一个槽位摊额度
  let order = rotated;
  if (wantedId) {
    const w = rotated.find(s => s.id === wantedId);
    if (w) {
      // 指定模型失败时，先在同一家渠道内换模型，再跨渠道兜底，尽量不「降级」到别家小模型
      const sameCh = rotated.filter(s => s !== w && s.c.ch === w.c.ch);
      const otherCh = rotated.filter(s => s !== w && s.c.ch !== w.c.ch);
      order = [w, ...sameCh, ...otherCh];
    }
  }
  return order.slice(0, Math.min(order.length, 40));
}
// 跨渠道交错：让前 N 个槽位来自不同上游（并发竞速才有意义），也让 /v1/models 前十个
// 不再是单一渠道全家桶 —— 第三方客户端常只取前 10 个当「可用模型示例」展示。
function diversifyAll(order) {
  const groups = new Map();
  for (const s of order) {
    const k = s.c ? s.c.provider : s.provider;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(s);
  }
  const out = [];
  while (out.length < order.length) {
    let added = false;
    for (const list of groups.values()) {
      if (list.length) { out.push(list.shift()); added = true; }
    }
    if (!added) break;
  }
  return out;
}
// 竞速：一批 4 个不同渠道同时真调，谁先回话用谁，立刻中止其余上游省额度。
// 全部失败才进入下一批；任何一批成功都不再等待慢槽位。
async function raceOnce(batch, messages, maxTokens, timeout, headStartMs = 0) {
  if (!batch.length || timeout < 1200) return null;
  const ctrl = new AbortController();
  let won = false;
  const attempts = batch.map(async (slot, idx) => {
    // 点名模型先独占一小段：能自己回话就精确命中，不回话再由其他旗舰接管（不傻等）
    if (idx > 0 && headStartMs) {
      await sleep(headStartMs);
      if (ctrl.signal.aborted) throw new Error('raced');
    }
    const c = slot.c;
    const r = await callUpstream(c, messages, maxTokens, slot.key, timeout, ctrl.signal);
    if (r.kind === 'ok') {
      if (won) throw new Error('raced');       // 已被别家抢先，丢弃本次结果
      won = true;
      ctrl.abort();                            // 中止其余 fetch / WebSocket
      return { c, r };
    }
    if (!won) {
      if (r.kind === 'dead') { c.dead = true; console.log('[abolish-live] ' + c.id); }
      else c.lastFailWhy = r.why;
    }
    throw new Error(r.why || 'fail');
  });
  try { return await Promise.any(attempts); }
  catch { ctrl.abort(); return null; }
}
function modelView(c) {
  return {
    id: c.id, object: 'model', owned_by: c.provider,
    tier: c.tier, display_name: c.label, params: c.params,
    // ready=真回过话；available=在轮转池里待命中、请求时会自动轮到（不是"不能用"）；
    // 只有整条通道被熔断时才标 cooling；needs_key 需填免费 Key。真失效模型已废除、不再列入。
    status: c.needsKey ? 'needs_key' : (c.lastOk > 0 ? 'ready' : (channelOff[c.ch] ? 'cooling' : 'available')),
    needs_key: c.needsKey, upstream: c.upstreamModel,
    context_length: c.ctx, capabilities: { tool_calling: true },
  };
}

const server = http.createServer(async (req, res) => {
  // 统一回显跨域来源：套壳 App 若用 credentials 模式，通配符 Origin 会被浏览器拒绝
  const origin = req.headers.origin;
  const rawWriteHead = res.writeHead.bind(res);
  res.writeHead = function (code, headers) {
    const h = { ...(headers || {}) };
    if (origin) {
      h['Access-Control-Allow-Origin'] = origin;
      h['Access-Control-Allow-Credentials'] = 'true';
      h['Vary'] = 'Origin';
    }
    return rawWriteHead(code, h);
  };
  let u; try { u = new URL(req.url, 'http://x'); } catch { return jsonRes(res, 400, { error: 'bad url' }); }
  if (req.method === 'OPTIONS') {
    // 动态回显预检请求头，避免某些浏览器/套壳因通配符不生效而 fetch reject
    const pre = { ...CORS };
    const asked = req.headers['access-control-request-headers'];
    if (asked) pre['Access-Control-Allow-Headers'] = asked;
    res.writeHead(204, pre); res.end(); return;
  }

  if (u.pathname === '/healthz')
    return jsonRes(res, 200, {
      ok: true, alive: alive().length, confirmed: confirmed().length,
      flagship: alive().filter(c => c.tier === 'flagship').length,
      byok: alive().filter(c => c.needsKey).length,
      dead: POOL.filter(c => c.dead).length,
      channels: [...new Set(alive().map(c => c.provider))],
      channelsOff: Object.keys(channelOff), githubToken: !!GH_TOKEN, lastTest: LAST_TEST,
      poolSlots: alive().length, flagshipSlots: alive().filter(c => c.tier === 'flagship').length,
      rr: RR, channelCoolMs: CHANNEL_COOL, mode: 'unlimited-pool-fast-race',
      raceN: RACE_N, raceTimeoutMs: RACE_TIMEOUT, reqDeadlineMs: REQ_DEADLINE_MS,
      available: alive().filter(c => !c.needsKey && !c.lastOk && !channelOff[c.ch]).length,
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
    const list = diversifyAll(alive().sort(byTier));
    return jsonRes(res, 200, {
      object: 'list',
      total: list.length,
      ready: confirmed().length,
      channels: [...new Set(list.map(c => c.provider))],
      note: '全部 ' + list.length + ' 个模型都在轮转池内可调用（第三方客户端若只显示前 10 个，那是它自己的"示例"截断）。'
        + 'ready=已真回话验证；available=池内待命、请求时自动轮到并复活；needs_key=填免费 Key 解锁；cooling=该通道当前被熔断，30 秒自动恢复。真失效模型已废除不列入。',
      data: list.map(modelView),
    });
  }

  // 路径兼容：/v1/chat/completions、/chat/completions、/v1/v1/chat/completions、尾斜杠
  const chatPath = /^\/(?:v1\/)*chat\/completions?\/?$/i.test(u.pathname);
  if (chatPath && req.method === 'POST') {
    // 免费档永远不校验 Key；Key 池（多个免费 Key 用逗号/换行分隔）让 BYOK 档线性扩容
    const keys = String(req.headers['x-upstream-key'] || '').split(/[\s,;]+/).map(s => s.trim()).filter(Boolean);
    const body = await readBody(req);
    // 第三方 App 常塞超长 system prompt / 搜索结果 / 多模态 content 数组 —— 一律收敛成纯文本，
    // 否则上游拿到对象会回奇怪结构，客户端又渲染成 [object Object]。
    const messages = (Array.isArray(body.messages) && body.messages.length ? body.messages : [{ role: 'user', content: 'hi' }])
      .map(m => ({ role: String(m?.role || 'user'), content: asText(m?.content) }))
      .filter(m => m.content);
    if (!messages.length) messages.push({ role: 'user', content: 'hi' });
    const maxTokens = body.max_tokens || 512;
    // stream 可能是 true / 1 / "true"（字符串 "false" 绝不能误判成流式）
    const stream = body.stream === true || body.stream === 1 || body.stream === 'true';
    const reqModel = String(body.model || '');
    const cid = 'chatcmpl-' + Date.now();

    // 统一的「流式收尾」：只发标准 chunk + [DONE]，绝不发注释行 / 裸 error 帧。
    // 很多套壳 App 对每一行都 JSON.parse，任何非 chunk 帧都会让它 catch 出 ❌[object Object]。
    const streamFinish = (model, text, isError) => {
      sse(res, oaiChunk(cid, model, '', false));              // role 帧
      if (text) sse(res, oaiChunk(cid, model, asText(text), false));  // 内容帧
      sse(res, oaiChunk(cid, model, '', true));                // finish 帧
      res.write('data: [DONE]\n\n'); res.end();
    };
    const openStream = () => {
      res.writeHead(200, { ...CORS, 'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      if (res.flushHeaders) res.flushHeaders();
      sse(res, oaiChunk(cid, reqModel || 'freeai-proxy', '', false));  // 立刻吐合法首帧，spinner 马上停
    };

    const wanted = POOL.find(c => c.id === reqModel);
    if (wanted && wanted.needsKey && !keys.length && !wanted.dead) {
      const msg = '「' + wanted.label + '」属于高级 BYOK 档：请在控制台填入免费 Key（OpenRouter / api.airforce 免费注册，无需绑卡），请求头 X-Upstream-Key。不填 Key 的话，选 /v1/models 里不带「需 Key」的模型即可直接调用。';
      if (stream) { openStream(); return streamFinish(reqModel, msg, true); }
      return jsonRes(res, 402, { error: { message: msg, type: 'needs_key', code: 'upstream_key_required', model: wanted.id } });
    }

    // 竞速候选：先在「旗舰档」里跨渠道挑，小模型只在旗舰全挂时才兜底
    const raw = pickOrder(reqModel, keys);
    const offLast = arr => arr.slice().sort((a, b) => (channelOff[a.c.ch] ? 1 : 0) - (channelOff[b.c.ch] ? 1 : 0));
    const flag = offLast(raw.filter(s => s.c.tier === 'flagship'));
    const rest = offLast(raw.filter(s => s.c.tier !== 'flagship'));
    const wantedSlot = reqModel ? raw[0] : null;
    const order = (wantedSlot && wantedSlot.c.tier !== 'flagship')
      ? [wantedSlot, ...flag, ...rest.filter(s => s !== wantedSlot)]
      : [...flag, ...rest];
    const mixed = diversifyAll(order);
    const raced = [
      ...mixed.filter(s => s.c.tier === 'flagship' || s === wantedSlot),
      ...mixed.filter(s => s.c.tier !== 'flagship' && s !== wantedSlot),
    ];
    if (stream) openStream();
    const deadline = Date.now() + REQ_DEADLINE_MS;
    let lastWhy = 'unknown', winner = null;
    const maxSlots = RACE_N * RACE_BATCHES;
    for (let i = 0; i < Math.min(raced.length, maxSlots) && !winner && Date.now() < deadline; i += RACE_N) {
      const batch = raced.slice(i, i + RACE_N);
      // 只有「点名模型所在通道健康且最近真回过话」才给独占窗口；
      // 已限流的通道（如偶发 429 的 llm7）不再白白等 0.9 秒，直接全池并发
      const giveHead = i === 0 && reqModel && wantedSlot
        && !channelOff[wantedSlot.c.ch] && wantedSlot.c.lastOk > 0;
      winner = await raceOnce(batch, messages, maxTokens, Math.min(RACE_TIMEOUT, deadline - Date.now()),
        giveHead ? 900 : 0);
      if (!winner) lastWhy = '一批 ' + batch.length + ' 家渠道均限流/超时';
    }
    if (!winner) {
      // 关键：失败也返回标准 chunk（HTTP 200 + 纯字符串提示），不让客户端 catch 出 [object Object]
      const msg = '【网关提示】当前所有上游渠道都在忙（' + lastWhy + '），请稍等约 20 秒后点「重新生成」再发一次。免费公益额度会波动，多试一次通常就好了。';
      if (stream) return streamFinish(reqModel || 'freeai-proxy', msg, true);
      return jsonRes(res, 200, wrapOAI(reqModel || 'freeai-proxy', msg), { 'X-FreeAI-Status': 'upstream_busy' });
    }
    const { c, r } = winner;
    c.lastOk = Date.now(); c.lastFailWhy = '';
    const model = reqModel || c.id;
    const text = asText(r.content);
    if (stream) return streamFinish(model, text, false);
    return jsonRes(res, 200, wrapOAI(model, text), { 'X-FreeAI-Model': c.id, 'X-FreeAI-Provider': c.provider });
  }

  jsonRes(res, 200, {
    name: 'freeai-proxy', version: '4.0-fast-race', alive: alive().length, confirmed: confirmed().length,
    poolSlots: alive().length, flagship: alive().filter(c => c.tier === 'flagship').length,
    mode: '跨渠道并发竞速(谁快用谁) + 流式秒首包 + 全局轮转 + 限流自动跳 + 30s 自动复活 + Key 池线性扩容',
    channels: [...new Set(POOL.map(c => c.provider))],
    endpoints: ['/healthz', '/v1/models', '/v1/chat/completions', '/v1/selftest'],
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('listening on ' + PORT + ' (githubToken=' + !!GH_TOKEN + ')');
  selftest(false);
});
