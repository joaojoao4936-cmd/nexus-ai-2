const MODEL = "@cf/qwen/qwen2.5-coder-32b-instruct";
const encoder = new TextEncoder();

function json(data, status, headers = {}) {
  return new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json; charset=utf-8", ...headers } });
}
function cors(origin, env) {
  const allowed = (env.ALLOWED_ORIGINS || "").split(",").map(x => x.trim()).filter(Boolean);
  if (!origin) return {};
  if (!allowed.includes(origin)) return null;
  return {
    "access-control-allow-origin": origin,
    "access-control-allow-methods": "POST, OPTIONS",
    "access-control-allow-headers": "Authorization, Content-Type",
    "access-control-max-age": "86400",
    "vary": "Origin"
  };
}
async function authenticated(request, env) {
  const token = request.headers.get("authorization") || "";
  if (!token.startsWith("Bearer ")) return false;
  const response = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
    headers: { apikey: env.SUPABASE_ANON_KEY, authorization: token }
  });
  return response.ok;
}
function toOpenAIStream(upstream) {
  const reader = upstream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  return new ReadableStream({
    async pull(controller) {
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) {
            if (buffer.trim()) emitLine(buffer.trim(), controller);
            controller.enqueue(encoder.encode("data: [DONE]\n\n"));
            controller.close();
            return;
          }
          buffer += decoder.decode(value, { stream: true });
          let index;
          while ((index = buffer.indexOf("\n")) >= 0) {
            const line = buffer.slice(0, index).trim();
            buffer = buffer.slice(index + 1);
            emitLine(line, controller);
          }
          return;
        }
      } catch (error) {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message: error.message || "Falha no fluxo de IA" } })}\n\n`));
        controller.close();
      }
    },
    async cancel() { try { await reader.cancel(); } catch {} }
  });
}
function emitLine(line, controller) {
  if (!line.startsWith("data:")) return;
  const raw = line.slice(5).trim();
  if (!raw || raw === "[DONE]") return;
  let item;
  try { item = JSON.parse(raw); } catch { return; }
  const text = item.response ?? item.output ?? item.choices?.[0]?.delta?.content ?? item.result?.response ?? "";
  if (text) controller.enqueue(encoder.encode(`data: ${JSON.stringify({ choices: [{ index: 0, delta: { content: String(text) } }] })}\n\n`));
  if (item.error) controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: item.error })}\n\n`));
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get("Origin");
    const corsHeaders = cors(origin, env);
    if (corsHeaders === null) return json({ error: "Origem não autorizada." }, 403);
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders || {} });
    const url = new URL(request.url);
    if (url.pathname === "/health") return json({ ok: true, service: "NEXUS AI 2.0", model: MODEL }, 200, corsHeaders || {});
    if (url.pathname !== "/chat" || request.method !== "POST") return json({ error: "Rota não encontrada." }, 404, corsHeaders || {});
    if (!env.SUPABASE_URL || !env.SUPABASE_ANON_KEY || !env.AI) return json({ error: "Worker sem configuração. Confira os bindings e variáveis." }, 503, corsHeaders || {});
    try {
      if (!await authenticated(request, env)) return json({ error: "Sessão inválida. Entre novamente." }, 401, corsHeaders || {});
      const raw = await request.text();
      if (raw.length > 100000) return json({ error: "Conversa muito grande. Reduza o histórico ou envie menos conteúdo." }, 413, corsHeaders || {});
      const body = JSON.parse(raw);
      if (!Array.isArray(body.messages) || body.messages.length < 1 || body.messages.length > 60) return json({ error: "Formato de conversa inválido." }, 400, corsHeaders || {});
      const messages = body.messages.map(m => ({
        role: ["system", "user", "assistant"].includes(m.role) ? m.role : "user",
        content: String(m.content || "").slice(0, 24000)
      }));
      const max_tokens = Math.max(128, Math.min(Number(body.max_tokens) || 1800, 3000));
      const temperature = Math.max(0, Math.min(Number(body.temperature) || 0.7, 1.5));
      const result = await env.AI.run(MODEL, { messages, stream: true, max_tokens, temperature });
      const stream = result && typeof result.getReader === 'function' ? result : result?.body;
      if (!stream) return json({ error: "O provedor de IA não retornou um fluxo válido." }, 502, corsHeaders || {});
      return new Response(toOpenAIStream(stream), {
        status: 200,
        headers: {
          ...(corsHeaders || {}),
          "content-type": "text/event-stream; charset=utf-8",
          "cache-control": "no-cache, no-transform",
          "x-content-type-options": "nosniff"
        }
      });
    } catch (error) {
      return json({ error: error.message || "Erro interno ao chamar a IA." }, 500, corsHeaders || {});
    }
  }
};
