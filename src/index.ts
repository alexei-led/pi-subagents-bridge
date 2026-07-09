import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/**
 * Bridge @tintinweb/pi-tasks RPC (v2) → nicobailon/pi-subagents RPC (v1).
 *
 * pi-tasks emits:    subagents:rpc:ping  subagents:rpc:spawn  subagents:rpc:stop
 * nicobailon hears:  subagents:rpc:v1:request {method: "spawn" | "stop"}
 *
 * The bridge answers ping locally, forwards spawn/stop to nicobailon,
 * and translates nicobailon's async-complete events back to tintinweb's
 * subagents:completed / subagents:failed events.
 *
 * Only agents spawned through this bridge are tracked — nicobailon's own
 * chain/parallel runs (e.g. pi-fusion) are left untouched.
 */

// ── tintinweb v2 channels ────────────────────────────────────────────────────
const T_PING      = "subagents:rpc:ping";
const T_SPAWN     = "subagents:rpc:spawn";
const T_STOP      = "subagents:rpc:stop";
const T_COMPLETED = "subagents:completed";
const T_FAILED    = "subagents:failed";

/** tintinweb reply channel: <channel>:reply:<requestId> */
const tReply = (ch: string, id: string) => `${ch}:reply:${id}`;

// ── nicobailon v1 channels ───────────────────────────────────────────────────
const NB_REQUEST  = "subagents:rpc:v1:request";
const NB_COMPLETE = "subagent:async-complete";

/** nicobailon reply channel: subagents:rpc:v1:reply:<requestId> */
const nbReply = (id: string) => `subagents:rpc:v1:reply:${id}`;

// ── Types ────────────────────────────────────────────────────────────────────

type TReplyOk<T>  = { success: true; data: T };
type TReplyErr    = { success: false; error: string };
type TReply<T>    = TReplyOk<T> | TReplyErr;

interface NbComplete {
  runId?:   string;
  asyncId?: string;
  id?:      string;
  success?: boolean;
  state?:   string;
  summary?: string;
  output?:  string;
  error?:   string;
  results?: Array<{ output?: string }>;
}

// ── Nicobailon RPC helper ─────────────────────────────────────────────────────

function nbRpc<T>(
  events: ExtensionAPI["events"],
  method: string,
  params: Record<string, unknown>,
  timeoutMs: number,
): Promise<T> {
  const id = randomUUID();

  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      unsub?.();
      reject(new Error(`nicobailon "${method}" RPC timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    const unsub = events.on(nbReply(id), (raw: unknown) => {
      unsub?.();
      clearTimeout(timer);
      const r = raw as { success: boolean; data?: T; error?: { message?: string } | string };
      if (r.success) {
        resolve(r.data as T);
      } else {
        const msg = typeof r.error === "object" ? r.error?.message : r.error;
        reject(new Error(msg || "nicobailon RPC error"));
      }
    });

    events.emit(NB_REQUEST, { version: 1, requestId: id, method, params });
  });
}

// ── Result extraction ─────────────────────────────────────────────────────────

function extractResult(p: NbComplete): string | undefined {
  if (p.summary?.trim()) return p.summary.trim();
  if (p.output?.trim())  return p.output.trim();
  const first = p.results?.[0];
  if (first?.output?.trim()) return first.output.trim();
  return undefined;
}

// ── Extension ────────────────────────────────────────────────────────────────

export default function registerBridge(pi: ExtensionAPI): void {
  /** runIds spawned through this bridge — used to filter async-complete events. */
  const spawned = new Set<string>();
  /** Seen completions — prevents double-processing if nicobailon fires twice. */
  const seen = new Set<string>();

  // PING — answered locally; no nicobailon call needed.
  // pi-tasks checks protocol version here; we declare v2 (what pi-tasks expects).
  pi.events.on(T_PING, (raw: unknown) => {
    const req = raw as { requestId?: string };
    if (!req?.requestId) return;
    const reply: TReply<{ version: number }> = { success: true, data: { version: 2 } };
    pi.events.emit(tReply(T_PING, req.requestId), reply);
  });

  // SPAWN — translate tintinweb → nicobailon and track the returned runId.
  pi.events.on(T_SPAWN, async (raw: unknown) => {
    const req = raw as {
      requestId?: string;
      type?: string;
      prompt?: string;
      options?: { description?: string; model?: string; maxTurns?: number };
    };
    const { requestId, type, prompt, options = {} } = req ?? {};
    if (!requestId || !type || !prompt) return;

    try {
      const data = await nbRpc<{ details?: { runId?: string; asyncId?: string } }>(
        pi.events,
        "spawn",
        {
          agent:   type,
          task:    prompt,
          async:   true,
          clarify: false,
          context: "fresh",
          description: options.description ?? prompt.slice(0, 80),
          ...(options.model    ? { model: options.model }                          : {}),
          ...(options.maxTurns ? { turnBudget: { maxTurns: options.maxTurns } }   : {}),
        },
        24_000, // pi-tasks spawn timeout is 30s; give nicobailon 24s
      );

      const runId = data?.details?.runId ?? data?.details?.asyncId;
      if (!runId) throw new Error("nicobailon spawn returned no runId in details");

      spawned.add(runId);
      pi.events.emit(tReply(T_SPAWN, requestId), { success: true, data: { id: runId } } satisfies TReplyOk<{ id: string }>);

    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      pi.events.emit(tReply(T_SPAWN, requestId), { success: false, error: msg } satisfies TReplyErr);
    }
  });

  // STOP — forward to nicobailon; always reply success since pi-tasks silences stop errors.
  pi.events.on(T_STOP, async (raw: unknown) => {
    const req = raw as { requestId?: string; agentId?: string };
    const { requestId, agentId } = req ?? {};
    if (!requestId) return;

    if (agentId) {
      try {
        await nbRpc(pi.events, "stop", { runId: agentId }, 8_000);
      } catch {
        // Agent may have already finished — not an error.
      }
      spawned.delete(agentId);
    }

    pi.events.emit(tReply(T_STOP, requestId), { success: true } satisfies TReplyOk<undefined>);
  });

  // COMPLETION — translate nicobailon:async-complete → tintinweb completed/failed.
  // Only processes agents spawned through this bridge (spawned.has guard).
  pi.events.on(NB_COMPLETE, (payload: unknown) => {
    const p = payload as NbComplete;
    const runId = p.runId ?? p.asyncId ?? p.id;
    if (!runId || !spawned.has(runId) || seen.has(runId)) return;

    seen.add(runId);
    spawned.delete(runId);

    if (p.state === "stopped") {
      const result = extractResult(p);
      pi.events.emit(T_FAILED, { id: runId, status: "stopped", ...(result ? { result } : {}) });

    } else if (p.success === false || p.state === "error" || p.state === "failed" || p.state === "aborted") {
      pi.events.emit(T_FAILED, {
        id:    runId,
        error: typeof p.error === "string" ? p.error : "Agent failed",
        status: p.state ?? "error",
      });

    } else {
      const result = extractResult(p);
      pi.events.emit(T_COMPLETED, { id: runId, ...(result ? { result } : {}) });
    }
  });

  // Announce presence. If pi-tasks loaded first and its initial ping already
  // timed out, this triggers it to re-check version compatibility.
  pi.events.emit("subagents:ready", {});
}
