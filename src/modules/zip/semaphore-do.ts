import { Env } from "../lib/types/types";

function jsonResponse(obj: unknown, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export class ZipSemaphoreDO {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  private sql() {
    return this.state.storage.sql as any;
  }

  private initIfNeeded() {
    const sql = this.sql();
    sql.exec(
      `CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        inUse INTEGER NOT NULL
      );`,
    );
    sql.exec(`INSERT INTO tokens (id, inUse) VALUES (1, 0) ON CONFLICT(id) DO NOTHING;`);
  }

  async fetch(req: Request): Promise<Response> {
    this.initIfNeeded();
    const url = new URL(req.url);

    if (req.method === "POST" && url.pathname === "/acquire") {
      const body = (await req.json().catch(() => ({}))) as any;
      const limit = typeof body.limit === "number" && body.limit > 0 ? body.limit : 1;

      const sql = this.sql();
      // One ZipSemaphore DO instance serializes acquire/release; we can safely read then
      // increment. The previous optimistic UPDATE-only path incorrectly returned success when
      // the UPDATE matched no rows at capacity (inUse already == limit).
      const rsBefore = sql.exec(`SELECT inUse FROM tokens WHERE id = 1;`);
      const inUseBefore = Number(rsBefore.one?.()?.inUse ?? 0);

      if (inUseBefore >= limit) {
        return jsonResponse(
          {
            acquired: false,
            inUse: inUseBefore,
            limit,
          },
          429,
        );
      }

      sql.exec(`UPDATE tokens SET inUse = inUse + 1 WHERE id = 1;`);
      const rsAfter = sql.exec(`SELECT inUse FROM tokens WHERE id = 1;`);
      const inUseAfter = Number(rsAfter.one?.()?.inUse ?? 0);

      if (inUseAfter > limit) {
        sql.exec(
          `UPDATE tokens SET inUse = CASE WHEN inUse > 0 THEN inUse - 1 ELSE 0 END WHERE id = 1;`,
        );
        const rsRb = sql.exec(`SELECT inUse FROM tokens WHERE id = 1;`);
        return jsonResponse(
          { acquired: false, inUse: Number(rsRb.one?.()?.inUse ?? 0), limit },
          429,
        );
      }

      return jsonResponse({ acquired: true, inUse: inUseAfter, limit }, 200);
    }

    if (req.method === "POST" && url.pathname === "/release") {
      const sql = this.sql();
      sql.exec(
        `UPDATE tokens SET inUse = CASE WHEN inUse > 0 THEN inUse - 1 ELSE 0 END WHERE id = 1;`,
      );
      const rs = sql.exec(`SELECT inUse FROM tokens WHERE id = 1;`);
      const row = rs.one?.();

      return jsonResponse({ released: true, inUse: Number(row?.inUse ?? 0) });
    }

    // if (req.method === "GET" && url.pathname === "/status") {
    //   const rs = this.sql().exec(`SELECT inUse FROM tokens WHERE id = 1;`);
    //   const row = rs.one?.();

    //   return jsonResponse({ inUse: Number(row?.inUse ?? 0) });
    // }

    return new Response("not found", { status: 404 });
  }
}
