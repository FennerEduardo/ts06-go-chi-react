import { describe, expect, it, vi } from 'vitest';
import { ApiError, COMMANDS, createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient, newTraceparent } from './orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';

function fakeFetch(status: number, body: unknown) {
  return vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }));
}

describe('OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi API client', () => {
  it('exposes one method per domain command', () => {
    expect(COMMANDS).toEqual(['process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi']);
  });

  it('posts the command with tenant and idempotency headers', async () => {
    const fetch = fakeFetch(201, { type: 'ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted', aggregateId: 'agg-1', version: 1 });
    const client = createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient({ baseUrl: 'https://api.test/', tenantId: 'acme', fetch });

    const result = await client.processOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi('agg-1', { amount: 100 }, { idempotencyKey: 'key-1' });

    expect(result).toEqual({ type: 'ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted', aggregateId: 'agg-1', version: 1 });
    const [url, init] = fetch.mock.calls[0];
    expect(url).toBe('https://api.test/api/v1/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi/agg-1/process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi');
    expect(init?.method).toBe('POST');
    expect((init?.headers as Record<string, string>)['X-Tenant-Id']).toBe('acme');
    expect((init?.headers as Record<string, string>)['X-Idempotency-Key']).toBe('key-1');
    expect(JSON.parse(String(init?.body))).toEqual({ amount: 100 });
  });

  it('sends a W3C traceparent that starts a new trace per request', async () => {
    const fetch = fakeFetch(201, { type: 'ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted', aggregateId: 'agg-1', version: 1 });
    const client = createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient({ fetch });

    await client.execute('agg-1', 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi');
    await client.execute('agg-1', 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi');

    const sent = fetch.mock.calls.map(([, init]) => (init?.headers as Record<string, string>).traceparent);
    for (const traceparent of sent) expect(traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    expect(sent[0]).not.toBe(sent[1]);
    expect(newTraceparent()).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
  });

  it('propagates the caller trace context or none when disabled', async () => {
    const traced = fakeFetch(201, {});
    const parent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
    await createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient({ fetch: traced, traceparent: () => parent }).execute('agg-1', 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi');
    expect((traced.mock.calls[0][1]?.headers as Record<string, string>).traceparent).toBe(parent);

    const untraced = fakeFetch(201, {});
    await createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient({ fetch: untraced, traceparent: false }).execute('agg-1', 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi');
    expect((untraced.mock.calls[0][1]?.headers as Record<string, string>).traceparent).toBeUndefined();
  });

  it('raises ApiError with the server detail on failure', async () => {
    const client = createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient({ fetch: fakeFetch(422, { detail: 'Command id is required' }) });
    await expect(client.execute('agg-1', 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi')).rejects.toEqual(new ApiError(422, 'Command id is required'));
  });
});
