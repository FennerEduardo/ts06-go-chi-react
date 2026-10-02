import { describe, expect, it, vi } from 'vitest';
import { ApiError, COMMANDS, createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient } from './orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';

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

  it('raises ApiError with the server detail on failure', async () => {
    const client = createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient({ fetch: fakeFetch(422, { detail: 'Command id is required' }) });
    await expect(client.execute('agg-1', 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi')).rejects.toEqual(new ApiError(422, 'Command id is required'));
  });
});
