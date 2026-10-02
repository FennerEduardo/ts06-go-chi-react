import { describe, expect, it, vi } from 'vitest';
import { createAppStore } from './store';
import { executeCommand } from './orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSlice';
import type { OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient } from '../api/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';

describe('orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi slice', () => {
  it('records the event returned by the backend', async () => {
    const result = { type: 'ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted', aggregateId: 'agg-1', version: 1 };
    const client = { execute: vi.fn().mockResolvedValue(result) } as unknown as OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient;
    const store = createAppStore(client);

    await store.dispatch(executeCommand({ id: 'agg-1', command: 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi' }));

    expect(store.getState().orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi).toEqual({ events: [result], status: 'idle', error: null });
  });

  it('keeps the error message when the command fails', async () => {
    const client = { execute: vi.fn().mockRejectedValue(new Error('Command id is required')) } as unknown as OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient;
    const store = createAppStore(client);

    await store.dispatch(executeCommand({ id: 'agg-1', command: 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi' }));

    expect(store.getState().orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi.status).toBe('failed');
    expect(store.getState().orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi.error).toBe('Command id is required');
  });
});
