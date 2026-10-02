import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Provider } from 'react-redux';
import { createAppStore } from '../store/store';
import { OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel } from './OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel';
import type { OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient } from '../api/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';

describe('OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel', () => {
  it('executes a command and lists the resulting event', async () => {
    const client = { execute: vi.fn().mockResolvedValue({ type: 'ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted', aggregateId: 'agg-1', version: 1 }) } as unknown as OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient;
    render(
      <Provider store={createAppStore(client)}>
        <OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel />
      </Provider>
    );

    await userEvent.type(screen.getByLabelText('Aggregate id'), 'agg-1');
    await userEvent.click(screen.getByRole('button', { name: 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi' }));

    expect(await screen.findByText('ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted v1')).toBeInTheDocument();
    expect(client.execute).toHaveBeenCalledWith('agg-1', 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi', undefined, expect.objectContaining({ idempotencyKey: expect.any(String) }));
  });

  it('disables commands until an aggregate id is entered', () => {
    render(
      <Provider store={createAppStore({ execute: vi.fn() } as unknown as OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient)}>
        <OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel />
      </Provider>
    );
    expect(screen.getByRole('button', { name: 'process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi' })).toBeDisabled();
  });
});
