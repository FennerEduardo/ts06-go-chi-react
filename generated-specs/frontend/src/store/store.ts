import { configureStore } from '@reduxjs/toolkit';
import type { OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient } from '../api/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';
import { defaultClient as defaultOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient, orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiReducer } from './orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSlice';

/** One API client per feature of the service (each slice uses its own). */
export interface AppClients {
  orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi: OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient;
}

export function createAppStore(clients: Partial<AppClients> = {}) {
  const extraArgument: { clients: AppClients } = {
    clients: {
      orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi: clients.orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi ?? defaultOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient(),
    }
  };
  return configureStore({
    reducer: { orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi: orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiReducer },
    middleware: getDefault => getDefault({ thunk: { extraArgument } })
  });
}

export type AppStore = ReturnType<typeof createAppStore>;
export type RootState = ReturnType<AppStore['getState']>;
export type AppDispatch = AppStore['dispatch'];
