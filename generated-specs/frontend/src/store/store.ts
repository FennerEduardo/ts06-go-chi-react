import { configureStore } from '@reduxjs/toolkit';
import { OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient } from '../api/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';
import { defaultClient, orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiReducer } from './orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSlice';

export function createAppStore(client: OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient = defaultClient()) {
  return configureStore({
    reducer: { orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi: orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiReducer },
    middleware: getDefault => getDefault({ thunk: { extraArgument: { client } } })
  });
}

export type AppStore = ReturnType<typeof createAppStore>;
export type RootState = ReturnType<AppStore['getState']>;
export type AppDispatch = AppStore['dispatch'];
