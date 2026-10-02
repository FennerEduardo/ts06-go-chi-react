import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { CommandName, CommandResult, createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient, OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient } from '../api/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';

export interface OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiState {
  events: CommandResult[];
  status: 'idle' | 'loading' | 'failed';
  error: string | null;
}

const initialState: OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiState = { events: [], status: 'idle', error: null };

export interface ExecuteArgs {
  id: string;
  command: CommandName;
  payload?: Record<string, unknown>;
}

export const executeCommand = createAsyncThunk<CommandResult, ExecuteArgs, { extra: { client: OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient } }>(
  'orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi/execute',
  ({ id, command, payload }, { extra }) => extra.client.execute(id, command, payload, { idempotencyKey: crypto.randomUUID() })
);

const orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSlice = createSlice({
  name: 'orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi',
  initialState,
  reducers: {},
  extraReducers: builder => {
    builder
      .addCase(executeCommand.pending, state => {
        state.status = 'loading';
        state.error = null;
      })
      .addCase(executeCommand.fulfilled, (state, action) => {
        state.status = 'idle';
        state.events.push(action.payload);
      })
      .addCase(executeCommand.rejected, (state, action) => {
        state.status = 'failed';
        state.error = action.error.message ?? 'Request failed';
      });
  }
});

export const orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiReducer = orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSlice.reducer;
export const defaultClient = () => createOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiClient({ baseUrl: import.meta.env.VITE_API_URL ?? '' });
