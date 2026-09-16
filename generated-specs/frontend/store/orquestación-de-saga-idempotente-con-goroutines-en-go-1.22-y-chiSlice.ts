// React 18 + Redux Toolkit State & Client
import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit';
import axios from 'axios';

export interface OrquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiState {
  items: any[];
  selectedItem: any | null;
  loading: boolean;
  error: string | null;
  tenantId: string | null;
}

const initialState: OrquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiState = {
  items: [],
  selectedItem: null,
  loading: false,
  error: null,
  tenantId: null
};

// Async Thunk to consume Backend API
export const fetchOrquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiList = createAsyncThunk(
  'orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChi/fetchList',
  async (tenantId: string | undefined, { rejectWithValue }) => {
    try {
      const response = await axios.get(`/api/v1/orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChi`, {
        headers: tenantId ? { 'X-Tenant-ID': tenantId } : {}
      });
      return response.data;
    } catch (err: any) {
      return rejectWithValue(err.response?.data?.message || 'Error fetching data');
    }
  }
);

export const executeOrquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiCommand = createAsyncThunk(
  'orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChi/executeCommand',
  async (payload: { commandName: string; data: any; idempotencyKey?: string }, { rejectWithValue }) => {
    try {
      const headers: Record<string, string> = {};
      if (payload.idempotencyKey) {
        headers['X-Idempotency-Key'] = payload.idempotencyKey;
      }
      const response = await axios.post(`/api/v1/orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChi/commands`, payload.data, { headers });
      return response.data;
    } catch (err: any) {
      return rejectWithValue(err.response?.data?.message || 'Error executing command');
    }
  }
);

export const orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiSlice = createSlice({
  name: 'orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChi',
  initialState,
  reducers: {
    setTenantId: (state, action: PayloadAction<string>) => {
      state.tenantId = action.payload;
    },
    onRealtimeEventReceived: (state, action: PayloadAction<{ eventType: string; payload: any }>) => {
      state.items.unshift(action.payload);
    },
    resetState: (state) => {
      Object.assign(state, initialState);
    }
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchOrquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiList.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(fetchOrquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiList.fulfilled, (state, action) => {
        state.loading = false;
        state.items = action.payload;
      })
      .addCase(fetchOrquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiList.rejected, (state, action) => {
        state.loading = false;
        state.error = action.payload as string;
      });
  }
});

export const { setTenantId, onRealtimeEventReceived, resetState } = orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiSlice.actions;
export default orquestaciónDeSagaIdempotenteConGoroutinesEnGo1.22YChiSlice.reducer;
