import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { Provider } from 'react-redux';
import { OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel } from './components/OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel';
import { createAppStore } from './store/store';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Provider store={createAppStore()}>
      <OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel />
    </Provider>
  </StrictMode>
);
