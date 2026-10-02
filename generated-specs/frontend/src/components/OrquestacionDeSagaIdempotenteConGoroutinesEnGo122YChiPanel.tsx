import { useState } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { COMMANDS } from '../api/orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi-client';
import type { AppDispatch, RootState } from '../store/store';
import { executeCommand } from '../store/orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiSlice';

export function OrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiPanel() {
  const dispatch = useDispatch<AppDispatch>();
  const { events, status, error } = useSelector((s: RootState) => s.orquestacionDeSagaIdempotenteConGoroutinesEnGo122YChi);
  const [aggregateId, setAggregateId] = useState('');

  return (
    <section aria-label="Orquestación de Saga Idempotente con Goroutines en Go 1.22 y chi">
      <h1>Orquestación de Saga Idempotente con Goroutines en Go 1.22 y chi</h1>
      <label>
        Aggregate id
        <input value={aggregateId} onChange={e => setAggregateId(e.target.value)} />
      </label>
      {COMMANDS.map(command => (
        <button key={command} disabled={!aggregateId || status === 'loading'} onClick={() => dispatch(executeCommand({ id: aggregateId, command }))}>
          {command}
        </button>
      ))}
      {error && <p role="alert">{error}</p>}
      <ul aria-label="events">
        {events.map(e => (
          <li key={`${e.aggregateId}-${e.version}`}>
            {e.type} v{e.version}
          </li>
        ))}
      </ul>
    </section>
  );
}
