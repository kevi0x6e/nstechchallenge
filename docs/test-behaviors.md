# Test behaviors

Formato Given / When / Then. A coluna **Automação** indica onde cada comportamento é provado:

- **IT:** `tests/Fleet.CacheWorker.Tests` (projetores + Lua reais contra um Valkey via Testcontainers; `just test`).
- **E2E:** `just e2e` (Postgres → Debezium → Kafka → worker → Valkey, verificado contra o oráculo do banco e por digest).

| ID | Comportamento | Automação |
|---|---|---|
| TB-01 | Update de estado refletido | IT `State_update_is_reflected` · E2E assert |
| TB-02 | Nova posição refletida | IT `New_position_is_reflected` · E2E assert |
| TB-03 | Posição fora de ordem ignorada | IT `Stale_position_is_ignored` · E2E (≈10% de `recordedAt` atrasado) |
| TB-04 | Estado fora de ordem ignorado | IT `Stale_state_is_ignored` · E2E (replay sobre cache vivo) |
| TB-05 | Evento CDC duplicado é no-op | IT `Duplicate_event_is_noop` · E2E (replay do offset 0 reentrega todos os eventos; kill do worker reentrega os offsets não commitados; inserts duplicados em `positions`) |
| TB-06 | Delete marca tombstone e não ressuscita | IT `Delete_flags_vehicle`, `Reinsert_after_delete_resurrects` · E2E assert |
| TB-07 | Posição antes do veículo existir | IT `Position_before_vehicle` · E2E (5 veículos da corrida) |
| TB-08 | Restart do worker no meio do stream não perde nada | IT `Redelivery_after_restart_is_noop` · E2E (kill do pod com o writer de posições em plena escrita + assert) |
| TB-09 | Replay do offset 0 reconstrói o cache idêntico | IT `Shuffled_duplicated_replays_converge` · E2E (digest: replay sobre o cache vivo e rebuild com FLUSHALL) |
| TB-10 | Transição snapshot → streaming | IT `Snapshot_then_streaming_overlap` · E2E (seed antes do conector e stream concorrente ao snapshot) |
| TB-11 | Estado espelha a ordem de commit, não o tempo de negócio | E2E assert (updates com `state_updated_at` antigo; o cache tem que bater com o banco) |

---

### TB-01 — Update de estado refletido
- **Given** o veículo 1 está em `REGISTERED` (LSN 100)
- **When** chega `op=u` com `state=ACTIVE` e LSN 200
- **Then** `vehicle:1` tem `state=ACTIVE`, `stateUpdatedAt` do evento, `deleted=0` e `stateVersion=LSN 200`

### TB-02 — Nova posição refletida
- **Given** o veículo 1 tem a posição 10 com `recordedAt=t1`
- **When** chega a posição 11 com `recordedAt=t2 > t1`
- **Then** `lat`, `lng`, `recordedAt` e `positionId` passam a ser os da posição 11

### TB-03 — Posição fora de ordem ignorada
- **Given** o veículo 1 tem a posição 11 com `recordedAt=20s`
- **When** chega a posição 12 (inserida depois) com `recordedAt=10s`
- **Then** o script devolve 0 e o cache continua com a posição 11

### TB-04 — Estado fora de ordem ignorado
- **Given** o veículo 1 está em `MAINTENANCE` com LSN 300
- **When** é re-entregue um `op=u` antigo (`ACTIVE`, LSN 200), seja por replay, rebalance ou restart
- **Then** o script devolve 0 e o estado continua `MAINTENANCE`

### TB-05 — Duplicata é no-op
- **Given** um cache com estado (LSN 300) e posição (id 5)
- **When** os mesmos eventos chegam de novo
- **Then** 0 escritas aplicadas e o digest do cache não muda

### TB-06 — Delete / tombstone
- **Given** o veículo 1 está `ACTIVE` (LSN 100) e tem uma posição
- **When** chega `op=d` com LSN 500, e depois um update atrasado (LSN 400) e uma posição atrasada
- **Then** `deleted=1`, sem `state`, com `stateVersion=LSN 500`. O update atrasado é ignorado. A posição atualiza o grupo de posição, mas o leitor continua vendo 404
- **And (TB-06b)** um re-insert com LSN 600 volta para `deleted=0` com o estado novo

### TB-07 — Posição antes do veículo
- **Given** não há `vehicle:9`
- **When** chega uma posição do veículo 9
- **Then** o hash existe só com o grupo de posição, e o leitor vê "estado desconhecido"
- **When** depois chega o `op=c` do veículo 9
- **Then** o mesmo hash passa a ter o estado e a posição. Na ordem inversa, o resultado é o mesmo

### TB-08 — Restart do worker no meio do stream
- **Given** o worker já aplicou um batch, mas o offset não foi commitado
- **When** o pod morre e o batch é re-entregue
- **Then** nenhuma escrita é aplicada (no-op) e nada se perde. No E2E, `kubectl delete pod` acontece com o writer de posições ainda escrevendo, e o assert final contra o banco passa

### TB-09 — Replay reconstrói o cache idêntico
- **Given** o histórico completo de eventos dos dois tópicos
- **When** ele é aplicado em 10 ordens aleatórias, com ~20% de duplicatas, batches de tamanho aleatório e intercalação aleatória entre os tópicos
- **Then** o SHA-256 canônico é igual ao da aplicação em ordem do log
- **E2E:**
  1. digest A depois do caos;
  2. offsets rebobinados para 0 com o cache populado → digest A;
  3. `FLUSHALL` + offsets rebobinados para 0 → digest A.

### TB-10 — Snapshot → streaming
- **Given** linhas gravadas antes de o conector existir (`op=r`, com o LSN do ponto do snapshot)
- **When** os updates de streaming (com LSN maior) chegam antes ou depois da linha do snapshot
- **Then** prevalece o estado de maior LSN, independente da ordem de chegada

### TB-11 — Estado espelha o banco
- **Given** o writer grava um update com `state_updated_at` *mais antigo* que o atual
- **When** o CDC entrega esse update (que tem LSN maior)
- **Then** o cache reflete o update, porque é o que o banco tem. O oráculo do E2E compara com o banco e pegaria uma implementação que ordenasse por tempo de negócio
