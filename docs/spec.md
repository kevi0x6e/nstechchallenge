# Spec — CDC-Driven Vehicle Cache

> Cadeia: **Spec (este doc)** → [ADR 0001](adr/0001-cdc-vehicle-cache.md) (decisões) → [test-behaviors](test-behaviors.md) → código → PR.
> Diagramas: [C4](c4.md).

## 1. Objetivo

Manter no Valkey, **apenas a partir de CDC**, uma view por veículo que responda com uma leitura: *onde ele está agora e em que estado está*. A view tem que ser correta diante de duplicatas, desordem, deletes, snapshot → streaming e a corrida entre streams. Também tem que ser **reconstruível de forma idêntica** por replay do log.

Fora de escopo: API HTTP de leitura (o leitor de referência é `just read <id>`), histórico de posições e consultas geoespaciais.

## 2. Read model (Valkey)

**Chave:** `vehicle:{id}` → Hash. A definição de código está em `src/Fleet.Contracts/VehicleViewSchema.cs`.

| Campo | Grupo | Exemplo | Significado |
|---|---|---|---|
| `state` | estado | `ACTIVE` | Estado atual (ausente se desconhecido ou deletado) |
| `stateUpdatedAt` | estado | `2026-01-01T00:12:00.000000Z` | Timestamp de negócio do estado (ISO-8601 UTC, µs) |
| `deleted` | estado | `0` / `1` | `1` = tombstone, e o leitor responde 404 |
| `stateVersion` | estado | `00000000000023456789` | LSN do Postgres, com 20 dígitos (ordem) |
| `lat`, `lng` | posição | `-23.551234` | Última posição (round-trip exato do double) |
| `recordedAt` | posição | `2026-01-01T00:03:12.345000Z` | Tempo de negócio da posição |
| `positionId` | posição | `1874` | PK da linha em `positions` |
| `positionVersion` | posição | `<µs 20d>:<id 20d>` | (`recordedAt`, `positionId`) (ordem) |

**Como interpretar uma leitura:**

| Hash | Significado para o leitor |
|---|---|
| inexistente | veículo nunca visto |
| sem `state`, com `deleted` ausente | posição recebida, veículo ainda não visto (corrida entre streams) |
| `deleted=1` | veículo removido → 404 (a posição pode estar presente, mas é ignorada) |
| `state` presente, `deleted=0` | normal |

## 3. Eventos CDC usados

Debezium PostgreSQL 3.3, `JsonConverter` com `schemas.enable=false`, tópicos `fleet.public.vehicles` e `fleet.public.positions`. Este é o subconjunto do envelope em que o worker confia; o resto é ignorado:

```jsonc
{
  "before": { "id": 7, ... } | null,   // delete: só a PK (REPLICA IDENTITY DEFAULT)
  "after":  { ...linha... }  | null,
  "source": { "lsn": 23456789, "ts_ms": 1767225600000, "table": "vehicles", ... },
  "op": "r" | "c" | "u" | "d"
}
```

- `vehicles.after`: `id` (bigint), `state` (text), `state_updated_at` (timestamptz → string ISO-8601, `ZonedTimestamp`).
- `positions.after`: `id` (bigserial), `vehicle_id`, `lat`, `lng` (double), `recorded_at` (timestamptz → string).
- **Chave Kafka:** `vehicles` → `{"id":7}`; `positions` → `{"vehicle_id":7}` (via `message.key.columns`).
- **Tombstone Kafka** (value `null`, depois de `op=d`): ignorado.
- `op=t` (truncate) não é emitido (é o default do Debezium: `skipped.operations=t`).

**Mapeamento para eventos do domínio** (`Projection/CdcMapper.cs`):

| Tópico | op | Evento | Versão |
|---|---|---|---|
| vehicles | r, c, u | `VehicleStateChange(deleted=false)` | `lsn` |
| vehicles | d | `VehicleStateChange(deleted=true)` | `lsn` |
| positions | r, c | `PositionObserved` | (`recorded_at`, `id`) |
| positions | outros | ignorado | — |

## 4. Componentes do worker (`src/Fleet.CacheWorker`)

| Componente | Responsabilidade |
|---|---|
| `Consumers/CdcConsumerService<TRow>` | Um por tópico, em thread dedicada. Faz o loop *batch → projetar → StoreOffset*, com retry do batch se o Valkey falhar, commit no revoke e no close, e um log de totais a cada 10s. |
| `Cdc/JsonValueDeserializer` + `CdcJsonContext` | Desserialização source-gen, direto do buffer. Tombstone vira `null`. |
| `Projection/CdcMapper` | Envelope → evento versionado (as regras da seção 3). |
| `Projection/LatestWins` | Coalescência no batch: o máximo por veículo. |
| `Projection/VehicleStateProjector`, `PositionProjector` | Mapeiam, coalescem e chamam o store. |
| `Valkey/ValkeyVehicleViewStore` + `LuaScripts` | Compare-and-set atômico por chave, com pipelining. |
| `Diagnostics/CacheWorkerMetrics` | Contadores `received`, `applied`, `stale` e `skipped` por stream. |
| `Configuration/Options` | `Kafka:*`, `Valkey:*`, validados no startup (`ValidateOnStart`). |

**Algoritmo de escrita (por grupo):**

```
current = HGET key <group>Version
if current != nil and current >= new: return 0      -- duplicata ou evento velho: no-op
HSET key <group>Version new, <campos do grupo>      -- delete: HDEL state*, deleted=1
return 1
```

## 5. Ambiente

| Peça | Onde |
|---|---|
| Ferramentas (just, kind, kubectl, devspace, dotnet 10) | `devenv.nix` |
| Cluster | kind `fleet` (`k8s/kind-config.yaml`) |
| Build e deploy | `devspace.yaml`: 3 imagens a partir do mesmo `Dockerfile` (arg `PROJECT`), manifests em `k8s/infra` e `k8s/apps` |
| Conector | `k8s/connector/fleet-cdc.json`, registrado pelo e2e com um nome novo a cada execução (força um snapshot limpo) |
| Comandos | `justfile` → `scripts/*.sh` |

**Mock writers** (`src/Fleet.MockWriters`, com o cenário em `src/Fleet.Scenario`):
- Uma seed gera um plano determinístico (`ScenarioPlan`). A mesma seed produz a mesma sequência de escritas.
- `snapshot`: veículos e posições gravados antes do conector existir.
- `stream`: o `VehicleWriter` e o `PositionWriter` rodam em paralelo, a taxas configuráveis (`--vehicles-rate`, `--positions-rate`).
- Injeções:
  - ~10% de `recordedAt` atrasado e ~5% de inserts duplicados;
  - ~10% de updates com `state_updated_at` antigo;
  - delete de 1 em cada 7 ids (metade deles é re-inserida);
  - 5 veículos cujas posições chegam antes da linha do veículo.

**Verifier** (`src/Fleet.E2E`):
- `assert`: espera lag 0 e compara campo a campo o cache com o **oráculo** calculado a partir do banco. Só o teste lê o banco.
- `digest`: espera quiescência e calcula o SHA-256 canônico.
- `read`: o leitor de referência.

## 6. Como sequenciei o trabalho

1. **Modelo de correção primeiro.** Defini o que é "mais novo" para cada stream e provei, no papel, que o merge por `max` é comutativo e idempotente. Isso virou o ADR.
2. **Núcleo testável sem infraestrutura:** mapper, tokens de versão e coalescência, com testes unitários.
3. **Scripts Lua contra um Valkey real** (Testcontainers): os 10 comportamentos e a **propriedade de replay** (10 permutações embaralhadas e duplicadas geram o mesmo digest).
4. **Consumer Kafka** com at-least-once explícito.
5. **Ambiente** (kind/devspace/just) e **cenário determinístico** com todos os casos difíceis.
6. **E2E com oráculo e digest**, e um caos real (restart do conector e kill do worker no meio do stream).
7. Docs revisados para bater com o que roda.

## 7. O que cortei e por quê

| Corte | Motivo / o que faria |
|---|---|
| DLQ para mensagens indecodificáveis | Hoje é log + métrica + skip. Em produção iria para um tópico `*.dlq` com alerta. |
| Avro/Protobuf + Schema Registry | JSON sem schema basta para o desafio. Em produção, usaria um contrato versionado com compatibilidade forward. |
| API HTTP de leitura | Fora do núcleo. `just read` e o `VehicleViewSchema` são o contrato. |
| OpenTelemetry exporter e health checks HTTP | As métricas já usam `System.Diagnostics.Metrics`. Falta só plugar o exporter e um `/healthz` com o lag. |
| TTL de tombstones | Quebraria a igualdade de replay (ADR, trade-offs). |
| Multi-node e alta disponibilidade (Kafka RF=3, Valkey replicado) | O ambiente local tem 8 GB de RAM. O design não muda. |
| Snapshot incremental de `positions` | Relevante com volume real (ADR, consequências). |
