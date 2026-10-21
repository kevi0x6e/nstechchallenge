# ADR 0001 — Cache de veículos projetado exclusivamente a partir de CDC

| Metadado | Valor |
|---|---|
| Status | Aceito |
| Data | 2026-10-20 |
| Autor | Kevin Kusy |
| Escopo | `src/Fleet.CacheWorker`, `k8s/connector/fleet-cdc.json`, schema Valkey (`src/Fleet.Contracts`) |
| Relacionados | [Spec](../spec.md) · [C4](../c4.md) · [Test behaviors](../test-behaviors.md) |

## Contexto

Leitores de baixa latência (UIs de rastreio, alertas, regras de risco) precisam responder, para qualquer veículo, à pergunta *"onde ele está agora e em que estado?"*. Ler isso direto do banco operacional adiciona carga ao caminho de escrita e acopla todos os leitores ao banco.

A solução é um **read model materializado** no Valkey, mantido por um worker .NET 10 que consome dois tópicos CDC (Debezium → Kafka): `vehicles` (mutável: insert/update/delete) e `positions` (insert-only, alta frequência).

Restrições que guiam as decisões:

- **Só CDC:** o worker nunca lê o banco. Tudo o que ele sabe chega como evento.
- **Self-healing:** fazer replay do stream desde o offset 0 tem que reconstruir um cache **idêntico**.
- **Realidades do CDC:**
  - Entrega at-least-once, com re-emissão no restart do Debezium e no rebalance do consumer.
  - Desordem: replay e redelivery fazem eventos velhos chegarem depois de eventos novos; telemetria atrasada traz `recordedAt` antigo.
  - Os dois streams têm partições e lag independentes, então uma posição pode chegar antes do veículo existir.
  - Há uma transição snapshot → streaming.
  - Deletes chegam como evento `op=d` seguido de tombstone Kafka.

## Decisão

### 1. Modelo: um hash por veículo, com dois registros LWW independentes

Chave `vehicle:{id}` (Hash), com **dois grupos de campos disjuntos**. Cada grupo tem sua própria versão:

| Grupo | Campos | Versão | Fonte |
|---|---|---|---|
| Estado | `state`, `stateUpdatedAt`, `deleted`, `stateVersion` | `source.lsn` do Debezium | tópico `vehicles` |
| Posição | `lat`, `lng`, `recordedAt`, `positionId`, `positionVersion` | (`recordedAt`, `positionId`) | tópico `positions` |

Cada grupo é um *last-writer-wins register* em que "writer" é definido pela versão, não pela ordem de chegada. O merge é `max(version)`, que é **comutativo, associativo e idempotente**. Isso é o que garante o resto:

- **Idempotência:** reaplicar um evento com versão ≤ à atual é no-op.
- **Ordem de entrega irrelevante:** qualquer permutação do mesmo conjunto de eventos converge para o mesmo estado.
- **Replay determinístico:** o cache é função pura do *conjunto* de eventos do log.
- **Corrida entre streams:** como os grupos são disjuntos, a ordem entre tópicos não importa.

Uma leitura (`HGETALL`) responde às duas perguntas de uma vez.

### 2. O que é "mais novo" (chave de ordenação)

**Estado → LSN (ordem de commit do Postgres).** Para uma mesma linha, o LSN do registro de mudança cresce na ordem de commit: o row lock serializa updates concorrentes da mesma linha.

- O cache **espelha o banco**: o estado atual é o da última mudança commitada.
- Eventos de snapshot (`op=r`) carregam o LSN do ponto consistente do slot, que é menor que o de qualquer mudança de streaming posterior. Por isso a sobreposição snapshot → streaming se resolve sozinha.

**Posição → tempo de negócio (`recordedAt`), com desempate por `positionId`.** Posições são fatos imutáveis, e a "mais recente" é a de maior `recordedAt`, não a última inserida. Telemetria atrasada (inserida depois, mas com `recordedAt` antigo) **não** pode vencer. O `positionId` (bigserial) desempata `recordedAt` iguais, inclusive inserts duplicados, de forma determinística.

**Por que não `ts_ms` nem o `stateUpdatedAt` de negócio para o estado?**
- `ts_ms` tem resolução de milissegundo e empata facilmente.
- Ordenar o estado pelo tempo de negócio faria o cache **divergir do banco**. Se o writer grava um update com `state_updated_at` antigo, o banco aceitou essa linha como estado atual, e o cache precisa refletir isso. Se o negócio quiser rejeitar updates "atrasados", essa regra pertence ao **write path**, com um `UPDATE ... WHERE state_updated_at < $new` no banco, e não ao cache.
- O cenário e2e injeta updates com timestamps antigos justamente para pegar uma implementação que ordene por tempo de negócio por engano.

### 3. Escrita condicional: compare-and-set em Lua

Cada escrita é um script Lua (`EVALSHA`) sobre **uma única chave**: lê a versão do grupo, compara e só então faz `HSET`. Isso é atômico (o Valkey executa scripts de forma serial) e cluster-safe. **Nunca há `SET` cego.**

- **Versões como strings de largura fixa** (`D20`, e `recordedAtMicros:positionId` para posições). A comparação lexicográfica equivale à numérica e não perde precisão. Os números do Lua são doubles: acima de 2^53, dois LSNs distintos ficariam iguais. Há teste cobrindo esse caso.

### 4. Deletes: tombstone lógico

- `op=d` com LSN maior grava `deleted=1` e `stateVersion=lsn`, e remove `state` e `stateUpdatedAt`. **O hash não é apagado.**
- Por que não `DEL`: um evento atrasado (posição ou update anterior ao delete) recriaria a chave, e o veículo "ressuscitaria". O resultado dependeria da ordem de chegada, e o replay deixaria de ser determinístico.
- Re-insert do mesmo id com LSN maior ressuscita legitimamente (`deleted=0`).
- O grupo de posição é mantido. Os leitores tratam `deleted=1` como **404**.
- O tombstone Kafka (value `null`) é descartado, porque a informação já veio no `op=d`.
- `DECOMMISSIONED` é apenas um estado de negócio, não um delete.

### 5. Posição antes do veículo

O script de posição cria o hash só com o grupo de posição. Os leitores veem `state` ausente, o que significa **"posição conhecida, estado ainda desconhecido"**. Quando o evento do veículo chega, ele completa o mesmo hash. Não há buffer nem espera no worker, e com isso não há estado em memória para perder num restart.

### 6. At-least-once e offsets

- `enable.auto.offset.store=false`: o offset só é armazenado **depois** de o Valkey confirmar o batch. O commit é feito em background pelo librdkafka (auto-commit de 1s) e explicitamente no revoke e no `Close()`.
- Crash entre a escrita e o commit → redelivery → no-op pela guarda de versão.
- Valkey fora do ar → retry com backoff exponencial **no mesmo batch**: a partição não avança e nenhum evento é pulado.
- Mensagem indecodificável → log + métrica + skip. Em produção, iria para uma DLQ (ver "cortes").

### 7. Particionamento e paralelismo

- `message.key.columns=public.positions:vehicle_id` faz as posições serem chaveadas por **veículo**, não pelo PK da posição. Isso dá ordem por veículo dentro de uma partição e permite a coalescência.
- Um consumer por tópico, cada um em thread dedicada. O firehose de posições não atrasa as mudanças de estado.
- Escala horizontal: mais réplicas no mesmo consumer group. A idempotência torna os rebalances seguros.

### 8. Performance

- **Micro-batch** (até `MaxBatchSize`=2000 mensagens ou `MaxBatchWaitMs`=20 ms depois da primeira).
- **Coalescência no batch:** só o evento de maior versão por veículo vai para o Valkey. É seguro porque o merge é `max`. No stream de posições, isso reduz as escritas na proporção de posições por veículo no batch.
- **Pipelining:** todas as chamadas do batch são disparadas antes do primeiro `await` (`Task.WhenAll`). O multiplexer do StackExchange.Redis as envia numa única conexão, e o batch custa cerca de 1 RTT.
- **System.Text.Json source-generated**, desserializando direto do buffer do librdkafka (`ReadOnlySpan<byte>`), sem reflection e sem string intermediária.
- O Valkey roda sem persistência (`--save "" --appendonly no`), porque o log Kafka *é* a persistência. Usa `noeviction`, porque evicção apagaria veículos silenciosamente.

### 9. Replay

- Tópicos CDC com `retention.ms=-1` e `cleanup.policy=delete` (sem compaction).
- Replay = consumer group de volta ao offset 0 (`--reset-offsets --to-earliest`), com ou sem `FLUSHALL`. O `just e2e` faz os dois e compara o SHA-256 canônico do cache.

### 10. Escolhas de tecnologia

| Item | Escolha | Por quê | Alternativas consideradas |
|---|---|---|---|
| Fonte | PostgreSQL 17, `wal_level=logical`, plugin `pgoutput` | `pgoutput` é nativo (não precisa de extensão) | `decoderbufs` (exige extensão) |
| CDC | Debezium 3.3 em Kafka Connect, JSON sem schema | Padrão de mercado e expõe `source.lsn`. JSON sem schema é simples e leve | Avro + Schema Registry (melhor contrato e evolução, mas é mais um serviço) |
| Broker | Apache Kafka 4.1, KRaft, single node | Sem ZooKeeper e com menos memória | Redpanda (menor, mas foge do "Kafka" pedido) |
| Cache | Valkey 8.1 | OSS, compatível com Redis e com Lua | — |
| Cliente Kafka | **Confluent.Kafka** | Wrapper oficial do librdkafka. Maduro, rápido e com controle fino de offsets (`StoreOffset`) | KafkaFlow e MassTransit (abstraem justamente o controle de offset que este problema exige) |
| Cliente Valkey | **StackExchange.Redis** | Padrão de fato em .NET, com multiplexação, pipelining implícito e cache de script (EVALSHA com fallback em NOSCRIPT) | Valkey GLIDE (oficial do Valkey, mas o binding .NET ainda é jovem, o que é um risco) |
| Worker | .NET 10 `BackgroundService` + `IOptions` validado + `LoggerMessage` source-gen + `System.Diagnostics.Metrics` | Idiomático, sem alocação no log e com métricas compatíveis com OpenTelemetry | — |
| Imagens | `runtime:10.0-noble-chiseled` | Sem shell e non-root, com superfície mínima | `runtime:10.0` |
| Ambiente | devenv (ferramentas) + kind (K8s) + devspace (build/deploy) + just (comandos) | É o pedido, e reproduz o ambiente a partir do clone | Docker Compose (mais simples, fora do pedido) |

## Consequências

**Positivas:**
- Correção por construção (CRDT-like), sem coordenação entre consumidores e sem estado em memória.
- Replay e restart são triviais.
- Uma leitura por veículo.

**Trade-offs aceitos:**
- **Tombstones crescem para sempre** (um hash pequeno por veículo deletado). É aceitável para frota, que tem cardinalidade baixa. Um TTL quebraria a igualdade de replay, a menos que a expiração também fosse derivada do log.
- **O estado espelha o banco, não o tempo de negócio**, como explicado na decisão 2.
- **Retenção infinita** nos tópicos: positions cresce sem limite. Ver "Onde quebra".
- **A posição de um veículo deletado continua no hash.** O reader decide pelo flag `deleted`.
- **O snapshot de `positions` lê a tabela inteira.** Em produção, seria melhor um snapshot incremental, ou `snapshot.mode=no_data` para positions.

## Onde o design quebra (e o que faria)

1. **Compaction ou retenção finita em `positions`.** Isso quebra a **correção**, não só a igualdade. A compaction por `vehicle_id` mantém a última posição *inserida*, que pode ser uma telemetria atrasada com `recordedAt` antigo. Depois de um replay, o cache mostraria a posição errada. A retenção finita pode expirar justamente a posição máxima. A saída é publicar um tópico derivado, já reduzido ao "max por veículo", e compactar esse tópico; ou gravar snapshots periódicos do cache junto com os offsets e fazer replay a partir deles.
2. **Compaction em `vehicles` + `delete.retention.ms`.** O Kafka remove o tombstone e o `op=d` depois do prazo. Um replay posterior não veria o veículo deletado: a chave sumiria em vez de virar tombstone, e o cache deixaria de ser idêntico. Por isso usamos `cleanup.policy=delete` com retenção infinita.
3. **Recriação do slot ou do conector com snapshot novo.** Gera um novo conjunto de eventos `op=r` com LSN maior: o mesmo conteúdo, com outra `stateVersion`. O conteúdo é correto, mas o digest é diferente de um cache construído antes da recriação. Para comparar, seria preciso ignorar as versões ou reconstruir desde o novo snapshot.
4. **Failover do Postgres para uma réplica (LSN de outra timeline).** O LSN continua monotônico na nova primária. Mas um restore de backup para um LSN anterior faria eventos novos parecerem "velhos". A mitigação é um prefixo de época/timeline na versão.
5. **Mudanças de schema não compatíveis** (por exemplo, renomear `state`) quebram o parser. Avro/Protobuf com Schema Registry e regras de compatibilidade resolvem.
6. **Hot keys:** um único veículo concentra posições numa partição. Em escala normal de frota isso não preocupa.

## Onde uma sugestão de IA estaria errada

- **"Use `ts_ms` (ou `updated_at`) para ordenar."** Empata no milissegundo, depende de relógio e diverge da ordem de commit.
- **"Faça `DEL` no delete."** Ressuscita o veículo com eventos atrasados e torna o replay não determinístico.
- **"Ordene o estado por `stateUpdatedAt`."** O cache passa a discordar do banco. O teste de oráculo do e2e pega isso.
- **"Compare as versões como número no Lua."** Perde precisão acima de 2^53, e dois LSNs diferentes ficam iguais.
- **"Use `enable.auto.commit` com o store automático."** Faz commit de offset antes de o Valkey confirmar a escrita, e uma queda perde dados.
- **"Bufferize posições até o veículo existir."** Cria estado em memória que se perde no restart e cresce sem limite quando o veículo nunca chega.
