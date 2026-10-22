# Fleet — Cache de veículos via CDC (.NET 10)

Um worker .NET 10 consome os streams CDC do Debezium (`vehicles` e `positions`) e mantém no **Valkey** uma view por veículo com o **estado atual** e a **última posição**. Ele **nunca** consulta o banco. A view é correta diante de duplicatas, desordem, deletes, snapshot → streaming e posições que chegam antes do veículo. Um replay desde o offset 0 reconstrói um cache **idêntico**.

```
mock writers ──SQL──▶ PostgreSQL ──WAL──▶ Debezium ──▶ Kafka ──▶ cache-worker ──Lua CAS──▶ Valkey ◀── readers
```

## Pré-requisitos

| Ferramenta | Observação |
|---|---|
| Docker (Desktop, OrbStack ou Colima) | rodando, com **≥ 4 GB de RAM** para a VM |
| [Nix](https://nixos.org/download) + [devenv](https://devenv.sh/getting-started/) | fornecem `just`, `kind`, `kubectl`, `devspace` e o .NET 10 SDK nas versões travadas em `devenv.lock` |

Nada mais precisa estar instalado no host.

## Os dois comandos

```bash
devenv shell      # entra no ambiente com todas as ferramentas (ou use direnv: `direnv allow`)
just up           # cria o cluster kind, builda as imagens e sobe Postgres, Kafka, Debezium, Valkey e o worker
just e2e          # prova ponta a ponta (sobe o ambiente antes, se necessário)
```

Se o `just` já estiver instalado no host, as receitas entram no `devenv shell` sozinhas.

O que o `just e2e` faz (e reexecuta do zero quantas vezes você quiser):
1. Reseta tabelas, slot e publication, tópicos, consumer group e cache.
2. Grava dados **antes** do conector existir, que serão capturados pelo snapshot.
3. Registra o Debezium (snapshot → streaming).
4. Roda os writers em paralelo com o caos ligado: `recordedAt` atrasado, inserts duplicados, updates com timestamp antigo, deletes, re-inserts e posições antes do veículo. No meio disso, **reinicia o conector** e **mata o worker**.
5. Compara o cache com um **oráculo calculado a partir do banco**. Só o teste lê o banco.
6. **Replay do offset 0 sobre o cache vivo** → o digest SHA-256 tem que ser igual.
7. **`FLUSHALL` + replay do offset 0** → o digest tem que ser igual (rebuild idêntico).

## Outras receitas

| Comando | O que faz |
|---|---|
| `just test` | Testes unitários e de integração (Lua contra um Valkey real via Testcontainers, incluindo a propriedade de replay) |
| `just read 7` | Lê `vehicle:7` como um leitor faria |
| `just replay` | Roda só as verificações de replay sobre o estado atual |
| `just status` | Pods, services e estado do conector |
| `just logs` | Logs do worker (com totais de received, applied e stale a cada 10s) |
| `just down` | Apaga o cluster |

## Documentos (a cadeia Spec → ADR → testes)

- [docs/spec.md](docs/spec.md): read model, eventos CDC, schema Valkey, componentes, sequência de trabalho e cortes.
- [docs/adr/0001-cdc-vehicle-cache.md](docs/adr/0001-cdc-vehicle-cache.md): decisões, alternativas, trade-offs e onde o design quebra.
- [docs/test-behaviors.md](docs/test-behaviors.md): Given/When/Then com o mapeamento para os testes automatizados.
- [docs/c4.md](docs/c4.md): Contexto, Containers, Componentes (Mermaid) e a sequência de um batch.

## Layout

```
src/Fleet.CacheWorker   worker: Cdc/ Projection/ Valkey/ Consumers/
src/Fleet.Contracts     contrato da view (chaves, campos, versões, digest)
src/Fleet.Scenario      plano determinístico (seed) compartilhado pelos writers e pelo verifier
src/Fleet.MockWriters   position writer + vehicle writer (só escrevem no Postgres)
src/Fleet.E2E           verifier: oráculo, digest, leitor
tests/                  xUnit + Testcontainers
k8s/                    manifests (infra, apps) + config do conector Debezium
scripts/                up / e2e / helpers chamados pelo justfile
```

## Troubleshooting

- **Pods `OOMKilled` ou pendentes:** aumente a memória da VM do Docker (Settings → Resources) para ≥ 4 GB.
- **Primeira execução lenta:** é o download das imagens (Kafka, Debezium, .NET SDK). As seguintes usam cache.
