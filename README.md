# wundergraph-subscriptions-snapshot

Custom module para o [Cosmo Router](https://github.com/wundergraph/cosmo) que envia o **último estado publicado** de uma subscription do Cosmo Streams (EDFS) para cada cliente **no momento em que ele se inscreve**, mesmo com a deduplicação do router.

## O problema

No EDFS, todos os clientes inscritos no mesmo provider + subjects/topics/channels compartilham uma única assinatura no broker. Quem entra depois só recebe eventos futuros, e até o próximo evento a tela fica vazia.

## Como funciona

O módulo implementa dois hooks do Cosmo Streams:

| Hook | Quando roda | O que faz |
|---|---|---|
| `BeforeEventsDispatch` | Uma vez por lote recebido do broker (não por assinante) | Guarda o último evento do lote, indexado por `provider type + provider id + subjects` |
| `SubscriptionOnStart` | Uma vez por cliente, ao se inscrever | Se existe snapshot para aquela chave, emite **só para esse cliente** com `EmitEvent` |

Como o evento do EDFS normalmente carrega só a chave da entidade (`{"__typename":"Employee","id":1}`), o router resolve os campos pedidos nos subgraphs na hora de entregar o snapshot. **O cliente recebe dados atuais**, não uma cópia velha.

Os subjects já chegam interpolados com os argumentos (`employeeUpdated.{{ args.id }}` → `employeeUpdated.42`), então cada combinação de argumentos tem seu próprio snapshot.

O módulo não define prioridade e, por isso, roda **depois** dos outros módulos no `SubscriptionOnStart`: se um módulo de autorização rejeitar o cliente, o snapshot não é enviado.

Subscriptions "clássicas" (servidas por um subgraph via WebSocket/SSE) são ignoradas, pois o router não expõe hook de recebimento para elas.

## Uso

Requer Go 1.26+ (mesma versão exigida pelo router).

```bash
go build -o router ./cmd/router
cp config.example.yaml config.yaml   # ajuste providers e o módulo
GRAPH_API_TOKEN=... ./router
```

Ou com Docker:

```bash
docker build -t router-snapshot .
docker run -p 3002:3002 \
  -e GRAPH_API_TOKEN=... \
  -e CONFIG_PATH=/config.yaml \
  -v ./config.yaml:/config.yaml \
  router-snapshot
```

Se você já tem um router customizado com outros módulos, basta importar o pacote no seu `main.go`:

```go
import _ "github.com/tpisani/wundergraph-subscriptions-snapshot/snapshot"
```

## Configuração

```yaml
modules:
  subscriptionSnapshot:
    fields: [employeeUpdated]  # campos raiz com snapshot; vazio = todas as subscriptions EDFS
    ttl: "10m"                 # descarta snapshots mais velhos; vazio ou "0" = nunca
    max_entries: 10000         # limite LRU; 0 = padrão (10000), -1 = sem limite
```

## Limitações e cuidados

- **Cache em memória, por instância.** O snapshot só existe depois que aquela instância do router recebeu ao menos um evento daquele subject. Após um restart, ou numa réplica nova, o primeiro cliente não recebe nada até o próximo evento.
- **Snapshot pode ficar "parado".** Quando o último cliente sai, o router fecha a assinatura no broker e o cache para de ser atualizado. Use `ttl` se a *chave* da entidade puder mudar sem que haja assinantes (os campos em si são sempre resolvidos de novo nos subgraphs).
- **Não passa pelos `OnReceiveEvents` de outros módulos.** O `EmitEvent` entrega direto ao cliente. Se você filtra eventos por usuário em `OnReceiveEvents`, replique o filtro (ou restrinja via `fields`).
- **Possível duplicata.** Se um evento chegar exatamente enquanto o cliente se inscreve, ele pode receber o mesmo estado duas vezes. Como os dados são idempotentes, normalmente é inofensivo.
- **Payload com dados além da chave.** Se seus eventos carregam campos extras (não só a chave), esses campos vêm do snapshot e podem estar desatualizados até o próximo evento.

## Desenvolvimento

```bash
go test ./...
```

Testado contra `github.com/wundergraph/cosmo/router` na versão `router@0.362.0`. Como o módulo é compilado junto com o router, atualize a dependência e recompile a cada upgrade do router.
