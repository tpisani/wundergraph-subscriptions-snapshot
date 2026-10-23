# wundergraph-subscriptions-snapshot

Custom module para o [Cosmo Router](https://github.com/wundergraph/cosmo) que envia a **última mensagem** de uma subscription servida por subgraph para cada cliente **no momento em que ele se inscreve**, mantendo a deduplicação do router.

## O problema

Com deduplicação, todos os clientes que assinam a mesma operação com as mesmas variáveis (e os mesmos headers repassados) compartilham **uma única** subscription do router com o subgraph. Quem entra depois só recebe as mensagens futuras e fica sem dados até a próxima atualização.

## Como funciona

```
cliente A ──┐                         ┌── tap: guarda a última mensagem
cliente B ──┼── router (dedup) ── SSE ┤      (chave = operação + variáveis + headers)
cliente C ──┘                         └── subgraph
```

O módulo usa dois pontos de extensão públicos do router:

| Hook | O que faz |
|---|---|
| `OnOriginResponse` | Quando o subgraph responde a uma subscription com `text/event-stream`, envolve o corpo da resposta com um leitor que **repassa todos os bytes intactos** para a engine e, em paralelo, guarda a última mensagem de dados. O parsing é uma cópia exata do que a engine faz. |
| `SubscriptionOnStart` | Roda para **cada** cliente que assina, inclusive quem entra numa subscription já deduplicada. Se existe uma mensagem guardada para a chave do cliente, ela é emitida **só para ele** com `EmitEvent`. |

A mensagem guardada é a resposta completa do subgraph, no mesmo formato que a engine recebe. A engine a trata como uma mensagem vinda do subgraph e resolve a seleção do próprio cliente, incluindo campos de entidades buscados em outros subgraphs.

**A chave do snapshot** é um SHA-256 de:
- hash normalizado da operação,
- variáveis (com as chaves ordenadas),
- valores dos headers em `key_headers` (padrão: `Authorization`).

Um cliente só recebe snapshot gravado com o **mesmo** `Authorization`. Como os valores entram no hash, tokens não ficam em memória em texto puro.

**Ciclo de vida:** quando o stream upstream termina (todos os clientes saíram, o subgraph mandou `complete`/`error` ou a conexão caiu), o snapshot é descartado. Um stream que está fechando não apaga o snapshot de um stream mais novo com a mesma chave.

O módulo não define prioridade, então roda **depois** dos outros módulos no `SubscriptionOnStart`: se um módulo de autorização rejeitar o cliente, nada é enviado.

## Requisito: SSE entre router e subgraph

O módulo só observa o protocolo **SSE** no trecho router → subgraph. Com WebSocket upstream, o router multiplexa várias subscriptions numa única conexão, e não há como observá-la de forma segura.

Isso **não** afeta os clientes: eles continuam usando graphql-ws, SSE ou multipart, e o router traduz entre os protocolos.

```bash
npx wgc subgraph publish prices --subscription-protocol sse_post --schema prices.graphql
```

Ou, num `graph.yaml` local:

```yaml
subgraphs:
  - name: prices
    routing_url: http://prices:4001/graphql
    subscription:
      protocol: sse_post   # ou sse (GET)
```

## Uso

Requer Go 1.26+ (a mesma versão exigida pelo router).

```bash
go build -o router ./cmd/router
cp config.example.yaml config.yaml
GRAPH_API_TOKEN=... ./router
```

Com Docker:

```bash
docker build -t router-snapshot .
docker run -p 3002:3002 -e GRAPH_API_TOKEN=... -e CONFIG_PATH=/config.yaml \
  -v ./config.yaml:/config.yaml router-snapshot
```

Se você já tem um router customizado com outros módulos, basta importar o pacote no seu `main.go`:

```go
import _ "github.com/tpisani/wundergraph-subscriptions-snapshot/snapshot"
```

## Configuração

```yaml
modules:
  subscriptionSnapshot:
    key_headers: [Authorization]   # headers do cliente que entram na chave; [] desliga
    operations: []                 # nomes de operação com snapshot; vazio = todas
    ttl: ""                        # descarta snapshots mais velhos; vazio = nunca
    max_entries: 10000             # limite LRU; 0 = padrão, -1 = sem limite
    max_event_bytes: 1048576       # maior evento SSE rastreado
```

Para isolar por tenant, por exemplo: `key_headers: [Authorization, X-Tenant-Id]`.

## Cuidados

- **Os `key_headers` precisam refletir quem pode ver os dados.** Se o subgraph devolve dados diferentes por usuário com base em algo que não está nesses headers (um cookie, por exemplo), inclua esse header na lista.
- **Cache em memória, por instância.** Cada réplica do router mantém o próprio cache, alimentado pela própria conexão upstream. Uma réplica nova se popula sozinha a partir da primeira mensagem que recebe.
- **Primeiro cliente.** Quem abre a subscription upstream não recebe snapshot, já que ainda não existe nenhum. O estado inicial desse cliente depende do subgraph emitir um valor ao abrir.
- **Mensagens só com erro** (sem `data`) não substituem o último estado válido.
- **Snapshot só passa por quem pode receber.** Clientes com `Authorization` diferente compartilham o mesmo stream upstream (se o router não repassa o header ao subgraph), mas não recebem o snapshot uns dos outros.

## Testes

```bash
go test ./...      # unitários
./e2e/run.sh       # ponta a ponta: router real + subgraph SSE + clientes (requer Node.js para npx wgc)
```

O teste de ponta a ponta sobe um subgraph de exemplo que atualiza um preço a cada 4s e verifica que:
- um cliente que entra no meio recebe o valor atual em milissegundos, tanto via SSE quanto via multipart;
- um cliente com outro `Authorization` não recebe o snapshot, mesmo compartilhando o mesmo stream upstream;
- depois que todos saem e o upstream fecha, não sobra snapshot velho.

Testado com `github.com/wundergraph/cosmo/router` na versão `router@0.362.0`. Como o módulo é compilado junto com o router e depende de como a engine lê o stream SSE, rode os testes a cada upgrade do router.
