# Tutorial Analítico: Validação do Teorema PACELC com a API do DynamoDB e Bastion Host Containerizado

Este roteiro estabelece um campo de testes laboratorial para o teorema PACELC sob a semântica do Amazon DynamoDB. Emuladores locais fornecidos por provedores de nuvem frequentemente mascaram a física dos sistemas distribuídos por operarem sobre arquiteturas monolíticas embutidas. Para tensionar de fato os eixos do teorema, este laboratório emprega o motor ScyllaDB equipado com o módulo Alternator, que traduz os contratos JSON nativos da AWS para um anel de replicação real de três nós.

A arquitetura inclui um contêiner cliente persistente (bastion host) em modo interativo, eliminando o custo de `docker run` a cada comando e aproximando o arranjo de uma topologia de produção, onde instâncias dedicadas sustentam sessões abertas contra o banco.

> **Todos os números, mensagens de erro e comportamentos descritos abaixo foram obtidos executando este laboratório de ponta a ponta.** As seções [Resultados medidos](#resultados-medidos) e [Erros e armadilhas](#erros-e-armadilhas-verificados-em-execução) registram o que a execução real produziu — inclusive onde a intuição falha.

---

## Pré-requisitos e garantia de reexecução

Todos os comandos deste tutorial são **idempotentes**: podem ser reexecutados quantas vezes for necessário, na ordem apresentada, sem produzir erro. Isso exige guardas explícitas em três pontos (criação de tabela, desconexão de rede e injeção de caos), detalhadas ao longo do texto.

| Item | Versão validada |
| --- | --- |
| Docker Engine | 28.4.0 |
| Docker Compose | v2.39.2 |
| ScyllaDB | 5.2.0 |
| AWS CLI (no bastion) | 2.36.11 |

**Usuários de Git Bash no Windows:** o Git Bash reescreve caminhos POSIX para caminhos Windows, o que quebra a montagem `/var/run/docker.sock` exigida pelo Pumba. Prefixe **todo** comando `docker` que envolva caminhos absolutos com `MSYS_NO_PATHCONV=1`. Sem isso, o Pumba falha com `mkdir C:\Program Files\Git\var: Acesso negado`. PowerShell, WSL, Linux e macOS não precisam desse prefixo.

---

## Fase 1: Estruturação da Topologia com Cliente Persistente

A orquestração provisiona três nós de armazenamento e um nó cliente ocioso. O serviço Alternator expõe a porta 8000 nos nós do banco. O contêiner cliente tem seu ponto de entrada subvertido para manter a sessão ativa indefinidamente.

> O `docker-compose.yml` é ignorado pelo Git (ver [.gitignore](../../.gitignore)). O conteúdo íntegro está reproduzido abaixo — copie-o para `docker-compose.yml` neste diretório.

```yaml
services:
  scylla-node1:
    image: scylladb/scylla:5.2.0
    container_name: scylla-node1
    command: --seeds=scylla-node1 --smp 1 --memory 1G --overprovisioned 1 --alternator-port 8000 --alternator-write-isolation only_rmw_uses_lwt
    networks:
      - dynamo-ring
    healthcheck:
      # Valida o motor CQL E o endpoint Alternator. O Alternator NAO faz bind em
      # 127.0.0.1: escuta apenas no IP do container, dai o `hostname -i`.
      test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES;' && curl -sf http://$$(hostname -i):8000 -o /dev/null"]
      interval: 10s
      timeout: 10s
      retries: 30
      start_period: 30s

  scylla-node2:
    image: scylladb/scylla:5.2.0
    container_name: scylla-node2
    command: --seeds=scylla-node1 --smp 1 --memory 1G --overprovisioned 1 --alternator-port 8000 --alternator-write-isolation only_rmw_uses_lwt
    networks:
      - dynamo-ring
    depends_on:
      scylla-node1:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES;' && curl -sf http://$$(hostname -i):8000 -o /dev/null"]
      interval: 10s
      timeout: 10s
      retries: 30
      start_period: 30s

  scylla-node3:
    image: scylladb/scylla:5.2.0
    container_name: scylla-node3
    command: --seeds=scylla-node1 --smp 1 --memory 1G --overprovisioned 1 --alternator-port 8000 --alternator-write-isolation only_rmw_uses_lwt
    networks:
      - dynamo-ring
    depends_on:
      scylla-node2:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES;' && curl -sf http://$$(hostname -i):8000 -o /dev/null"]
      interval: 10s
      timeout: 10s
      retries: 30
      start_period: 30s

  aws-client:
    image: amazon/aws-cli
    container_name: aws-client
    entrypoint: /bin/bash
    command: -c "tail -f /dev/null"
    environment:
      AWS_ACCESS_KEY_ID: dummy
      AWS_SECRET_ACCESS_KEY: dummy
      AWS_DEFAULT_REGION: us-east-1
    networks:
      - dynamo-ring
    depends_on:
      scylla-node3:
        condition: service_healthy

networks:
  dynamo-ring:
    name: pacelc-dynamo-network
    driver: bridge
```

Três decisões nele merecem justificativa:

**`--alternator-write-isolation only_rmw_uses_lwt`** — sem alguma variante desta flag o serviço Alternator não inicializa. O valor escolhido é o que **mais se aproxima da semântica do DynamoDB real**: apenas operações read-modify-write (escritas condicionais, `UpdateItem` com `ConditionExpression`) pagam o custo do consenso Paxos; escritas simples usam quórum comum. A Fase 4 mostra os dois níveis de consistência distintos que essa escolha produz.

> **Variante `always`.** O alias `always` (de `always_use_lwt`) força **toda** escrita a passar por Paxos. Funciona, mas afasta o laboratório do comportamento do DynamoDB: até um `PutItem` trivial vira transação. Vale trocar a flag e repetir a Fase 4 para ver o nível de consistência mudar de `LOCAL_QUORUM` para `LOCAL_SERIAL` nas escritas simples.

**`--seeds=scylla-node1`** — apenas o primeiro nó é seed. Listar um nó ainda não inicializado como seed atrasa a convergência do Gossip sem benefício.

**Healthcheck com `hostname -i`** — o Alternator **não faz bind em `127.0.0.1`**; escuta somente no IP do contêiner. Um healthcheck contra `localhost:8000` falha com `http_code=000` mesmo com o serviço saudável. O healthcheck valida o motor CQL *e* a porta Alternator:

```yaml
test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES;' && curl -sf http://$$(hostname -i):8000 -o /dev/null"]
```

### Inicialização

```bash
docker compose up -d --wait
```

A flag `--wait` bloqueia até que todos os healthchecks passem. Como os nós sobem em série (cada um aguarda o anterior ficar saudável), **o bootstrap completo leva cerca de 5 minutos** (321 s medidos). Isso é esperado, não um travamento.

### Portão de verificação do anel

`--wait` garante que os serviços respondem, mas **não** que os três nós já ingressaram no anel — um nó pode estar em estado `UJ` (Up/Joining) e ainda assim responder aos healthchecks. Antes de prosseguir, aguarde os três `UN` (Up/Normal):

```bash
docker exec scylla-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel OK: 3 nos UN"; exit 0; }
    sleep 5
  done
  echo "TIMEOUT: anel incompleto"; nodetool status; exit 1'
```

Saída esperada:

```
anel OK: 3 nos UN
```

Para inspecionar o anel manualmente:

```bash
docker exec scylla-node1 nodetool status
```

---

## Fase 2: Esquema e Carga de Calibração

Assuma o controle do bastion. A partir daqui, os comandos operam nativamente contra a rede do banco:

```bash
docker exec -it aws-client bash
```

### Criação idempotente da tabela

Executar `create-table` duas vezes falha com `ResourceInUseException: Table Pagamentos already exists`. A guarda com `describe-table` torna o passo reexecutável:

```bash
aws dynamodb describe-table --table-name Pagamentos \
    --endpoint-url http://scylla-node1:8000 >/dev/null 2>&1 ||
aws dynamodb create-table \
    --table-name Pagamentos \
    --attribute-definitions AttributeName=Id,AttributeType=S \
    --key-schema AttributeName=Id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url http://scylla-node1:8000
```

`put-item` é naturalmente idempotente (sobrescreve a chave):

```bash
aws dynamodb put-item \
    --table-name Pagamentos \
    --item '{"Id": {"S": "txn_001"}, "Status": {"S": "Processado"}}' \
    --endpoint-url http://scylla-node1:8000
```

### Confirmação do fator de replicação

O Alternator cria um keyspace próprio. Vale confirmar o RF, pois ele define o quórum que sustenta as Fases 3 e 4:

```bash
docker exec scylla-node1 cqlsh -e 'DESCRIBE KEYSPACE "alternator_Pagamentos";' | head -1
```

```
CREATE KEYSPACE "alternator_Pagamentos" WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'} AND durable_writes = true;
```

**RF = 3 em 3 nós.** Todo nó é réplica de toda partição, e o quórum é 2. Esse único fato explica tudo o que se segue.

---

## Fase 3: Tensionar o eixo ELC (Else: Latency vs. Consistency)

### Por que atrasar um só nó não funciona

A abordagem intuitiva — degradar `scylla-node2` e observar a leitura forte ficar lenta — **não produz efeito algum**. Medição real com 2000 ms injetados apenas no `node2`:

| Modo | Latência observada |
| --- | --- |
| Leitura forte (`--consistent-read`) | 12 ms |
| Leitura eventual | 12 ms |

O motivo é o RF = 3. A leitura forte usa `LOCAL_QUORUM`, que exige 2 das 3 réplicas. O coordenador `node1` é ele próprio uma réplica e responde instantaneamente; `node3` está saudável e completa o quórum. **O nó degradado nunca entra no caminho crítico.** Para forçar o quórum a esperar, é preciso degradar **dois** nós.

### Injeção de caos

Em um segundo terminal no hospedeiro:

```bash
# Guarda de idempotência: remove execução anterior, se existir
docker rm -f pumba-elc 2>/dev/null

MSYS_NO_PATHCONV=1 docker run -d --name pumba-elc --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba netem --duration 3m delay --time 2000 \
  scylla-node2 scylla-node3
```

Confirme que o atraso está ativo (o `node2` deve levar segundos; o `node1`, milissegundos):

```bash
docker exec aws-client bash -c '
  time curl -s -o /dev/null --max-time 20 http://scylla-node2:8000
  time curl -s -o /dev/null http://scylla-node1:8000'
```

### O contraste ELC só emerge sob carga contínua

Este é o achado menos intuitivo do laboratório e **invalida a medição por comando avulso**.

O ScyllaDB usa um *dynamic snitch*, que pontua réplicas pela latência observada e roteia leituras `LOCAL_ONE` (eventuais) para a mais rápida. Esse aprendizado exige requisições sucessivas. Medindo com **1 segundo de intervalo** entre chamadas, o contraste desaparece por completo:

| Iteração | Eventual | Forte |
| --- | --- | --- |
| 1 | 2020 ms | 2029 ms |
| 5 | 2026 ms | 2020 ms |
| 10 | 2019 ms | 2024 ms |

Com o snitch "frio", o coordenador volta a sondar as réplicas degradadas e a leitura eventual paga o mesmo preço da forte.

Portanto, meça **em rajada e com aquecimento**. Copie o script abaixo para o bastion:

```bash
cat > /tmp/medir-elc.sh <<'EOF'
#!/bin/bash
EP=http://scylla-node1:8000
N=${1:-20}
hit() {
  curl -s -o /dev/null -X POST $EP \
    -H "X-Amz-Target: DynamoDB_20120810.GetItem" \
    -H "Content-Type: application/x-amz-json-1.0" \
    -H "Authorization: AWS4-HMAC-SHA256 Credential=dummy/20260730/us-east-1/dynamodb/aws4_request,SignedHeaders=host,Signature=x" \
    -d "$1"
}
EV='{"TableName":"Pagamentos","Key":{"Id":{"S":"txn_001"}}}'
CO='{"TableName":"Pagamentos","Key":{"Id":{"S":"txn_001"}},"ConsistentRead":true}'
mediana() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}'; }

# aquecimento: deixa o dynamic snitch pontuar as replicas
for i in $(seq 1 10); do hit "$EV" >/dev/null; hit "$CO" >/dev/null; done

evs=(); cos=()
for i in $(seq 1 "$N"); do
  S=$(date +%s%N); hit "$EV"; E=$(date +%s%N); evs+=( $(( (E-S)/1000000 )) )
  S=$(date +%s%N); hit "$CO"; E=$(date +%s%N); cos+=( $(( (E-S)/1000000 )) )
done
echo "  amostras: $N (apos 10 de aquecimento)"
echo "  EL  (eventual, CL=LOCAL_ONE) .... mediana $(mediana "${evs[@]}")ms"
echo "  EC  (forte,   CL=LOCAL_QUORUM) .. mediana $(mediana "${cos[@]}")ms"
EOF
bash /tmp/medir-elc.sh 20
```

Resultado sob caos:

```
  amostras: 20 (apos 10 de aquecimento)
  EL  (eventual, CL=LOCAL_ONE) .... mediana 15ms
  EC  (forte,   CL=LOCAL_QUORUM) .. mediana 2012ms
```

E o mesmo script **sem** caos, como controle:

```
  EL  (eventual, CL=LOCAL_ONE) .... mediana 12ms
  EC  (forte,   CL=LOCAL_QUORUM) .. mediana 13ms
```

**Leitura do resultado.** Sem assimetria de latência na malha, EL e EC custam o mesmo (12 ms vs. 13 ms) — consistência forte não é intrinsecamente cara. Sob degradação, a leitura forte precisa equalizar o estado com uma réplica lenta e paga **~134× mais** (2012 ms vs. 15 ms). O custo da consistência não é um valor fixo: é uma função da saúde da malha.

Encerre o caos:

```bash
docker stop pumba-elc 2>/dev/null
```

### Por que não medir com `aws` avulso

O AWS CLI v2 leva **~1000 ms apenas para inicializar** (interpretador Python), enquanto a operação real no banco custa ~12 ms. Medição isolada:

```
cli-startup-only: 1001ms
5 leituras quorum via HTTP puro: 50ms total
```

O bastion host elimina o custo do `docker run`, mas **não** elimina o startup do CLI. Um `time aws dynamodb get-item` mede predominantemente Python, não o banco. Por isso o script acima usa HTTP direto via `curl`.

---

## Fase 4: Problematizar o eixo PAC sob ruptura sistêmica

As desconexões de rede **não são idempotentes**: repetir `disconnect` falha com `is not connected to network` e repetir `connect` falha com `endpoint with name scylla-node3 already exists`. Use guardas:

```bash
net_out() { docker network disconnect pacelc-dynamo-network "$1" 2>/dev/null; echo "  $1 fora da rede"; }
net_in()  { docker network connect    pacelc-dynamo-network "$1" 2>/dev/null; echo "  $1 de volta na rede"; }
```

### 4a — Um nó fora: o quórum se mantém

```bash
net_out scylla-node3
sleep 5
```

No bastion:

```bash
aws dynamodb put-item \
    --table-name Pagamentos \
    --item '{"Id": {"S": "txn_002"}, "Status": {"S": "Pendente"}}' \
    --endpoint-url http://scylla-node1:8000
```

A escrita **conclui normalmente**. O motivo, porém, não é tolerância a falhas por retenção local: com RF = 3, o quórum é 2, e os dois nós restantes ainda o satisfazem. É uma escrita de quórum legítima, plenamente durável — não um enfileiramento para reconciliação posterior.

Este é o comportamento **PA** que o DynamoDB real exibe: perder uma minoria de réplicas não interrompe o serviço. Todas as quatro operações permanecem disponíveis.

### 4b — Dois nós fora: o quórum se rompe

Este é o teste decisivo, e é ele que separa a hipótese "PA" da realidade:

```bash
net_out scylla-node2
sleep 8
```

No bastion:

```bash
aws dynamodb put-item \
    --table-name Pagamentos \
    --item '{"Id": {"S": "txn_003"}, "Status": {"S": "Falha"}}' \
    --endpoint-url http://scylla-node1:8000
```

A escrita é **recusada**:

```
An error occurred (InternalServerError) when calling the PutItem operation:
exceptions::unavailable_exception (Cannot achieve consistency level for cl LOCAL_QUORUM. Requires 2, alive 1)
```

Repita agora com uma escrita **condicional**, que é read-modify-write e por isso usa Paxos:

```bash
aws dynamodb update-item \
    --table-name Pagamentos \
    --key '{"Id": {"S": "txn_001"}}' \
    --update-expression 'SET #s = :v' \
    --expression-attribute-names '{"#s":"Status"}' \
    --expression-attribute-values '{":v":{"S":"rmw"}}' \
    --condition-expression 'attribute_exists(Id)' \
    --endpoint-url http://scylla-node1:8000
```

```
Cannot achieve consistency level for cl LOCAL_SERIAL. Requires 2, alive 1
```

O nível de consistência muda de `LOCAL_QUORUM` para `LOCAL_SERIAL` — é a assinatura do Paxos, acionado só pela operação condicional. Ambas falham, mas por caminhos diferentes.

Agora as duas leituras, sob a mesma partição:

```bash
# Eventual — sobrevive
aws dynamodb get-item --table-name Pagamentos \
    --key '{"Id": {"S": "txn_001"}}' \
    --endpoint-url http://scylla-node1:8000

# Forte — falha
aws dynamodb get-item --table-name Pagamentos \
    --key '{"Id": {"S": "txn_001"}}' --consistent-read \
    --endpoint-url http://scylla-node1:8000
```

```
Cannot achieve consistency level for cl LOCAL_QUORUM. Requires 2, alive 1
```

### O que isso demonstra

Matriz completa medida (`only_rmw_uses_lwt`, RF = 3):

| Operação | Nível de consistência | 3 nós | 2 nós | 1 nó |
| --- | --- | --- | --- | --- |
| `PutItem` simples | `LOCAL_QUORUM` | OK | OK | **Recusada** |
| `UpdateItem` condicional (RMW) | `LOCAL_SERIAL` (Paxos) | OK | OK | **Recusada** |
| `GetItem --consistent-read` | `LOCAL_QUORUM` | OK | OK | **Recusada** |
| `GetItem` (eventual) | `LOCAL_ONE` | OK | OK | **OK** |

A leitura da tabela é a lição do laboratório:

**Contra falha minoritária (1 de 3), o sistema é PA**: nada se degrada, exatamente como o DynamoDB gerenciado. É este o regime para o qual serviços de nuvem são dimensionados, e é por isso que a rótulo "PA" se sustenta na prática.

**Contra falha majoritária (2 de 3), o quórum se rompe e o sistema passa a PC**: escritas e leituras fortes são recusadas para não admitir divergência de estado. Sobra apenas a leitura eventual (`LOCAL_ONE`), que responde de uma única réplica e pode devolver dado obsoleto.

A conclusão relevante para arquitetura é que **o quadrante não é uma propriedade do produto, e sim da configuração e da severidade da falha**. Trocar a flag para `always` move as escritas simples de `LOCAL_QUORUM` para `LOCAL_SERIAL` sem alterar a disponibilidade — o custo sobe, a fronteira do quórum não se move. E nenhum ajuste de isolamento torna a escrita disponível sob perda de quórum: no Alternator o caminho de escrita é sempre quorum-based.

### Restauração

```bash
net_in scylla-node2
net_in scylla-node3
sleep 20
docker exec scylla-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel restaurado: 3 UN"; exit 0; }
    sleep 5
  done; exit 1'
```

O anel reconverge e os contêineres preservam seus endereços IP originais.

---

## Resultados medidos

Consolidado de uma execução completa em cluster recém-criado:

| Cenário | EL (eventual) | EC (forte) | Razão |
| --- | --- | --- | --- |
| Malha saudável | 12 ms | 13 ms | 1,1× |
| Delay 2000 ms em **1** nó | 12 ms | 12 ms | 1,0× (sem efeito) |
| Delay 2000 ms em **2** nós | 15 ms | 2012 ms | **134×** |
| Delay 2000 ms, requisições espaçadas em 1 s | 2019 ms | 2024 ms | 1,0× (snitch frio) |

| Nós vivos | `PutItem` | `UpdateItem` cond. | Leitura forte | Leitura eventual | Quadrante |
| --- | --- | --- | --- | --- | --- |
| 3 | OK | OK | OK | OK | — |
| 2 | OK | OK | OK | OK | **PA** |
| 1 | **Recusada** | **Recusada** | **Recusada** | OK | **PC** |

Tempos de execução medidos:

| Etapa | Tempo |
| --- | --- |
| Bootstrap do cluster (`up -d --wait`) | 321 s |
| T1 — portão do anel | 2 s |
| T2 — schema + carga | 3 s |
| T3 — eixo ELC | 76 s |
| T4 — eixo PAC + restauração | 80 s |
| **Total (bootstrap + testes)** | **~8 min** |

Custos de referência: startup do AWS CLI v2 **~1001 ms** por invocação; operação Alternator via HTTP direto **~12 ms**.

---

## Erros e armadilhas verificados em execução

| Sintoma | Causa | Correção |
| --- | --- | --- |
| Alternator não inicializa | Flag `--alternator-write-isolation` ausente ou inválida | Usar `only_rmw_uses_lwt` (ou `always`) |
| Resultado de caos contaminado | `netem` não é removido instantaneamente ao parar o Pumba | Aguardar ~30 s entre experimentos de caos |
| Healthcheck falha com `http_code=000` | Alternator não faz bind em `127.0.0.1` | Sondar `http://$(hostname -i):8000` |
| `ResourceInUseException` ao repetir o tutorial | `create-table` não é idempotente | Guardar com `describe-table \|\| create-table` |
| `is not connected to network` | `disconnect` não é idempotente | Sufixar `2>/dev/null` |
| `endpoint with name X already exists` | `connect` não é idempotente | Sufixar `2>/dev/null` |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash reescreve `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |
| Nome de contêiner Pumba em conflito | Execução anterior não removida | `docker rm -f pumba-elc 2>/dev/null` antes |
| Leitura forte não fica lenta | Só um nó degradado; quórum satisfeito pelos saudáveis | Degradar **dois** nós |
| EL e EC igualmente lentas | Snitch frio por requisições espaçadas | Medir em rajada, após aquecimento |
| Latências ~1000 ms sem caos | Startup do AWS CLI v2 | Medir via HTTP direto (`curl`) |
| Nó em `UJ` após `--wait` | Healthcheck não implica ingresso no anel | Aguardar 3× `UN` em `nodetool status` |

---

## Fechamento Analítico

O isolamento do cliente em contêiner persistente aproxima o laboratório de uma arquitetura de produção, mas o experimento mostra que essa purificação é parcial: o custo dominante da medição passou a ser o startup do próprio AWS CLI, não a infraestrutura. Instrumentação honesta exigiu descer ao protocolo HTTP.

Três resultados desmontam intuições comuns sobre sistemas distribuídos:

1. **Consistência forte não é intrinsecamente cara.** Em malha saudável, EC custou o mesmo que EL (13 ms vs. 12 ms). O preço da consistência é uma função da degradação da rede, não uma taxa fixa — e sob estresse essa função é abrupta, saltando para 134×.

2. **O quadrante depende da severidade da falha, não só do produto.** O mesmo cluster é PA ao perder uma réplica minoritária e PC ao perder o quórum, quando recusa escritas para não admitir divergência. Chamar o ScyllaDB de "banco AP" sem qualificar o modo de falha e a configuração é impreciso.

3. **A topologia determina o que um experimento consegue observar.** Com RF = 3 em 3 nós, degradar um único nó não produz sinal algum, porque o quórum se forma sem ele. Um experimento mal dimensionado produz a conclusão errada com toda a aparência de rigor.

Para o projeto de software escalável, a lição é assimilar a consistência eventual como regra arquitetural primária e reservar a coerência absoluta às operações em que a divergência de estado comprometa o domínio de negócio de forma irreversível — sabendo que, nessas operações, o sistema pode legitimamente recusar-se a responder.

---

## Encerramento do laboratório

```bash
docker rm -f pumba-elc 2>/dev/null
docker compose down -v
```
