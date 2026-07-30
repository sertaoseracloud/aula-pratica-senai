# Laboratório 4 — DynamoDB / ScyllaDB: até onde vai o "sempre disponível"

**Duração: ~8 minutos** (321 s subindo o anel + 161 s de testes).

## O que você vai fazer

Subir três nós ScyllaDB com o módulo **Alternator**, que expõe a API do DynamoDB. Você vai usar o `aws` CLI exatamente como usaria contra a AWS de verdade — mesmos comandos, mesmos parâmetros — só apontando para outro endpoint.

Depois vai atrasar a rede e derrubar nós, medindo onde o "sempre disponível" do DynamoDB para de valer.

## Por que não usar o DynamoDB Local

O emulador oficial da AWS roda como processo único. Ele responde os mesmos contratos JSON, mas não tem anel de replicação, não tem quórum, não tem nó para derrubar. Serve para testar código; não serve para estudar comportamento distribuído.

O ScyllaDB com Alternator dá as duas coisas: a mesma API e um cluster real de três nós.

## O que você vai descobrir

O DynamoDB tem fama de "PA/EL" — sempre disponível, consistência eventual por padrão. O laboratório mostra que isso é verdade **até certo ponto**, e o ponto exato é o quórum:

| Nós vivos | Escrita | Leitura forte | Leitura eventual | Quadrante |
| --- | --- | --- | --- | --- |
| 2 de 3 | OK | OK | OK | **PA** |
| 1 de 3 | **Recusada** | **Recusada** | OK | **PC** |

Perder a minoria não afeta nada. Perder o quórum para tudo, menos a leitura eventual.

---

## Antes de começar

| O que | Versão testada |
| --- | --- |
| Docker Engine | 28.4.0 |
| Docker Compose | v2.39.2 |
| ScyllaDB | 5.2.0 |
| AWS CLI (dentro do contêiner) | 2.36.11 |

Todos os comandos podem ser repetidos quantas vezes você quiser sem dar erro. Isso exige proteção em três pontos — criação de tabela, desconexão de rede e injeção de caos — e cada um está sinalizado no texto.

**Se você usa Git Bash no Windows:** prefixe todo comando `docker` que tenha caminho absoluto com `MSYS_NO_PATHCONV=1`. Sem isso o Pumba falha com `mkdir C:\Program Files\Git\var: Acesso negado`. PowerShell, WSL, Linux e macOS não precisam.

---

## Passo 1 — Criar o docker-compose.yml

A topologia é de três nós de banco mais um contêiner cliente ocioso (*bastion host*), que fica de pé só para você executar comandos de dentro da rede do cluster.

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

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

Três decisões merecem explicação.

### `--alternator-write-isolation only_rmw_uses_lwt`

Sem alguma variante dessa flag o Alternator **não inicializa**.

O valor escolhido é o que mais se aproxima do DynamoDB real: só operações *read-modify-write* (escrita condicional, `UpdateItem` com `ConditionExpression`) pagam o custo de consenso via Paxos. Escritas simples usam quórum comum, que é mais barato.

O Teste T4 mostra os dois níveis de consistência distintos que isso produz.

> **Vale experimentar depois:** troque para `always` (alias de `always_use_lwt`), que força **toda** escrita a passar por Paxos, e repita o Teste T4. Você vai ver o nível de consistência mudar de `LOCAL_QUORUM` para `LOCAL_SERIAL` até num `PutItem` trivial. É mais caro e mais distante do DynamoDB, mas mostra bem o efeito da flag.

### `--seeds=scylla-node1`

Só o primeiro nó é seed. Listar um nó que ainda não subiu como seed atrasa a convergência do Gossip sem ganho.

### Healthcheck com `hostname -i`

O Alternator **não escuta em `127.0.0.1`** — só no IP do contêiner. Um healthcheck contra `localhost:8000` retorna `http_code=000` mesmo com o serviço perfeitamente saudável. Por isso:

```yaml
test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES;' && curl -sf http://$$(hostname -i):8000 -o /dev/null"]
```

O teste valida as duas coisas: o motor CQL e a porta do Alternator.

## Passo 2 — Subir o anel

```bash
docker compose up -d --wait
```

**Isso leva cerca de 5 minutos** (321 s medidos). Os nós sobem em série, cada um esperando o anterior ficar saudável. É lento de propósito, não é travamento.

**Não comece a medir quando o comando voltar.** O `--wait` garante que os healthchecks passaram, mas um nó pode estar em `UJ` (entrando no anel) e ainda assim passar. Espere os três `UN`:

```bash
docker exec scylla-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel OK: 3 nos UN"; exit 0; }
    sleep 5
  done
  echo "TIMEOUT: anel incompleto"; nodetool status; exit 1'
```

Para inspecionar o anel a qualquer momento:

```bash
docker exec scylla-node1 nodetool status
```

## Passo 3 — Criar a tabela e gravar um item

Entre no bastion:

```bash
docker exec -it aws-client bash
```

Daqui em diante os comandos são `aws` puro, apontando para o nó coordenador.

**Criação idempotente.** Rodar `create-table` duas vezes falha com `ResourceInUseException: Table Pagamentos already exists`. A proteção com `describe-table` deixa o passo repetível:

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

O `put-item` já é idempotente por natureza (sobrescreve a chave):

```bash
aws dynamodb put-item \
    --table-name Pagamentos \
    --item '{"Id": {"S": "txn_001"}, "Status": {"S": "Processado"}}' \
    --endpoint-url http://scylla-node1:8000
```

### Confirme o fator de replicação

Vale conferir, porque é ele que define o quórum que sustenta os dois testes seguintes:

```bash
docker exec scylla-node1 cqlsh -e 'DESCRIBE KEYSPACE "alternator_Pagamentos";' | head -1
```

```
CREATE KEYSPACE "alternator_Pagamentos" WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'} AND durable_writes = true;
```

**RF = 3 em 3 nós.** Todo nó guarda cópia de tudo, e o quórum é 2. Guarde esse número — ele explica todo o resto.

---

## Teste T3 — Quanto custa a leitura forte (eixo ELC)

### Primeiro: por que atrasar um nó só não funciona

O caminho intuitivo seria degradar o `scylla-node2` e ver a leitura forte ficar lenta. **Não produz efeito nenhum.** Medido, com 2000 ms injetados só no `node2`:

| Modo | Latência |
| --- | --- |
| Leitura forte (`--consistent-read`) | 12 ms |
| Leitura eventual | 12 ms |

O motivo é o RF=3. A leitura forte usa `LOCAL_QUORUM`, que precisa de 2 das 3 réplicas. O coordenador `node1` é ele mesmo uma réplica e responde na hora; o `node3` está saudável e fecha o quórum. **O nó lento simplesmente não é usado.**

Para forçar o quórum a esperar, é preciso degradar **dois** nós.

### Injetar a latência

Num segundo terminal, no host:

```bash
# Proteção: remove execução anterior, se existir
docker rm -f pumba-elc 2>/dev/null

MSYS_NO_PATHCONV=1 docker run -d --name pumba-elc --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba netem --duration 3m delay --time 2000 \
  scylla-node2 scylla-node3
```

Confirme que pegou — o `node2` deve responder em segundos, o `node1` em milissegundos:

```bash
docker exec aws-client bash -c '
  time curl -s -o /dev/null --max-time 20 http://scylla-node2:8000
  time curl -s -o /dev/null http://scylla-node1:8000'
```

### Segundo: por que medir com comando avulso não funciona

Este é o achado menos óbvio do laboratório.

O ScyllaDB usa um *dynamic snitch*: ele pontua as réplicas pela latência observada e manda as leituras eventuais (`LOCAL_ONE`) para a mais rápida. Só que esse aprendizado precisa de requisições seguidas para acontecer.

Medindo com **1 segundo de intervalo** entre chamadas, a diferença desaparece:

| Iteração | Eventual | Forte |
| --- | --- | --- |
| 1 | 2020 ms | 2029 ms |
| 5 | 2026 ms | 2020 ms |
| 10 | 2019 ms | 2024 ms |

Com o snitch "frio", o coordenador volta a sondar as réplicas lentas e a leitura eventual paga o mesmo preço da forte. **Ou seja: rodar um `aws get-item` avulso, como manda a intuição, não demonstra nada.**

Some a isso que o AWS CLI v2 gasta **~1000 ms só para iniciar** (é Python), contra ~12 ms da operação real. Um `time aws dynamodb get-item` mede principalmente o interpretador.

### O script correto

Ele mede em rajada, com aquecimento, e fala HTTP direto para não pagar o startup do CLI:

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

Rode uma vez **sem** o Pumba (controle) e outra **com** ele ligado.

### O que você vai ver

Sob caos:

```
  EL  (eventual, CL=LOCAL_ONE) .... mediana 15ms
  EC  (forte,   CL=LOCAL_QUORUM) .. mediana 2012ms
```

Sem caos:

```
  EL  (eventual, CL=LOCAL_ONE) .... mediana 12ms
  EC  (forte,   CL=LOCAL_QUORUM) .. mediana 13ms
```

**Como ler isso.** Sem problema na rede, leitura forte e eventual custam o mesmo (13 ms contra 12 ms) — consistência forte não é cara por natureza. Com dois nós degradados, a leitura forte precisa esperar uma réplica lenta e passa a custar **~134× mais**.

O preço da consistência não é fixo: é função da saúde da rede.

Encerre o caos:

```bash
docker stop pumba-elc 2>/dev/null
sleep 30
```

> Os 30 segundos evitam que a próxima medição herde este atraso.

---

## Teste T4 — Até onde vai a disponibilidade (eixo PAC)

Desconectar e reconectar rede **não é idempotente**: repetir `disconnect` dá `is not connected to network`, e repetir `connect` dá `endpoint with name scylla-node3 already exists`. Use funções com proteção:

```bash
net_out() { docker network disconnect pacelc-dynamo-network "$1" 2>/dev/null; echo "  $1 fora da rede"; }
net_in()  { docker network connect    pacelc-dynamo-network "$1" 2>/dev/null; echo "  $1 de volta na rede"; }
```

### T4a — Um nó fora: o quórum aguenta

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

**A escrita passa normalmente.** E é importante entender o motivo certo: não é que o banco guardou localmente para reconciliar depois. Com RF=3 o quórum é 2, e os dois nós restantes atendem. É uma escrita de quórum legítima, totalmente durável.

Este é o comportamento **PA** que o DynamoDB real exibe: perder uma minoria de réplicas não interrompe o serviço.

### T4b — Dois nós fora: o quórum quebra

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

Agora repita com uma escrita **condicional**, que é read-modify-write e por isso usa Paxos:

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

Repare que o nível mudou de `LOCAL_QUORUM` para `LOCAL_SERIAL`. Essa é a assinatura do Paxos, acionado só pela operação condicional. As duas falham, mas por caminhos diferentes.

Por fim, as duas leituras sob a mesma partição:

```bash
# Eventual — funciona
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

### A matriz completa

| Operação | Nível usado | 3 nós | 2 nós | 1 nó |
| --- | --- | --- | --- | --- |
| `PutItem` simples | `LOCAL_QUORUM` | OK | OK | **Recusada** |
| `UpdateItem` condicional | `LOCAL_SERIAL` (Paxos) | OK | OK | **Recusada** |
| `GetItem --consistent-read` | `LOCAL_QUORUM` | OK | OK | **Recusada** |
| `GetItem` (eventual) | `LOCAL_ONE` | OK | OK | **OK** |

**Com falha minoritária (1 de 3), o sistema é PA.** Nada se degrada — exatamente como o DynamoDB gerenciado. É para esse regime que serviços de nuvem são dimensionados, e é por isso que o rótulo "PA" se sustenta na prática.

**Com falha majoritária (2 de 3), o quórum quebra e o sistema vira PC.** Escritas e leituras fortes são recusadas para não admitir divergência. Sobra só a leitura eventual, que responde de uma réplica só e pode devolver dado velho.

Vale notar o que **não** muda o resultado: trocar a flag para `always` move as escritas simples de `LOCAL_QUORUM` para `LOCAL_SERIAL`, mas não muda a disponibilidade. O custo sobe, a fronteira do quórum fica no mesmo lugar. **Nenhum ajuste de isolamento torna a escrita disponível sem quórum** — no Alternator o caminho de escrita é sempre baseado em quórum.

### Restaurar

```bash
net_in scylla-node2 ; net_in scylla-node3 ; sleep 20

docker exec scylla-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel restaurado: 3 UN"; exit 0; }
    sleep 5
  done; exit 1'
```

Os contêineres voltam com os mesmos IPs.

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir o anel (`up -d --wait`) | 321 s |
| T1 — esperar o anel fechar | 2 s |
| T2 — schema + carga | 3 s |
| T3 — eixo ELC | 76 s |
| T4 — eixo PAC + restauração | 80 s |
| **Total** | **~8 min** |

### Resumo das medições

| Cenário | EL (eventual) | EC (forte) | Razão |
| --- | --- | --- | --- |
| Rede saudável | 12 ms | 13 ms | 1,1× |
| Delay 2000 ms em **1** nó | 12 ms | 12 ms | 1,0× (sem efeito) |
| Delay 2000 ms em **2** nós | 15 ms | 2012 ms | **134×** |
| Delay 2000 ms, chamadas espaçadas em 1 s | 2019 ms | 2024 ms | 1,0× (snitch frio) |

Custos de referência: AWS CLI v2 **~1001 ms** por invocação; operação Alternator via HTTP direto **~12 ms**.

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| Alternator não inicializa | Flag `--alternator-write-isolation` ausente ou inválida | Usar `only_rmw_uses_lwt` (ou `always`) |
| Healthcheck falha com `http_code=000` | Alternator não escuta em `127.0.0.1` | Sondar `http://$(hostname -i):8000` |
| `ResourceInUseException` ao repetir | `create-table` não é idempotente | Proteger com `describe-table \|\| create-table` |
| `is not connected to network` | `disconnect` não é idempotente | Sufixar `2>/dev/null` |
| `endpoint with name X already exists` | `connect` não é idempotente | Sufixar `2>/dev/null` |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash converte `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |
| Nome de contêiner Pumba em conflito | Execução anterior não removida | `docker rm -f pumba-elc 2>/dev/null` antes |
| Leitura forte não fica lenta | Só um nó degradado; quórum fechado pelos saudáveis | Degradar **dois** nós |
| EL e EC igualmente lentas | Snitch frio por chamadas espaçadas | Medir em rajada, com aquecimento |
| Latências de ~1000 ms sem caos | Startup do AWS CLI v2 | Medir via HTTP direto (`curl`) |
| Nó em `UJ` depois do `--wait` | Healthcheck não garante ingresso no anel | Esperar 3× `UN` |
| Resultado de caos contaminado | `netem` não some na hora | Esperar ~30 s entre experimentos |

---

## O que levar disso para o trabalho

O bastion host aproxima o laboratório de uma arquitetura de produção, mas o experimento mostra que a purificação é parcial: o custo dominante da medição virou o startup do próprio AWS CLI, não a infraestrutura. Medir honestamente exigiu descer ao HTTP.

Três resultados desmontam intuições comuns:

**1. Consistência forte não é cara por natureza.** Em rede saudável, EC custou o mesmo que EL (13 ms contra 12 ms). O preço é função da degradação da rede, e sob estresse a função é abrupta: salta para 134×. Isso significa que o custo não aparece em teste de carga — aparece no incidente.

**2. O quadrante depende da severidade da falha, não só do produto.** O mesmo cluster é PA ao perder uma réplica e PC ao perder o quórum. Chamar o ScyllaDB de "banco AP" sem qualificar o modo de falha e a configuração é impreciso.

**3. A topologia decide o que o experimento consegue enxergar.** Com RF=3 em 3 nós, degradar um único nó não produz sinal nenhum, porque o quórum se forma sem ele. Um experimento mal dimensionado devolve a conclusão errada com toda a aparência de rigor — e foi exatamente isso que aconteceu na primeira versão deste laboratório.

Para o projeto de sistemas escaláveis, a recomendação é adotar consistência eventual como padrão e reservar a coerência forte às operações onde a divergência de estado causa dano irreversível — sabendo que, nessas operações, o sistema pode legitimamente se recusar a responder.

---

## Encerrar

```bash
docker rm -f pumba-elc 2>/dev/null
docker compose down -v
```
