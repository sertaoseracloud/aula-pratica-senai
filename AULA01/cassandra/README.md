# Tutorial Analítico: Validação do Teorema PACELC no Apache Cassandra

Este roteiro constrói um laboratório local de três nós para tensionar empiricamente o teorema PACELC no Apache Cassandra. Diferente dos demais laboratórios deste repositório, aqui o quadrante não é fixado pela configuração do servidor: **o Cassandra expõe o nível de consistência como parâmetro de cada consulta**, de modo que a mesma topologia transita entre PA/EL e PC/EC conforme o `CONSISTENCY` declarado.

> Todos os números e mensagens de erro abaixo foram obtidos executando este laboratório de ponta a ponta. A seção [Resultados medidos](#resultados-medidos) consolida a evidência.

**Tempo total estimado: ~7 minutos** (bootstrap 251 s + testes ~130 s).

---

## Fase 1: Estruturação da Topologia

> O `docker-compose.yml` é ignorado pelo Git (ver [.gitignore](../../.gitignore)). Copie o conteúdo abaixo para `docker-compose.yml` neste diretório.

```yaml
services:
  cassandra-node1:
    image: cassandra:4.1
    container_name: cassandra-node1
    environment:
      - CASSANDRA_CLUSTER_NAME=PACELC_Cluster
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      - CASSANDRA_SEEDS=cassandra-node1
      - MAX_HEAP_SIZE=512M
      - HEAP_NEWSIZE=128M
    ports:
      - "9042:9042"
    networks:
      - cassandra-ring
    healthcheck:
      test: ["CMD-SHELL", "nodetool status | grep -q '^UN' && cqlsh -e 'DESCRIBE KEYSPACES;'"]
      interval: 15s
      timeout: 15s
      retries: 40
      start_period: 60s

  cassandra-node2:
    image: cassandra:4.1
    container_name: cassandra-node2
    environment:
      - CASSANDRA_CLUSTER_NAME=PACELC_Cluster
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      - CASSANDRA_SEEDS=cassandra-node1
      - MAX_HEAP_SIZE=512M
      - HEAP_NEWSIZE=128M
    networks:
      - cassandra-ring
    # Cassandra exige bootstrap serializado: dois nos ingressando ao mesmo tempo
    # colidem na negociacao de tokens. O gate service_healthy serializa o ingresso.
    depends_on:
      cassandra-node1:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "nodetool status | grep -q '^UN' && cqlsh -e 'DESCRIBE KEYSPACES;'"]
      interval: 15s
      timeout: 15s
      retries: 40
      start_period: 60s

  cassandra-node3:
    image: cassandra:4.1
    container_name: cassandra-node3
    environment:
      - CASSANDRA_CLUSTER_NAME=PACELC_Cluster
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      - CASSANDRA_SEEDS=cassandra-node1
      - MAX_HEAP_SIZE=512M
      - HEAP_NEWSIZE=128M
    networks:
      - cassandra-ring
    depends_on:
      cassandra-node2:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "nodetool status | grep -q '^UN' && cqlsh -e 'DESCRIBE KEYSPACES;'"]
      interval: 15s
      timeout: 15s
      retries: 40
      start_period: 60s

networks:
  cassandra-ring:
    name: pacelc-network
    driver: bridge
```

Três decisões merecem justificativa:

**`CASSANDRA_SEEDS=cassandra-node1`** — apenas o primeiro nó é seed. Declarar um nó ainda inexistente como seed atrasa a convergência do Gossip sem benefício.

**`depends_on: condition: service_healthy`** — o Cassandra exige **bootstrap serializado**. Dois nós ingressando simultaneamente colidem na negociação de tokens. Sem este portão, o cluster sobe de forma intermitente.

**`MAX_HEAP_SIZE=512M`** — o padrão dimensiona o heap pela RAM do host e três JVMs sem limite esgotam a memória de uma máquina de desenvolvimento.

### Inicialização

```bash
docker compose up -d --wait
```

O bootstrap serializado leva **cerca de 4 minutos** (251 s medidos). Confirme o anel antes de prosseguir — `--wait` garante que os serviços respondem, não que ingressaram no anel:

```bash
docker exec cassandra-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel OK: 3 nos UN"; exit 0; }
    sleep 5
  done
  echo "TIMEOUT"; nodetool status; exit 1'
```

## Fase 2: Modelo de Dados Replicado

O Fator de Replicação 3 é mandatório: força cópias em toda a topologia, o que torna os níveis de consistência distinguíveis. Os comandos usam `IF NOT EXISTS` para permitir reexecução:

```bash
docker exec cassandra-node1 cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS pacelc_lab WITH replication = {'class':'SimpleStrategy','replication_factor':3};
CREATE TABLE IF NOT EXISTS pacelc_lab.sensordata (id UUID PRIMARY KEY, status text);
INSERT INTO pacelc_lab.sensordata (id, status) VALUES (uuid(), 'baseline');
SELECT count(*) FROM pacelc_lab.sensordata;"
```

---

## Fase 3: Eixo PAC — Disponibilidade sob Partição

As desconexões de rede não são idempotentes. Defina guardas antes de começar:

```bash
net_out() { docker network disconnect pacelc-network "$1" 2>/dev/null; echo "  $1 fora da rede"; }
net_in()  { docker network connect    pacelc-network "$1" 2>/dev/null; echo "  $1 de volta na rede"; }
```

Uma sonda que exercita os três níveis de consistência na mesma partição:

```bash
probe() {
  for cl in ONE QUORUM ALL; do
    out=$(docker exec cassandra-node1 cqlsh -e "CONSISTENCY $cl; SELECT count(*) FROM pacelc_lab.sensordata;" 2>&1)
    echo "$out" | grep -qE "\(1 rows\)" && echo "  CONSISTENCY $cl ... OK" || echo "  CONSISTENCY $cl ... FALHOU"
  done
}
```

Execute a sonda em três estados de rede sucessivos:

```bash
probe                                    # 3 nós
net_out cassandra-node3 ; sleep 6  ; probe   # 2 nós
net_out cassandra-node2 ; sleep 10 ; probe   # 1 nó
```

### Resultado medido

| Nós vivos | `ONE` | `QUORUM` | `ALL` |
| --- | --- | --- | --- |
| 3 | OK | OK | OK |
| 2 | OK | OK | **Falha** |
| 1 | OK | **Falha** | **Falha** |

Esta tabela é o coração do laboratório. A **mesma topologia, no mesmo instante, é disponível ou indisponível conforme o nível exigido pela consulta**. Não existe "o quadrante do Cassandra": existe o quadrante de cada consulta.

As falhas se manifestam de duas formas distintas, e a diferença é informativa:

```
QUORUM com 1 nó vivo:
ReadTimeout: code=1200 [Coordinator node timed out waiting for replica nodes' responses]
message="Operation timed out - received only 1 responses."
info={'consistency': 'QUORUM', 'required_responses': 2, 'received_responses': 1}
```

```
ALL com 1 nó vivo:
NoHostAvailable: Unavailable('code=1000 [Unavailable exception] message="Cannot achieve consistency level ALL"
info={'consistency': 'ALL', 'required_replicas': 3, 'alive_replicas': 1}')
```

O `QUORUM` devolve **timeout**, não indisponibilidade: logo após o corte de rede o Gossip ainda considera os nós vivos, então o coordenador tenta e espera. O `ALL` devolve **Unavailable** imediato, porque já contabilizou `alive_replicas: 1`. Executar a mesma sonda um minuto depois muda a mensagem do `QUORUM` para `Unavailable` — a percepção da falha é assíncrona.

### Restauração

```bash
net_in cassandra-node2 ; net_in cassandra-node3 ; sleep 25
docker exec cassandra-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel restaurado: 3 UN"; exit 0; }
    sleep 5
  done; exit 1'
```

---

## Fase 4: Eixo ELC — Latência vs. Consistência

### Por que aqui um único nó degradado basta

No laboratório [ScyllaDB/Alternator](../dynamodb-scylladb/README.md) foi preciso degradar **dois** nós para produzir sinal, porque `LOCAL_QUORUM` (2 de 3) se satisfaz com as réplicas saudáveis. Aqui usamos `CONSISTENCY ALL`, que exige **as três** réplicas: qualquer nó degradado entra obrigatoriamente no caminho crítico. Um só nó basta.

### Injeção de caos

```bash
docker rm -f pumba-cass 2>/dev/null
MSYS_NO_PATHCONV=1 docker run -d --name pumba-cass --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba netem --duration 4m delay --time 2000 cassandra-node2
```

> `MSYS_NO_PATHCONV=1` é necessário apenas no Git Bash do Windows, que reescreve `/var/run/docker.sock` para um caminho Windows e faz o Pumba falhar com `mkdir C:\Program Files\Git\var: Acesso negado`.

### Medição com startup amortizado

O `cqlsh` é Python e custa **~800 ms só para abrir a sessão** — mais que a operação medida. Um `time cqlsh -e "INSERT ..."` mede predominantemente o interpretador. O script abaixo paga o startup uma vez e divide o custo por N escritas:

```bash
cat > /tmp/cass-elc.sh <<'EOF'
#!/bin/bash
N=${1:-10}
gen() {
  echo "CONSISTENCY $1;"
  for i in $(seq 1 $N); do
    echo "INSERT INTO pacelc_lab.sensordata (id, status) VALUES (uuid(), '$2');"
  done
}
ms() { echo $(( ( $(date +%s%N) - $1 ) / 1000000 )); }
S=$(date +%s%N); cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1; BASE=$(ms $S)
S=$(date +%s%N); gen ONE latency_test     | cqlsh >/dev/null 2>&1; T_ONE=$(ms $S)
S=$(date +%s%N); gen ALL consistency_test | cqlsh >/dev/null 2>&1; T_ALL=$(ms $S)
echo "  startup cqlsh (custo fixo) ... ${BASE}ms"
echo "  $N escritas CONSISTENCY ONE .. ${T_ONE}ms  (~$(( (T_ONE-BASE)/N ))ms/escrita)"
echo "  $N escritas CONSISTENCY ALL .. ${T_ALL}ms  (~$(( (T_ALL-BASE)/N ))ms/escrita)"
EOF
docker cp /tmp/cass-elc.sh cassandra-node1:/tmp/cass-elc.sh
docker exec cassandra-node1 bash /tmp/cass-elc.sh 10
```

### Resultado medido

| Cenário | `CONSISTENCY ONE` | `CONSISTENCY ALL` |
| --- | --- | --- |
| Malha saudável | ~0 ms/escrita | ~0 ms/escrita |
| `cassandra-node2` com 2000 ms | **9 ms/escrita** | **2002 ms/escrita** |

Em rede saudável, exigir `ALL` custa o mesmo que exigir `ONE` — consistência forte não tem preço intrínseco. Sob degradação de um único nó, a razão salta para **~222×**. O custo da consistência é função da saúde da malha, não uma taxa fixa.

Encerre o caos e aguarde a limpeza do `netem` antes de qualquer outra medição:

```bash
docker stop pumba-cass 2>/dev/null
sleep 30
```

---

## Resultados medidos

| Etapa | Tempo |
| --- | --- |
| Bootstrap do cluster (`up -d --wait`) | 251 s |
| T1 — portão do anel | 2 s |
| T2 — schema RF=3 | 3 s |
| T3 — eixo PAC (3 estados + restauração) | 78 s |
| T4 — eixo ELC (controle + caos) | ~50 s |
| **Total** | **~7 min** |

---

## Erros e armadilhas verificados em execução

| Sintoma | Causa | Correção |
| --- | --- | --- |
| Cluster sobe de forma intermitente | Nós ingressando em paralelo colidem na negociação de tokens | `depends_on: condition: service_healthy` |
| Host sem memória com 3 nós | Heap padrão dimensionado pela RAM total | `MAX_HEAP_SIZE=512M`, `HEAP_NEWSIZE=128M` |
| Nó em `UJ` após `--wait` | Healthcheck não implica ingresso no anel | Aguardar 3× `UN` em `nodetool status` |
| Latências ~800 ms sem caos | Startup do `cqlsh` (Python) | Amortizar N escritas em uma sessão |
| `CONSISTENCY ALL` não fica lento | Nenhum nó degradado no caminho crítico | Degradar qualquer réplica (com `ALL`, todas são críticas) |
| `QUORUM` devolve timeout e não `Unavailable` | Gossip ainda não marcou os nós como mortos | Esperar ~1 min ou aceitar as duas formas de falha |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash reescreve `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |
| Segunda rodada de caos com números estranhos | `netem` não é removido instantaneamente | Aguardar ~30 s entre experimentos |

---

## Implicações Arquiteturais

O Cassandra não ocupa um ponto do espaço PACELC: ele **expõe o eixo como parâmetro de chamada**. A matriz da Fase 3 mostra a mesma topologia respondendo ou recusando conforme o nível pedido, e a da Fase 4 mostra que o preço da consistência é zero em rede saudável e proibitivo sob degradação.

A consequência de projeto é que a decisão PACELC no Cassandra não é tomada na infraestrutura, e sim **em cada consulta, pelo desenvolvedor**. Isso é poder e é risco: um `CONSISTENCY ALL` esquecido em um caminho quente transforma a degradação de um único nó em indisponibilidade percebida por toda a aplicação. Projetar com Cassandra exige tratar o nível de consistência como decisão explícita e revisável por operação — reservando `QUORUM` ou `ALL` para os pontos onde a divergência de estado comprometa o domínio de negócio de forma irreversível.

---

## Encerramento

```bash
docker rm -f pumba-cass 2>/dev/null
docker compose down -v
```
