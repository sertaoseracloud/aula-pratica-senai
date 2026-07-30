# Laboratório 3 — Cassandra: o quadrante é escolhido em cada consulta

**Duração: ~7 minutos** (251 s subindo o anel + ~130 s de testes).

## O que você vai fazer

Subir três nós Cassandra com fator de replicação 3 (cada nó guarda uma cópia de tudo), e rodar a **mesma consulta** com três níveis de consistência diferentes enquanto derruba nós.

## O que você vai descobrir

Nos laboratórios anteriores, quem decidia o comportamento era a infraestrutura: o contrato da réplica no PostgreSQL, o motor no Redis. Aqui é diferente.

**No Cassandra, quem decide é quem escreve a query.** A mesma topologia, no mesmo instante, responde ou falha dependendo de uma palavra na consulta:

| Nós vivos | `CONSISTENCY ONE` | `CONSISTENCY QUORUM` | `CONSISTENCY ALL` |
| --- | --- | --- | --- |
| 3 | OK | OK | OK |
| 2 | OK | OK | **Falha** |
| 1 | OK | **Falha** | **Falha** |

Essa tabela é o laboratório inteiro. Não existe "o quadrante do Cassandra" — existe o quadrante de cada consulta.

---

## Passo 1 — Criar o docker-compose.yml

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

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

Três escolhas nele merecem explicação:

**`CASSANDRA_SEEDS=cassandra-node1`** — só o primeiro nó é seed. Se você listar um nó que ainda não existe como seed, o Gossip demora mais para convergir sem ganho nenhum.

**`depends_on: condition: service_healthy`** — o Cassandra precisa que os nós entrem no anel **um de cada vez**. Dois nós entrando juntos colidem na negociação de tokens. Sem esse portão, o cluster sobe de forma intermitente: às vezes funciona, às vezes não, e você perde tempo procurando um problema que é de ordem de inicialização.

**`MAX_HEAP_SIZE=512M`** — por padrão o Cassandra dimensiona o heap pela RAM total da máquina. Três JVMs fazendo isso ao mesmo tempo derrubam um notebook.

## Passo 2 — Subir o anel

```bash
docker compose up -d --wait
```

**Isso leva cerca de 4 minutos** (251 s medidos). É lento porque os nós entram em série, de propósito. Não é travamento.

**Não comece a medir quando o comando voltar.** O `--wait` só confirma que os processos responderam. Um nó pode estar em estado `UJ` (entrando no anel) e ainda assim passar no healthcheck. Espere os três `UN` (Up/Normal):

```bash
docker exec cassandra-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel OK: 3 nos UN"; exit 0; }
    sleep 5
  done
  echo "TIMEOUT"; nodetool status; exit 1'
```

Para ver o anel a qualquer momento:

```bash
docker exec cassandra-node1 nodetool status
```

## Passo 3 — Criar o modelo de dados

O fator de replicação 3 é obrigatório neste laboratório: é ele que coloca uma cópia em cada nó e torna os níveis de consistência distinguíveis. Com RF=1 nada disso funcionaria.

```bash
docker exec cassandra-node1 cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS pacelc_lab WITH replication = {'class':'SimpleStrategy','replication_factor':3};
CREATE TABLE IF NOT EXISTS pacelc_lab.sensordata (id UUID PRIMARY KEY, status text);
INSERT INTO pacelc_lab.sensordata (id, status) VALUES (uuid(), 'baseline');
SELECT count(*) FROM pacelc_lab.sensordata;"
```

Os `IF NOT EXISTS` deixam você repetir o laboratório sem erro.

---

## Teste T3 — Quem responde quando os nós caem (eixo PAC)

### Preparar as funções

Desconectar e reconectar rede **não é idempotente**: repetir `disconnect` num nó já desconectado dá erro e para o script. Use estas funções:

```bash
net_out() { docker network disconnect pacelc-network "$1" 2>/dev/null; echo "  $1 fora da rede"; }
net_in()  { docker network connect    pacelc-network "$1" 2>/dev/null; echo "  $1 de volta na rede"; }
```

E esta sonda, que roda a mesma consulta nos três níveis:

```bash
probe() {
  for cl in ONE QUORUM ALL; do
    out=$(docker exec cassandra-node1 cqlsh -e "CONSISTENCY $cl; SELECT count(*) FROM pacelc_lab.sensordata;" 2>&1)
    echo "$out" | grep -qE "\(1 rows\)" && echo "  CONSISTENCY $cl ... OK" || echo "  CONSISTENCY $cl ... FALHOU"
  done
}
```

### Rodar

```bash
probe                                        # 3 nós
net_out cassandra-node3 ; sleep 6  ; probe   # 2 nós
net_out cassandra-node2 ; sleep 10 ; probe   # 1 nó
```

### O que você vai ver

| Nós vivos | `ONE` | `QUORUM` | `ALL` |
| --- | --- | --- | --- |
| 3 | OK | OK | OK |
| 2 | OK | OK | **Falha** |
| 1 | OK | **Falha** | **Falha** |

A lógica é simples quando você vê a tabela:

- `ONE` precisa de **1** réplica — sempre tem, enquanto o coordenador estiver de pé.
- `QUORUM` precisa de **2** de 3 — cai quando sobra só um nó.
- `ALL` precisa das **3** — cai assim que qualquer nó sai.

### As duas formas de falha (e por que a diferença importa)

Os erros não são iguais, e isso muda o que seu código precisa fazer.

**Com `QUORUM` e 1 nó vivo:**

```
ReadTimeout: code=1200 [Coordinator node timed out waiting for replica nodes' responses]
message="Operation timed out - received only 1 responses."
info={'consistency': 'QUORUM', 'required_responses': 2, 'received_responses': 1}
```

**Com `ALL` e 1 nó vivo:**

```
NoHostAvailable: Unavailable('code=1000 [Unavailable exception] message="Cannot achieve consistency level ALL"
info={'consistency': 'ALL', 'required_replicas': 3, 'alive_replicas': 1}')
```

O `QUORUM` deu **timeout**, não indisponibilidade. Logo depois do corte de rede o Gossip ainda acha que os nós estão vivos, então o coordenador tenta e fica esperando até estourar o tempo. O `ALL` deu **Unavailable** na hora, porque já tinha contabilizado `alive_replicas: 1` e nem tentou.

Rode a mesma sonda um minuto depois e o `QUORUM` também passa a devolver `Unavailable` — o Cassandra leva um tempo para perceber que os nós caíram.

Na prática: seu cliente pode receber **timeout** ou **erro imediato** para a mesma falha, dependendo de quando a consulta chegou. Os dois casos precisam estar tratados.

### Restaurar

```bash
net_in cassandra-node2 ; net_in cassandra-node3 ; sleep 25

docker exec cassandra-node1 bash -c '
  for i in $(seq 1 60); do
    [ "$(nodetool status | grep -c "^UN")" = "3" ] && { echo "anel restaurado: 3 UN"; exit 0; }
    sleep 5
  done; exit 1'
```

---

## Teste T4 — Quanto custa a consistência (eixo ELC)

### Por que aqui basta atrasar um nó

No [laboratório do ScyllaDB](../dynamodb-scylladb/README.md) é preciso degradar **dois** nós para ver qualquer efeito, porque lá o nível é `LOCAL_QUORUM` (2 de 3): as réplicas saudáveis atendem o quórum sozinhas e o nó lento nunca entra no caminho.

Aqui usamos `CONSISTENCY ALL`, que exige **as três** réplicas. Qualquer nó lento entra obrigatoriamente no caminho crítico. Um só basta.

### Injetar a latência

```bash
docker rm -f pumba-cass 2>/dev/null
MSYS_NO_PATHCONV=1 docker run -d --name pumba-cass --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba netem --duration 4m delay --time 2000 cassandra-node2
```

> `MSYS_NO_PATHCONV=1` só é necessário no Git Bash do Windows. Sem ele o Pumba falha com `mkdir C:\Program Files\Git\var: Acesso negado`.

### O script de medição

O `cqlsh` é Python e gasta **~800 ms só para abrir a sessão** — mais do que a operação que você quer medir. Um `time cqlsh -e "INSERT ..."` mede principalmente o interpretador.

O script abaixo paga esse custo uma vez e divide por N escritas:

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

Rode uma vez **antes** de ligar o Pumba (controle) e outra **com** ele ligado.

### O que você vai ver

| Cenário | `CONSISTENCY ONE` | `CONSISTENCY ALL` |
| --- | --- | --- |
| Rede saudável | ~0 ms/escrita | ~0 ms/escrita |
| `cassandra-node2` com 2000 ms | **9 ms/escrita** | **2002 ms/escrita** |

Repare na primeira linha: **em rede saudável, exigir `ALL` custa o mesmo que exigir `ONE`.** Consistência forte não tem preço intrínseco.

Na segunda linha a razão salta para **~222×**. O custo da consistência é função da saúde da rede — e como ele é praticamente zero em condições normais, não aparece em teste de carga. Aparece no incidente.

Encerre o caos:

```bash
docker stop pumba-cass 2>/dev/null
sleep 30
```

> Os 30 segundos evitam que uma próxima medição herde este atraso.

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir o anel (`up -d --wait`) | 251 s |
| T1 — esperar o anel fechar | 2 s |
| T2 — criar schema RF=3 | 3 s |
| T3 — eixo PAC (3 estados + restauração) | 78 s |
| T4 — eixo ELC (controle + caos) | ~50 s |
| **Total** | **~7 min** |

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| Cluster sobe às vezes sim, às vezes não | Nós entrando no anel em paralelo colidem | `depends_on: condition: service_healthy` |
| Máquina fica sem memória | Heap padrão dimensionado pela RAM total | `MAX_HEAP_SIZE=512M`, `HEAP_NEWSIZE=128M` |
| Nó em `UJ` mesmo depois do `--wait` | Healthcheck não garante ingresso no anel | Esperar 3× `UN` |
| Latências de ~800 ms sem caos nenhum | Startup do `cqlsh` (Python) | Amortizar N escritas numa sessão |
| `CONSISTENCY ALL` não fica lento | Nenhum nó degradado no caminho | Com `ALL` qualquer nó serve — confira se o Pumba subiu |
| `QUORUM` dá timeout em vez de `Unavailable` | Gossip ainda não marcou os nós como mortos | Normal; espere ~1 min ou trate os dois casos |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash converte `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |
| Segunda rodada de caos com números estranhos | Latência residual | Esperar 30 s entre experimentos |

---

## O que levar disso para o trabalho

O Cassandra não fica parado num ponto do PACELC. Ele **entrega o eixo como parâmetro de chamada**, e as duas tabelas deste laboratório mostram os dois lados disso:

- a do Teste T3 mostra a mesma topologia respondendo ou falhando conforme o nível pedido;
- a do Teste T4 mostra que o preço da consistência é zero em rede boa e proibitivo em rede ruim.

A consequência prática é incômoda: **a decisão de arquitetura não está na infraestrutura, está no código de quem escreve a query.** Um `CONSISTENCY ALL` esquecido num caminho quente transforma a lentidão de um único nó em indisponibilidade percebida pela aplicação inteira. E não vai aparecer em nenhum teste, porque em ambiente saudável ele é gratuito.

Ao usar Cassandra, trate o nível de consistência como decisão explícita e revisável por operação. Duas perguntas ajudam:

1. **Esta leitura pode devolver dado alguns milissegundos velho sem quebrar nada?** Se sim, `ONE`.
2. **Se este nó cair, prefiro erro ou dado desatualizado?** A resposta define entre `QUORUM` e `ONE`.

Reserve `QUORUM` e `ALL` para os pontos onde a divergência de estado causa dano de verdade — e documente o porquê, para que ninguém "otimize" depois sem entender.

---

## Encerrar

```bash
docker rm -f pumba-cass 2>/dev/null
docker compose down -v
```
