# Tutorial Analítico: Validação do Teorema PACELC em Memória (Emulação ElastiCache com Redis)

O Amazon ElastiCache, ancorado no motor Redis, é comercializado como o ápice da baixa latência — e sustenta essa métrica através de replicação **assíncrona**. Este laboratório orquestra um agrupamento Redis (um primário, duas réplicas, um cliente bastion) e usa o comando de barreira `WAIT` para transitar o banco entre os quadrantes do PACELC.

O resultado central deste laboratório o distingue de todos os outros do repositório: **o Redis nunca recusa uma escrita**. Nem sob degradação, nem sob partição total. O `WAIT` não impede a escrita — apenas revela, depois do fato, quantas réplicas a receberam.

> Todos os números abaixo foram obtidos executando este laboratório de ponta a ponta.

**Tempo total estimado: ~2 minutos** (bootstrap 60 s + testes ~65 s). É o laboratório mais rápido do repositório.

---

## Fase 1: Topologia de Replicação em Memória

> O `docker-compose.yml` é ignorado pelo Git (ver [.gitignore](../../.gitignore)). Copie o conteúdo abaixo para `docker-compose.yml` neste diretório.

```yaml
services:
  redis-primary:
    image: redis:7.0
    container_name: redis-primary
    command: redis-server
    networks:
      - elasticache-ring
    healthcheck:
      test: ["CMD-SHELL", "redis-cli ping | grep -q PONG"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 5s

  redis-replica-1:
    image: redis:7.0
    container_name: redis-replica-1
    command: redis-server --replicaof redis-primary 6379
    networks:
      - elasticache-ring
    depends_on:
      redis-primary:
        condition: service_healthy
    healthcheck:
      # master_link_status:up prova que a replicacao foi de fato estabelecida,
      # nao apenas que o processo respondeu ao PING.
      test: ["CMD-SHELL", "redis-cli INFO replication | grep -q 'master_link_status:up'"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 5s

  redis-replica-2:
    image: redis:7.0
    container_name: redis-replica-2
    command: redis-server --replicaof redis-primary 6379
    networks:
      - elasticache-ring
    depends_on:
      redis-primary:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "redis-cli INFO replication | grep -q 'master_link_status:up'"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 5s

  redis-client:
    image: redis:7.0
    container_name: redis-client
    entrypoint: /bin/bash
    command: -c "tail -f /dev/null"
    networks:
      - elasticache-ring
    depends_on:
      redis-replica-1:
        condition: service_healthy
      redis-replica-2:
        condition: service_healthy

networks:
  elasticache-ring:
    name: pacelc-elasticache-network
    driver: bridge
```

O healthcheck das réplicas verifica `master_link_status:up`, não apenas `PING`. Um Redis réplica responde `PONG` muito antes de concluir a sincronização inicial com o primário; sem essa distinção o laboratório começa a medir antes de existir replicação.

### Inicialização

```bash
docker compose up -d --wait
docker exec redis-client redis-cli -h redis-primary INFO replication | grep -E "role|connected_slaves|slave[0-9]"
```

Saída esperada:

```
role:master
connected_slaves:2
slave0:ip=172.20.0.3,port=6379,state=online,offset=14,lag=1
slave1:ip=172.20.0.4,port=6379,state=online,offset=14,lag=1
```

---

## Fase 2: A armadilha do `WAIT` — leia antes de medir

Esta é a armadilha mais séria do laboratório, e ela **falha silenciosamente**: o teste parece passar e não prova nada.

O `WAIT n timeout` bloqueia até que `n` réplicas confirmem as escritas **da conexão que o executou**. Se `SET` e `WAIT` viajarem em conexões diferentes, o `WAIT` não tem escrita pendente para aguardar e retorna imediatamente, devolvendo apenas a contagem de réplicas conectadas.

Cada invocação de `redis-cli` abre uma conexão nova. Portanto:

```bash
# ERRADO — duas conexoes. Retorna em ~13ms mesmo com replica degradada.
redis-cli -h redis-primary SET k v
redis-cli -h redis-primary WAIT 2 5000
```

```bash
# CERTO — uma unica conexao, comandos no mesmo fluxo.
printf 'SET k v\nWAIT 2 5000\n' | redis-cli -h redis-primary
```

Medição do erro, com `redis-replica-1` degradada em 2000 ms:

| Forma | Tempo | Veredito |
| --- | --- | --- |
| Conexões separadas | 13 ms | falso negativo — não prova nada |
| Mesma conexão | 2024 ms | correto |

No `redis-cli` interativo o problema não aparece, porque a sessão é uma só. Ele surge ao automatizar o laboratório em script — que é exatamente como se produzem medições reproduzíveis.

Script de medição correto:

```bash
cat > /tmp/redis-lab.sh <<'EOF'
#!/bin/bash
H="-h redis-primary"
ms() { echo $(( ( $(date +%s%N) - $1 ) / 1000000 )); }
sq() { printf "%b" "$1" | redis-cli $H 2>&1 | tr '\n' ' '; }
S=$(date +%s%N); O=$(sq "SET k:b v1\nWAIT 0 5000\n"); echo "  SET sem barreira   (EL) ... $(ms $S)ms  [$O]"
S=$(date +%s%N); O=$(sq "SET k:c v1\nWAIT 1 5000\n"); echo "  SET + WAIT 1 ............. $(ms $S)ms  [$O]"
S=$(date +%s%N); O=$(sq "SET k:d v1\nWAIT 2 5000\n"); echo "  SET + WAIT 2       (EC) ... $(ms $S)ms  [$O]"
EOF
docker cp /tmp/redis-lab.sh redis-client:/tmp/redis-lab.sh
docker exec redis-client bash /tmp/redis-lab.sh
```

---

## Fase 3: Eixo ELC — Latência vs. Consistência

Injete 2000 ms de atraso em **uma** das réplicas:

```bash
docker rm -f pumba-redis 2>/dev/null
MSYS_NO_PATHCONV=1 docker run -d --name pumba-redis --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba netem --duration 3m delay --time 2000 redis-replica-1
sleep 8
docker exec redis-client bash /tmp/redis-lab.sh
```

> `MSYS_NO_PATHCONV=1` é necessário apenas no Git Bash do Windows.

### Resultado medido

| Operação | Sem caos | `redis-replica-1` com 2000 ms |
| --- | --- | --- |
| `SET` sem barreira (EL) | 16 ms | **15 ms** |
| `SET` + `WAIT 1 5000` | 18 ms | **15 ms** |
| `SET` + `WAIT 2 5000` (EC) | 18 ms | **2024 ms** |

A gradação entre `WAIT 1` e `WAIT 2` é o achado mais elegante deste laboratório. Com uma réplica saudável e outra degradada, `WAIT 1` é satisfeito instantaneamente pela réplica rápida; `WAIT 2` obriga o cliente a esperar a lenta. **O eixo ELC não é binário — é um dial**, e o `WAIT` é o botão que o gira.

Encerre o caos e aguarde a limpeza do `netem`:

```bash
docker stop pumba-redis 2>/dev/null
sleep 30
```

---

## Fase 4: Eixo PAC — Disponibilidade sob Partição

```bash
net_out() { docker network disconnect pacelc-elasticache-network "$1" 2>/dev/null; }
net_in()  { docker network connect    pacelc-elasticache-network "$1" 2>/dev/null; }

docker exec redis-client bash /tmp/redis-lab.sh          # 2 réplicas
net_out redis-replica-2 ; sleep 12
docker exec redis-client bash /tmp/redis-lab.sh          # 1 réplica
net_out redis-replica-1 ; sleep 12
docker exec redis-client bash /tmp/redis-lab.sh          # 0 réplicas
net_in redis-replica-1 ; net_in redis-replica-2 ; sleep 15
```

### Resultado medido

| Réplicas conectadas | `SET` puro | `WAIT 1 5000` | `WAIT 2 5000` |
| --- | --- | --- | --- |
| 2 | 14 ms | 15 ms → `1` | 15 ms → `2` |
| 1 | 20 ms | 16 ms → `1` | **5098 ms → `1`** |
| 0 | 22 ms | **5103 ms → `0`** | **5117 ms → `0`** |

Duas leituras se impõem:

**O `SET` puro nunca falha.** Mesmo com o primário completamente isolado, sem uma única réplica alcançável, a escrita é aceita em 22 ms. Este é o quadrante **PA** em sua forma mais pura — e mais perigosa. O Cassandra e o ScyllaDB, na mesma situação, **recusam** a escrita. O Redis a aceita e não avisa.

**O `WAIT` não impede nada; apenas informa.** Ele esgota o timeout e devolve o número de réplicas que confirmaram — `1` ou `0`. A escrita **já ocorreu** no primário e permanece lá. O `WAIT` não é um mecanismo de consistência: é um mecanismo de *observabilidade* da consistência. Cabe à aplicação decidir o que fazer com um retorno menor que o exigido — e essa decisão não é oferecida pelo banco.

### Um detalhe que engana o operador

Durante toda a partição, o primário continua reportando:

```
connected_slaves:2
```

O Redis leva cerca de 60 s (`repl-timeout`) para reclassificar uma réplica particionada. Durante essa janela, **a métrica mente**: um painel de monitoração baseado em `connected_slaves` mostraria saúde plena enquanto as escritas já não estão sendo replicadas. Só o retorno do `WAIT` revela o estado verdadeiro em tempo real.

---

## Resultados medidos

| Etapa | Tempo |
| --- | --- |
| Bootstrap (`up -d --wait`) | 60 s |
| T1 — verificação da topologia | 1 s |
| T2 — eixo ELC (controle + caos) | ~10 s |
| T3 — eixo PAC (3 estados + restauração) | 58 s |
| **Total** | **~2 min** |

---

## Erros e armadilhas verificados em execução

| Sintoma | Causa | Correção |
| --- | --- | --- |
| `WAIT` retorna instantaneamente e o teste "passa" | `SET` e `WAIT` em conexões diferentes | Mesmo fluxo: `printf 'SET..\nWAIT..\n' \| redis-cli` |
| Medição começa antes de existir replicação | `PING` responde antes da sincronização inicial | Healthcheck em `master_link_status:up` |
| `connected_slaves:2` durante partição | `repl-timeout` de ~60 s | Confiar no retorno do `WAIT`, não na métrica |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash reescreve `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |
| Segunda rodada de caos com números estranhos | `netem` não é removido instantaneamente | Aguardar ~30 s entre experimentos |

---

## Implicações Arquiteturais

Adotar ElastiCache — ou qualquer Redis com replicação assíncrona — é assumir o quadrante **PA/EL** por padrão. O laboratório mostra que esse compromisso não é uma nuance de configuração: é estrutural. O primário aceita escritas em qualquer condição de rede, inclusive isolado de todas as réplicas, e a perda dessas escritas em um failover é uma consequência aritmética disso.

O `WAIT` oferece uma saída parcial e é importante entender exatamente o quanto ela vale. Ele não torna a escrita transacional nem a desfaz quando o quórum não é atingido — a mutação já está no primário. Ele apenas devolve à aplicação um número honesto, e transfere a ela a responsabilidade de compensar. Para travas distribuídas e barreiras transacionais isso é utilizável, desde que o código trate explicitamente o retorno insuficiente. Onde essa compensação não existir, o `WAIT` cria uma falsa sensação de garantia — que, somada à métrica `connected_slaves` que demora um minuto para dizer a verdade, é uma combinação especialmente traiçoeira em produção.

---

## Encerramento

```bash
docker rm -f pumba-redis 2>/dev/null
docker compose down -v
```
