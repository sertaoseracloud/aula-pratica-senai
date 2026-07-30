# Laboratório 2 — ElastiCache / Redis: o banco que nunca diz não

**Duração: ~2 minutos** (60 s subindo + ~65 s de testes). É o laboratório mais rápido do repositório.

## O que você vai fazer

Subir um Redis com um primário e duas réplicas, e usar o comando `WAIT` para forçar o primário a esperar confirmação das réplicas antes de liberar o cliente.

Depois você vai atrasar uma réplica e desconectar as duas, observando o que muda.

## O que você vai descobrir

O resultado central deste laboratório separa o Redis de todos os outros deste repositório:

**O Redis nunca recusa uma escrita.** Nem com uma réplica lenta, nem com uma réplica fora, nem com o primário completamente isolado. O `SET` sempre volta `OK`.

O `WAIT` **não impede** a escrita. Ele só conta, depois do fato, quantas réplicas receberam o dado. O que você faz com essa informação é problema seu — o banco não decide por você.

Há ainda uma armadilha de medição que faz o teste passar sem provar nada. Ela está no Passo 3, e vale ler antes de rodar qualquer coisa.

---

## Passo 1 — Criar o docker-compose.yml

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

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

**Por que o healthcheck das réplicas checa `master_link_status:up` e não só `PING`:** uma réplica Redis responde `PONG` bem antes de terminar de sincronizar com o primário. Se o healthcheck fosse só o `PING`, o laboratório começaria a medir antes de existir replicação, e os números não significariam nada.

## Passo 2 — Subir e conferir

```bash
docker compose up -d --wait

docker exec redis-client redis-cli -h redis-primary INFO replication \
  | grep -E "role|connected_slaves|slave[0-9]"
```

Você deve ver:

```
role:master
connected_slaves:2
slave0:ip=172.20.0.3,port=6379,state=online,offset=14,lag=1
slave1:ip=172.20.0.4,port=6379,state=online,offset=14,lag=1
```

Se `connected_slaves` vier `0` ou `1`, espere alguns segundos e repita.

---

## Passo 3 — A armadilha do `WAIT` (leia antes de medir)

Esta é a parte mais importante do laboratório, e o perigo dela é que **o teste errado parece funcionar**. Não dá erro, não trava, devolve um número plausível. E não prova nada.

### Como o `WAIT` funciona de verdade

`WAIT n timeout` espera até que `n` réplicas confirmem **as escritas feitas por aquela conexão específica**.

A palavra decisiva é *conexão*. Se o `WAIT` roda numa conexão que não escreveu nada, ele não tem o que esperar: retorna na hora, devolvendo só a contagem de réplicas conectadas.

E aqui está o problema: **cada vez que você chama `redis-cli`, abre uma conexão nova.**

```bash
# ERRADO — duas invocações, duas conexões.
# O WAIT não sabe do SET anterior e volta em ~13ms mesmo com réplica degradada.
redis-cli -h redis-primary SET k v
redis-cli -h redis-primary WAIT 2 5000
```

```bash
# CERTO — uma única conexão, os dois comandos no mesmo fluxo.
printf 'SET k v\nWAIT 2 5000\n' | redis-cli -h redis-primary
```

### A diferença medida

Com a `redis-replica-1` atrasada em 2000 ms:

| Como você escreve | Tempo | O que significa |
| --- | --- | --- |
| Conexões separadas | 13 ms | **falso negativo** — não testou nada |
| Mesma conexão | 2024 ms | correto |

No `redis-cli` interativo isso não aparece, porque a sessão é uma só do começo ao fim. O problema surge quando você automatiza o laboratório em script — que é justamente o que se faz para ter medição reproduzível.

### O script correto

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

A função `sq` é o que garante a conexão única: ela manda os dois comandos pelo mesmo `redis-cli`.

---

## Teste T2 — Quanto custa esperar as réplicas (eixo ELC)

Atrase **uma** das réplicas em 2 segundos:

```bash
docker rm -f pumba-redis 2>/dev/null
MSYS_NO_PATHCONV=1 docker run -d --name pumba-redis --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gaiaadm/pumba netem --duration 3m delay --time 2000 redis-replica-1
sleep 8

docker exec redis-client bash /tmp/redis-lab.sh
```

> `MSYS_NO_PATHCONV=1` só é necessário no Git Bash do Windows.

### O que você vai ver

| Operação | Sem caos | Com `redis-replica-1` atrasada |
| --- | --- | --- |
| `SET` sem barreira | 16 ms | **15 ms** |
| `SET` + `WAIT 1 5000` | 18 ms | **15 ms** |
| `SET` + `WAIT 2 5000` | 18 ms | **2024 ms** |

Repare na linha do meio. Com uma réplica saudável e outra lenta:

- `WAIT 1` pede **uma** confirmação — a réplica rápida atende na hora, e você nem percebe que a outra está mal.
- `WAIT 2` pede **as duas** — agora você é obrigado a esperar a lenta.

**O eixo ELC não é uma chave liga/desliga, é um dial.** O `WAIT` é o botão, e você escolhe a posição por operação: `WAIT 0` para um contador de visitas, `WAIT 2` para uma trava distribuída.

Encerre o caos e espere a limpeza:

```bash
docker stop pumba-redis 2>/dev/null
sleep 30
```

> Os 30 segundos evitam que o próximo teste herde este atraso.

---

## Teste T3 — O que acontece quando as réplicas somem (eixo PAC)

```bash
net_out() { docker network disconnect pacelc-elasticache-network "$1" 2>/dev/null; }
net_in()  { docker network connect    pacelc-elasticache-network "$1" 2>/dev/null; }

docker exec redis-client bash /tmp/redis-lab.sh          # com 2 réplicas
net_out redis-replica-2 ; sleep 12
docker exec redis-client bash /tmp/redis-lab.sh          # com 1 réplica
net_out redis-replica-1 ; sleep 12
docker exec redis-client bash /tmp/redis-lab.sh          # com 0 réplicas
net_in redis-replica-1 ; net_in redis-replica-2 ; sleep 15
```

### O que você vai ver

| Réplicas alcançáveis | `SET` puro | `WAIT 1 5000` | `WAIT 2 5000` |
| --- | --- | --- | --- |
| 2 | 14 ms | 15 ms → `1` | 15 ms → `2` |
| 1 | 20 ms | 16 ms → `1` | **5098 ms → `1`** |
| 0 | 22 ms | **5103 ms → `0`** | **5117 ms → `0`** |

Duas coisas para observar com atenção.

### O `SET` nunca falha

Olhe a primeira coluna inteira. Mesmo com o primário totalmente isolado, sem **nenhuma** réplica alcançável, a escrita é aceita em 22 ms.

Compare com os outros laboratórios: Cassandra e ScyllaDB, na mesma situação, **recusam** a escrita com erro. O Redis aceita e não avisa.

Essa é a forma mais pura do quadrante **PA** — e a mais perigosa, porque não existe erro para o seu código tratar. A aplicação segue achando que gravou. Se houver failover antes de a réplica se reconectar, o dado simplesmente desaparece.

### O `WAIT` não protege, só informa

Olhe as células de 5098 ms e 5103 ms. O `WAIT` esgotou o timeout de 5 segundos e devolveu `1` ou `0`.

Só que **a escrita já aconteceu**. Ela está no primário. O `WAIT` não desfaz nada, não trava nada, não recusa nada. Ele apenas devolve um número honesto e passa a decisão para você.

Ou seja: o `WAIT` não é um mecanismo de consistência, é um mecanismo de **observabilidade** da consistência. Se o seu código não olha o retorno e não faz nada quando ele vem menor que o pedido, o `WAIT` não está te protegendo de coisa alguma.

### O detalhe que engana o time de operação

Durante toda a partição, o primário continua reportando:

```
connected_slaves:2
```

O Redis leva cerca de 60 segundos (`repl-timeout`) para reclassificar uma réplica que sumiu. Durante essa janela **a métrica mente**: um painel de monitoração baseado em `connected_slaves` mostra saúde perfeita enquanto as escritas já não estão sendo replicadas.

Só o retorno do `WAIT` mostra a verdade em tempo real.

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir o cluster | 60 s |
| T1 — conferir topologia | 1 s |
| T2 — eixo ELC (controle + caos) | ~10 s |
| T3 — eixo PAC (3 estados + restauração) | 58 s |
| **Total** | **~2 min** |

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| `WAIT` volta na hora e o teste "passa" | `SET` e `WAIT` em conexões diferentes | Mesmo fluxo: `printf 'SET..\nWAIT..\n' \| redis-cli` |
| Medição começa sem existir replicação | `PING` responde antes da sincronização | Healthcheck em `master_link_status:up` |
| `connected_slaves:2` durante a partição | `repl-timeout` de ~60 s | Confiar no retorno do `WAIT`, não na métrica |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash converte `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |
| Segunda rodada de caos com números estranhos | Latência residual do teste anterior | Esperar 30 s antes de continuar |

---

## O que levar disso para o trabalho

Usar ElastiCache — ou qualquer Redis com replicação assíncrona — significa aceitar o quadrante **PA/EL** por padrão. E o laboratório mostra que isso não é um detalhe de configuração que você ajusta depois: é estrutural. O primário aceita escritas em qualquer condição de rede, inclusive isolado de tudo. Perder essas escritas num failover é consequência aritmética disso, não um bug.

O `WAIT` oferece uma saída parcial, e vale entender exatamente o quanto ela vale:

**O que o `WAIT` faz:** devolve um número honesto de quantas réplicas confirmaram.

**O que o `WAIT` não faz:** não torna a escrita transacional, não desfaz nada quando o número vem baixo, não impede a escrita de acontecer.

Para travas distribuídas e barreiras transacionais o `WAIT` é utilizável — desde que o código trate explicitamente o retorno insuficiente. Se você chama `WAIT 2 5000` e ignora o retorno, você não ganhou garantia nenhuma; só adicionou até 5 segundos de latência ao caminho crítico.

Some isso à métrica `connected_slaves`, que demora um minuto para dizer a verdade, e você tem uma combinação especialmente traiçoeira: o painel diz que está tudo bem, o código acha que gravou, e o dado não está em lugar nenhum além de um processo que pode reiniciar.

---

## Encerrar

```bash
docker rm -f pumba-redis 2>/dev/null
docker compose down -v
```
