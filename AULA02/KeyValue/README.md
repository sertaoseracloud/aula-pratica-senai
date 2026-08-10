# Laboratório 5 — Chave-valor / Redis: gravar é mudar a memória

**Duração: ~3 minutos** (9 s subindo + ~2 min de operações).

## O que você vai fazer

Subir um Redis de nó único e gravar nas cinco estruturas que ele oferece — string, hash, lista, conjunto e conjunto ordenado. Depois você vai matar o processo de duas formas diferentes e conferir o que sobreviveu.

## O que você vai descobrir

Duas coisas, e a segunda costuma incomodar.

**A estrutura que você escolhe é a operação de escrita que você ganha.** Não existe "gravar um dado" genérico no Redis. Você grava numa lista, e ganha `RPUSH`. Grava num conjunto ordenado, e ganha `ZINCRBY`. A modelagem não é sobre onde o dado cabe — é sobre qual escrita você vai precisar fazer daqui a seis meses.

**E o `OK` não quer dizer que o dado está em disco.** Com a configuração de fábrica, uma escrita confirmada pelo Redis pode desaparecer se o processo cair no segundo seguinte:

| Como o processo terminou | O dado gravado antes volta? |
| --- | --- |
| `docker restart` (SIGTERM, desligamento limpo) | Sim |
| `docker kill` (SIGKILL, queda) | **Não** |
| `docker kill` depois de um `SAVE` explícito | Sim |

Você vai produzir essa tabela no Passo 6.

---

## Passo 1 — Criar o docker-compose.yml

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

```yaml
services:
  redis-lab:
    image: redis:7.0
    container_name: redis-lab
    # Sem arquivo de configuracao e sem volume nomeado: o Redis sobe com os
    # defaults de fabrica. Isso e proposital -- o Passo 6 mede exatamente o
    # que esses defaults garantem, e o que eles nao garantem.
    command: redis-server
    networks:
      - keyvalue-net
    healthcheck:
      test: ["CMD-SHELL", "redis-cli ping | grep -q PONG"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 3s

networks:
  keyvalue-net:
    name: aula02-keyvalue-network
    driver: bridge
```

Duas ausências merecem explicação.

**Não há `volumes:`.** O arquivo de dados vive no sistema de arquivos do contêiner. Isso basta para o Passo 6, porque `docker kill` seguido de `docker start` reaproveita o mesmo contêiner — e é justamente a queda do processo, não a remoção do contêiner, que o laboratório mede.

**Não há `--appendonly yes`.** Ligar o AOF resolveria o problema do Passo 6, e é por isso que ele fica desligado: o objetivo é ver o comportamento padrão antes de ver a correção.

## Passo 2 — Subir

```bash
docker compose up -d --wait
```

Aqui o `--wait` basta. Não há cluster para formar, nem réplica para sincronizar: quando o `PING` responde, o Redis aceita escrita.

```bash
docker exec redis-lab redis-cli PING
docker exec redis-lab redis-cli INFO server | grep redis_version
```

Confira também com o que ele subiu — esses dois valores são a razão de o Passo 6 funcionar:

```bash
docker exec redis-lab redis-cli CONFIG GET save
docker exec redis-lab redis-cli CONFIG GET appendonly
```

```
save
3600 1 300 100 60 10000
appendonly
no
```

Leia a primeira linha assim: *salve em disco se passou 1 hora e houve 1 alteração; ou 5 minutos e 100 alterações; ou 1 minuto e 10 mil alterações.* Uma escrita isolada num sistema parado pode esperar **uma hora** até ser gravada.

---

## Passo 3 — Cinco estruturas, cinco formas de gravar

Comece do zero. O `FLUSHALL` é o que torna este laboratório repetível:

```bash
docker exec redis-lab redis-cli FLUSHALL
```

Agora grave o mesmo pedido de cinco formas, cada uma respondendo a uma pergunta diferente da loja:

```bash
R() { docker exec redis-lab redis-cli "$@"; }

# 1. String com prazo de validade -- a sessao do cliente
R SET sessao:ana '{"cliente":"ana","carrinho":3}' EX 300
R TTL sessao:ana

# 2. Hash -- o carrinho, campo a campo
R HSET carrinho:ana produto:caneca 2 produto:teclado 1
R HINCRBY carrinho:ana produto:caneca 3
R HGETALL carrinho:ana

# 3. Lista -- a fila de pedidos a processar
R RPUSH fila:pedidos pedido:1001 pedido:1002
R LRANGE fila:pedidos 0 -1

# 4. Conjunto -- os produtos que a cliente ja viu, sem repetir
R SADD vistos:ana caneca teclado caneca
R SMEMBERS vistos:ana

# 5. Conjunto ordenado -- o ranking de mais vendidos
R ZINCRBY ranking:produtos 5 caneca
R ZINCRBY ranking:produtos 2 teclado
R ZREVRANGE ranking:produtos 0 -1 WITHSCORES
```

### O que você vai ver

| Comando | Retorno | O que o retorno está dizendo |
| --- | --- | --- |
| `SET ... EX 300` | `OK` | gravado, e programado para sumir em 300 s |
| `TTL sessao:ana` | `300` | o prazo é do **dado**, não da aplicação |
| `HSET` com 2 campos | `2` | dois campos **novos** foram criados |
| `HINCRBY ... 3` | `5` | leu 2, somou 3, gravou 5 — sem você ler nada |
| `RPUSH` com 2 itens | `2` | a lista agora tem 2 elementos |
| `SADD` com 3 argumentos | **`2`** | `caneca` estava repetido e foi descartado |
| `ZINCRBY 5 caneca` | `5` | a chave não existia; começou do zero e somou |

A linha do `SADD` é a que vale reler. Você mandou três valores e o Redis gravou dois. **Não houve erro.** A deduplicação é o contrato da estrutura, e o retorno `2` é a única forma de você saber que algo foi descartado.

O `HGETALL` e o `ZREVRANGE` confirmam o estado final:

```
produto:caneca
5
produto:teclado
1
```

```
caneca
5
teclado
2
```

> **Por que `R()` e não `redis-cli` direto.** O laboratório inteiro roda por `docker exec`, sem instalar nada na sua máquina. A função só encurta a digitação — ela não muda o comportamento, porque cada `redis-cli` aqui é um comando independente.

---

## Passo 4 — Gravar sem ler antes

Esta é a diferença prática entre um banco chave-valor e um cache que você mesmo escreveria com um dicionário.

```bash
R() { docker exec redis-lab redis-cli "$@"; }

# Baixar estoque sem nunca ler o estoque
R SET estoque:caneca 10
R DECRBY estoque:caneca 3
R DECRBY estoque:caneca 3

# Contar sem inicializar
R DEL visitas
R INCR visitas

# Gravar so se ninguem gravou antes -- a trava
R SET pedido:1001 processando NX
R SET pedido:1001 processando NX
```

### O que você vai ver

| Comando | Retorno |
| --- | --- |
| `DECRBY estoque:caneca 3` | `7` |
| `DECRBY estoque:caneca 3` | `4` |
| `INCR visitas` (chave inexistente) | `1` |
| `SET pedido:1001 ... NX` (1ª vez) | `OK` |
| `SET pedido:1001 ... NX` (2ª vez) | *(vazio)* |

Três coisas acontecem aí.

**O `DECRBY` não é um atalho de digitação.** A alternativa — `GET`, subtrair na aplicação, `SET` — tem uma janela entre a leitura e a escrita em que outro processo pode gravar. Dois clientes fazendo isso ao mesmo tempo vendem a mesma caneca duas vezes. O `DECRBY` acontece dentro do Redis, e o Redis executa um comando por vez.

**O `INCR` inventa a chave.** Não existe "inicializar o contador". A ausência da chave é tratada como zero, e é por isso que você nunca precisa de um `if` antes de contar.

**O retorno vazio do segundo `SET ... NX` é a resposta, não uma falha.** Nada foi gravado porque a chave já existia. Isso é o que torna o `NX` utilizável como trava distribuída — e é também o que torna este passo repetível: rode o bloco dez vezes e o estoque continua caindo, mas o `pedido:1001` nunca é sobrescrito.

---

## Passo 5 — O que o `MULTI/EXEC` garante (e o que não)

O `MULTI/EXEC` é frequentemente lido como "transação". Ele agrupa comandos e os executa sem intercalar ninguém — mas **não desfaz nada**. Os dois testes abaixo mostram os dois comportamentos, e eles são diferentes.

Prepare o cenário:

```bash
R() { docker exec redis-lab redis-cli "$@"; }
R FLUSHALL
R HSET carrinho:ana caneca 2
R RPUSH fila a
```

### A) Erro de tipo — descoberto só no `EXEC`

`carrinho:ana` é um hash. `INCR` num hash é inválido, mas o Redis só percebe na hora de executar:

```bash
docker exec redis-lab bash -c "printf 'MULTI\nINCR carrinho:ana\nRPUSH fila b\nEXEC\n' | redis-cli"
docker exec redis-lab redis-cli LRANGE fila 0 -1
```

```
OK
QUEUED
QUEUED
WRONGTYPE Operation against a key holding the wrong kind of value

2
```

```
a
b
```

O `INCR` falhou. O `RPUSH` **executou assim mesmo** — o `b` está na lista. Não houve rollback.

### B) Erro de sintaxe — descoberto ao enfileirar

Agora um comando malformado, que o Redis rejeita antes mesmo de enfileirar:

```bash
docker exec redis-lab bash -c "printf 'MULTI\nRPUSH\nRPUSH fila c\nEXEC\n' | redis-cli"
docker exec redis-lab redis-cli LRANGE fila 0 -1
```

```
OK
ERR wrong number of arguments for 'rpush' command

QUEUED
EXECABORT Transaction discarded because of previous errors.
```

```
a
b
```

O `c` não entrou. O bloco inteiro foi descartado.

### O que a diferença significa

| Tipo de erro | Quando o Redis percebe | O que acontece com os outros comandos |
| --- | --- | --- |
| Sintaxe (argumentos errados) | ao enfileirar | **nenhum executa** — `EXECABORT` |
| Tipo (operação inválida no valor) | no `EXEC` | **todos os outros executam** |

O caso perigoso é o segundo, porque é o que depende de dados de produção. Um erro de sintaxe você descobre no primeiro teste; um `WRONGTYPE` aparece quando uma chave recebeu um tipo diferente do esperado — e aí o `MULTI` grava metade do que você pediu, sem desfazer.

**`MULTI/EXEC` dá isolamento, não atomicidade.** Ninguém executa no meio do seu bloco. Mas se um comando falhar, o que já rodou fica gravado, e é seu código que precisa consertar.

---

## Passo 6 — A gravação que o Redis confirmou e perdeu

Aqui está o resultado central do laboratório. Os três cenários abaixo são o mesmo `SET`, mudando só a forma como o processo termina.

> Os `sleep 4` dão tempo de o Redis reabrir a porta depois de subir. Sem eles o `redis-cli` seguinte falha por conexão recusada, e parece que o dado sumiu quando na verdade o banco ainda não subiu.

```bash
R() { docker exec redis-lab redis-cli "$@"; }
docker compose down >/dev/null 2>&1
docker compose up -d --wait

# A) desligamento limpo
R SET pedido:3001 confirmado
docker restart redis-lab ; sleep 4
echo "A) GET pedido:3001 -> [$(R GET pedido:3001)]   DBSIZE=$(R DBSIZE)"

# B) queda
R SET pedido:3002 confirmado
docker kill redis-lab ; docker start redis-lab ; sleep 4
echo "B) GET pedido:3002 -> [$(R GET pedido:3002)]   DBSIZE=$(R DBSIZE)"

# C) queda, mas com SAVE explicito antes
R SET pedido:3003 confirmado
R SAVE
docker kill redis-lab ; docker start redis-lab ; sleep 4
echo "C) GET pedido:3003 -> [$(R GET pedido:3003)]   DBSIZE=$(R DBSIZE)"
```

### O que você vai ver

```
A) GET pedido:3001 -> [confirmado]   DBSIZE=1
B) GET pedido:3002 -> []             DBSIZE=1
C) GET pedido:3003 -> [confirmado]   DBSIZE=2
```

| Cenário | Sinal recebido | O dado voltou | Por quê |
| --- | --- | --- | --- |
| A — `docker restart` | SIGTERM | **Sim** | o Redis grava o RDB antes de sair |
| B — `docker kill` | SIGKILL | **Não** | não houve chance de gravar nada |
| C — `SAVE` + `docker kill` | SIGKILL | **Sim** | o RDB já estava em disco |

### O detalhe que fecha o argumento

Olhe o `DBSIZE` da linha B: **1**, não 0.

O banco não voltou vazio. Ele voltou com o `pedido:3001` — a foto tirada no desligamento limpo do cenário A. O `pedido:3002` foi gravado **depois** dessa foto e nunca entrou em nenhuma outra.

Ou seja, não é que "o Redis perde tudo quando cai". É pior de diagnosticar: ele volta com um estado antigo e plausível. Não há erro, não há arquivo corrompido, não há alerta. Só faltam as escritas mais recentes — exatamente as que o cliente acabou de fazer e lembra ter feito.

E o `SET pedido:3002` tinha respondido `OK`.

### As duas correções

O `SAVE` do cenário C não serve para produção: ele bloqueia o servidor inteiro enquanto grava. Em uso real existem duas saídas, e as duas se declaram na subida:

```yaml
command: redis-server --appendonly yes --appendfsync everysec
```

O AOF registra cada escrita num log e o sincroniza a cada segundo — a janela de perda cai de "até uma hora" para "até um segundo". Com `--appendfsync always` a janela fecha de vez, ao custo de um `fsync` por escrita.

> **Por que não dá para ligar isso agora com `CONFIG SET appendonly yes`.** Dá — e não adianta. Sem arquivo de configuração, a mudança vale só para o processo em execução: na próxima subida o Redis lê `appendonly no` de novo e carrega o RDB, ignorando o AOF que ele mesmo escreveu. Testar isso de verdade exige mudar o `command:` do compose e recriar o contêiner.

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir (`up -d --wait`) | 9 s |
| Passo 3 — cinco estruturas | 8 s |
| Passo 4 — escrita sem leitura | 6 s |
| Passo 5 — `MULTI/EXEC` (dois casos) | 5 s |
| Passo 6 — persistência (3 cenários, 3 subidas) | 45 s |
| **Total** | **~3 min** |

A primeira execução soma o download da imagem `redis:7.0` (~30 MB).

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| `Could not connect to Redis` logo após `docker start` | o processo ainda não reabriu a porta | os `sleep 4` do Passo 6 |
| `SADD` retorna menos do que você mandou | valores repetidos são descartados em silêncio | é o contrato do conjunto — leia o retorno |
| `MULTI` grava metade e não desfaz | `WRONGTYPE` só aparece no `EXEC` | tratar no código; não existe rollback |
| Dado some depois de `docker kill` | RDB com `save 3600 1` e AOF desligado | `--appendonly yes` no `command:` |
| `DBSIZE` volta 1 em vez de 0 após a queda | o RDB antigo foi recarregado | é o resultado esperado do cenário B |
| `CONFIG SET appendonly yes` não sobrevive ao restart | sem arquivo de configuração, a mudança é volátil | mudar o `command:` e recriar |
| Segunda execução do Passo 3 com números diferentes | o `FLUSHALL` não rodou | rodar `FLUSHALL` antes do bloco |

---

## O que levar disso para o trabalho

**A escolha da estrutura é a decisão de modelagem.** Num banco relacional você modela o dado e escolhe a consulta depois. Aqui é o contrário: ao gravar um ranking como conjunto ordenado, você ganhou `ZINCRBY` e perdeu a possibilidade de perguntar "quais produtos custam mais de 200". Vale escrever a operação de escrita que você vai precisar **antes** de escolher a estrutura.

**Ler o retorno não é opcional.** `SADD` descartando duplicata, `SET NX` recusando a gravação, `WAIT` devolvendo menos réplicas do que você pediu — nenhum desses casos gera erro. Um cliente que só verifica exceção não enxerga nenhum deles. No Redis, o valor de retorno *é* o canal de erro.

**E `OK` significa "está na memória", não "está gravado".** Isso é uma escolha de projeto, não um defeito: é ela que dá ao Redis a latência pela qual você o escolheu. O problema é assumir a durabilidade sem ter configurado. A pergunta a fazer antes de colocar qualquer coisa no Redis é: **se este processo cair agora, quanto do último minuto eu posso perder sem que alguém precise ser avisado?**

Se a resposta for "nada", ou você liga o AOF com `appendfsync always` — pagando o `fsync` — ou esse dado não deveria estar só aqui.

Vale reparar que este laboratório encontrou o mesmo padrão do [Laboratório 2](../../AULA01/elastic-cache-redis/README.md), por um caminho diferente. Lá o Redis aceitava escrita sem nenhuma réplica alcançável; aqui ele aceita escrita sem nada em disco. As duas são a mesma decisão: **confirmar primeiro, persistir depois.**

---

## Encerrar

```bash
docker compose down -v
```
