# Laboratório 7 — Família de colunas / Cassandra: toda gravação é uma sobrescrita

**Duração: ~5 minutos** (74 s subindo + ~3 min de operações).

## O que você vai fazer

Subir um nó Cassandra, declarar uma tabela com chave de partição e chave de agrupamento, e gravar pedidos — inclusive por cima de pedidos que já existem.

## O que você vai descobrir

Os dois laboratórios anteriores terminaram com o banco te avisando de alguma coisa: o Redis devolvendo `2` em vez de `3` no `SADD`, o MongoDB lançando `E11000`. Aqui não há aviso.

**No Cassandra o `INSERT` não insere: ele sobrescreve.** Gravar duas vezes na mesma chave primária não dá erro, não dá alerta e não deixa rastro — o segundo valor simplesmente toma o lugar do primeiro. `UPDATE` numa linha que nunca existiu também funciona, e cria a linha. `DELETE` de algo que nunca existiu é aceito igualmente.

A razão é a mesma nos três casos: **o Cassandra nunca lê antes de gravar.** Não existe a pergunta "já existe?", porque respondê-la exigiria consultar todas as réplicas antes de cada escrita — que é exatamente o custo que o motor foi desenhado para não pagar.

| Operação | Em um banco relacional | Aqui |
| --- | --- | --- |
| `INSERT` em chave que já existe | erro de chave duplicada | **sobrescreve em silêncio** |
| `UPDATE` em linha inexistente | 0 linhas afetadas | **cria a linha** |
| `DELETE` de linha inexistente | 0 linhas afetadas | aceito, grava um marcador |

Existe uma forma de obrigar o banco a verificar antes. Ela tem nome — `IF NOT EXISTS` — e tem preço, que você vai medir no Passo 5.

---

## Passo 1 — Criar o docker-compose.yml

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

```yaml
services:
  cassandra-lab:
    image: cassandra:4.1
    container_name: cassandra-lab
    environment:
      - CASSANDRA_CLUSTER_NAME=Aula02_Collumn
      # Um no so: sem outro no para conversar, o Gossip converge em segundos.
      - CASSANDRA_SEEDS=cassandra-lab
      # Por padrao o Cassandra dimensiona o heap pela RAM total da maquina.
      # Sem este limite uma JVM sozinha ja incomoda um notebook.
      - MAX_HEAP_SIZE=512M
      - HEAP_NEWSIZE=128M
    networks:
      - collumn-net
    healthcheck:
      # Aceitar CQL e o que interessa: o no pode estar UN e ainda recusar
      # comando enquanto o CQL nao subiu.
      test: ["CMD-SHELL", "nodetool status | grep -q '^UN' && cqlsh -e 'DESCRIBE KEYSPACES;'"]
      interval: 10s
      timeout: 15s
      retries: 30
      start_period: 30s

networks:
  collumn-net:
    name: aula02-collumn-network
    driver: bridge
```

**Um nó, e não três.** O [Laboratório 3](../../AULA01/cassandra/README.md) precisa de três porque mede níveis de consistência — que só existem havendo réplicas. Aqui o assunto é a semântica da gravação, que é idêntica com um nó ou com cem. A diferença prática é grande: 74 s para subir, contra os 251 s do anel de três.

**O healthcheck testa CQL, não só o processo.** `nodetool status` pode devolver `UN` enquanto a camada CQL ainda não aceita comando. Sem o `cqlsh -e` no teste, o `--wait` volta cedo demais e o primeiro `CREATE KEYSPACE` falha.

## Passo 2 — Subir

```bash
docker compose up -d --wait
docker exec cassandra-lab nodetool status
```

```
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load        Tokens  Owns (effective)  Host ID                               Rack
UN  172.21.0.2  104.36 KiB  16      100.0%            5f726377-0d0c-4228-9ba3-96df503fd5d1  rack1
```

**Leva ~74 s.** Diferente do laboratório de três nós, aqui o `--wait` é suficiente: não há anel para fechar, e o healthcheck já exige CQL respondendo. Quando o comando volta, pode gravar.

---

## Passo 3 — O schema que você é obrigado a declarar

Depois do MongoDB, a mudança é abrupta: aqui não se grava nada sem declarar a tabela, os tipos e — o que mais importa — a chave primária.

```bash
C() { docker exec cassandra-lab cqlsh -e "$1"; }

C "
CREATE KEYSPACE IF NOT EXISTS loja WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS loja.pedidos_por_cliente (
  cliente   text,
  criado_em timestamp,
  pedido_id uuid,
  total     decimal,
  status    text,
  PRIMARY KEY ((cliente), criado_em, pedido_id)
) WITH CLUSTERING ORDER BY (criado_em DESC);

CREATE TABLE IF NOT EXISTS loja.vendas_por_produto (
  produto  text PRIMARY KEY,
  unidades counter
);

TRUNCATE loja.pedidos_por_cliente;
TRUNCATE loja.vendas_por_produto;"
```

Os `IF NOT EXISTS` e os dois `TRUNCATE` são o que deixa este laboratório repetível: rode o bloco quantas vezes quiser e os passos seguintes produzem sempre os mesmos números.

### A linha que decide tudo

```
PRIMARY KEY ((cliente), criado_em, pedido_id)
```

Ela tem duas partes, e elas fazem coisas diferentes:

| Parte | Papel | Consequência |
| --- | --- | --- |
| `(cliente)` — chave de **partição** | escolhe em qual nó o dado vive | só dá para consultar sabendo o cliente |
| `criado_em, pedido_id` — chaves de **agrupamento** | ordenam as linhas dentro da partição | a ordem é gravada em disco, não calculada na consulta |

E `CLUSTERING ORDER BY (criado_em DESC)` significa que os pedidos já ficam guardados do mais novo para o mais antigo. Ler "os últimos 10 pedidos da ana" é varrer o começo de uma partição — sem ordenação, sem índice secundário.

**É por isso que a tabela se chama `pedidos_por_cliente` e não `pedidos`.** No relacional você modela a entidade e escreve as consultas depois. Aqui a consulta vem primeiro e a tabela é derivada dela. Precisar também de "pedidos por data" significa **outra tabela**, com a mesma informação gravada de novo. Duplicar dado não é gambiarra neste modelo — é o projeto.

---

## Passo 4 — `INSERT` é `UPSERT`

```bash
C() { docker exec cassandra-lab cqlsh -e "$1"; }

C "INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status)
   VALUES ('ana', '2026-08-09 10:00:00', 11111111-1111-1111-1111-111111111111, 250, 'novo');
   SELECT cliente, criado_em, total, status FROM loja.pedidos_por_cliente WHERE cliente='ana';"
```

```
 cliente | criado_em                       | total | status
---------+---------------------------------+-------+--------
     ana | 2026-08-09 10:00:00.000000+0000 |   250 |   novo

(1 rows)
```

Agora **exatamente a mesma chave primária**, com outros valores:

```bash
C "INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status)
   VALUES ('ana', '2026-08-09 10:00:00', 11111111-1111-1111-1111-111111111111, 999, 'cancelado');
   SELECT cliente, criado_em, total, status FROM loja.pedidos_por_cliente WHERE cliente='ana';"
```

```
 cliente | criado_em                       | total | status
---------+---------------------------------+-------+-----------
     ana | 2026-08-09 10:00:00.000000+0000 |   999 | cancelado
```

Sem erro. Sem aviso. Ainda **uma** linha, e o pedido de 250 não existe mais em lugar nenhum.

O `WRITETIME` mostra que só sobrou a gravação mais recente:

```bash
C "SELECT cliente, total, status, WRITETIME(status) FROM loja.pedidos_por_cliente WHERE cliente='ana';"
```

```
 cliente | total | status    | writetime(status)
---------+-------+-----------+-------------------
     ana |   999 | cancelado |  1786318860793638
```

### E o `UPDATE` faz o mesmo ao contrário

```bash
C "UPDATE loja.pedidos_por_cliente SET total=77, status='novo'
   WHERE cliente='bruno' AND criado_em='2026-08-09 11:00:00' AND pedido_id=22222222-2222-2222-2222-222222222222;
   SELECT cliente, total, status FROM loja.pedidos_por_cliente WHERE cliente='bruno';"
```

```
 cliente | total | status
---------+-------+--------
   bruno |    77 |   novo

(1 rows)
```

O `bruno` nunca teve pedido. O `UPDATE` criou a linha.

**`INSERT` e `UPDATE` são o mesmo comando com sintaxes diferentes.** Cada gravação diz "no instante T, esta coluna vale isto", e a leitura devolve o T mais alto. Não há estado anterior a consultar, então não há como recusar.

---

## Passo 5 — `IF NOT EXISTS` e o que ele custa

É possível pedir ao Cassandra que verifique antes. Na mesma chave da `ana`, que já existe:

```bash
C "INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status)
   VALUES ('ana', '2026-08-09 10:00:00', 11111111-1111-1111-1111-111111111111, 1, 'duplicado') IF NOT EXISTS;"
```

```
 [applied] | cliente | criado_em                       | pedido_id                            | status    | total
-----------+---------+---------------------------------+--------------------------------------+-----------+-------
     False |     ana | 2026-08-09 10:00:00.000000+0000 | 11111111-1111-1111-1111-111111111111 | cancelado |   999
```

Numa chave livre:

```bash
C "INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status)
   VALUES ('carla', '2026-08-09 12:00:00', 33333333-3333-3333-3333-333333333333, 400, 'novo') IF NOT EXISTS;"
```

```
 [applied]
-----------
      True
```

Repare que o retorno `False` **vem acompanhado da linha que já estava lá**. Você descobre que falhou e por quê numa única ida ao banco.

Isso se chama *lightweight transaction* (LWT), e o adjetivo é enganoso: por baixo roda um consenso Paxos, com quatro idas e vindas entre réplicas em vez de uma.

### Medir o custo

O `cqlsh` é Python e gasta **~700 ms só para abrir a sessão** — mais do que qualquer escrita que você queira medir. O script paga esse custo uma vez e divide por N:

```bash
cat > /tmp/cass-write.sh <<'EOF'
#!/bin/bash
N=${1:-200}
ms() { echo $(( ( $(date +%s%N) - $1 ) / 1000000 )); }
gen_plain() { for i in $(seq 1 $N); do
  echo "INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status) VALUES ('bench', '2026-08-09 10:00:00', uuid(), $i, 'plain');"; done; }
gen_lwt() { for i in $(seq 1 $N); do
  echo "INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status) VALUES ('bench', '2026-08-09 10:00:00', uuid(), $i, 'lwt') IF NOT EXISTS;"; done; }
S=$(date +%s%N); cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1; BASE=$(ms $S)
S=$(date +%s%N); gen_plain | cqlsh >/dev/null 2>&1; T_P=$(ms $S)
S=$(date +%s%N); gen_lwt   | cqlsh >/dev/null 2>&1; T_L=$(ms $S)
echo "  startup cqlsh (custo fixo) ..... ${BASE}ms"
echo "  $N INSERT simples .............. ${T_P}ms  (~$(( (T_P-BASE)*1000/N ))us/escrita)"
echo "  $N INSERT IF NOT EXISTS (LWT) .. ${T_L}ms  (~$(( (T_L-BASE)*1000/N ))us/escrita)"
EOF
docker cp /tmp/cass-write.sh cassandra-lab:/tmp/cass-write.sh
MSYS_NO_PATHCONV=1 docker exec cassandra-lab bash /tmp/cass-write.sh 200
```

> **O `MSYS_NO_PATHCONV=1` não é decoração.** No Git Bash do Windows, sem ele, o `/tmp/cass-write.sh` do `docker exec` é convertido para caminho Windows e você recebe:
>
> ```
> bash: C:/Users/.../AppData/Local/Temp/cass-write.sh: No such file or directory
> ```
>
> O arquivo está lá dentro do contêiner; quem se perdeu foi o caminho. Nos demais ambientes o prefixo é inofensivo.

### O que você vai ver

Três execuções seguidas:

```
  startup cqlsh (custo fixo) ..... 698ms
  200 INSERT simples .............. 1309ms  (~3055us/escrita)
  200 INSERT IF NOT EXISTS (LWT) .. 1951ms  (~6265us/escrita)

  startup cqlsh (custo fixo) ..... 762ms
  200 INSERT simples .............. 1185ms  (~2115us/escrita)
  200 INSERT IF NOT EXISTS (LWT) .. 1823ms  (~5305us/escrita)

  startup cqlsh (custo fixo) ..... 692ms
  200 INSERT simples .............. 1188ms  (~2480us/escrita)
  200 INSERT IF NOT EXISTS (LWT) .. 1810ms  (~5590us/escrita)
```

| Operação | Custo medido | Razão |
| --- | --- | --- |
| `INSERT` simples | 2,1 – 3,1 ms/escrita | — |
| `INSERT ... IF NOT EXISTS` | 5,3 – 6,3 ms/escrita | **~2,2×** |

E este é o **piso**, não o teto: com um nó só, o Paxos não atravessa a rede — as quatro fases acontecem dentro do mesmo processo. Num cluster de três nós em zonas diferentes, cada fase vira ida e volta de rede.

Você está pagando ~2,2× para receber um `[applied] False` que, sem LWT, o banco jamais te daria. É uma troca defensável — desde que aplicada nas escritas onde a duplicata causa dano, e não em todas por precaução.

---

## Passo 6 — Gravações com comportamento próprio

### TTL: a gravação que se apaga sozinha

```bash
C "INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status)
   VALUES ('efemero', '2026-08-09 13:00:00', 44444444-4444-4444-4444-444444444444, 10, 'reservado') USING TTL 20;
   SELECT cliente, status, TTL(status) FROM loja.pedidos_por_cliente WHERE cliente='efemero';"
```

```
 cliente | status    | ttl(status)
---------+-----------+-------------
 efemero | reservado |          20
```

Espere e confira:

```bash
sleep 25
C "SELECT cliente, status FROM loja.pedidos_por_cliente WHERE cliente='efemero';"
```

```
 cliente | status
---------+--------


(0 rows)
```

O TTL é **por coluna**, não por linha — `TTL(status)` é uma pergunta sobre aquela célula. Dá para gravar um pedido permanente cuja coluna `reserva` expira em 15 minutos.

### Contadores: o tipo que só aceita soma

```bash
C "UPDATE loja.vendas_por_produto SET unidades = unidades + 3 WHERE produto='caneca';
   UPDATE loja.vendas_por_produto SET unidades = unidades + 5 WHERE produto='caneca';
   SELECT * FROM loja.vendas_por_produto;"
```

```
 produto | unidades
---------+----------
  caneca |        8
```

Agora tente inserir um valor:

```bash
C "INSERT INTO loja.vendas_por_produto (produto, unidades) VALUES ('teclado', 1);"
```

```
InvalidRequest: Error from server: code=2200 [Invalid query]
message="INSERT statements are not allowed on counter tables, use UPDATE instead"
```

Um dos raríssimos casos em que o Cassandra recusa uma gravação — e a razão é justamente a regra deste laboratório. Como toda escrita normal é uma sobrescrita, um contador não pode ser escrito por valor: duas somas concorrentes viraram uma só. `unidades + 3` é uma operação comutativa, que não depende de ler o valor atual. Por isso funciona, e por isso `INSERT` não.

Se você rodar o bloco do contador de novo, o valor vira 16, depois 24. **Contadores não são idempotentes** — repetir a mesma soma soma de novo. Numa fila com entrega "pelo menos uma vez", isso vira contagem inflada, e não há como distinguir a repetição do evento legítimo.

---

## Passo 7 — `BATCH` não é transação

Um lote comum funciona como esperado:

```bash
C "BEGIN BATCH
   INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status) VALUES ('lote', '2026-08-09 14:00:00', 55555555-5555-5555-5555-555555555555, 10, 'novo');
   INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status) VALUES ('lote', '2026-08-09 14:01:00', 66666666-6666-6666-6666-666666666666, 20, 'novo');
   APPLY BATCH;
   SELECT cliente, total FROM loja.pedidos_por_cliente WHERE cliente='lote';"
```

```
 cliente | total
---------+-------
    lote |    20
    lote |    10

(2 rows)
```

> Repare na ordem: o `total` 20 vem primeiro porque foi criado às 14:01, e a tabela foi declarada `CLUSTERING ORDER BY (criado_em DESC)`. A ordenação do Passo 3 aparece aqui sem nenhuma cláusula `ORDER BY`.

Os dois limites abaixo mostram o que o `BATCH` não é.

```bash
C "BEGIN BATCH
   UPDATE loja.vendas_por_produto SET unidades = unidades + 1 WHERE produto='caneca';
   INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status) VALUES ('x','2026-08-09 15:00:00', 77777777-7777-7777-7777-777777777777, 1, 'novo');
   APPLY BATCH;"
```

```
InvalidRequest: Error from server: code=2200 [Invalid query]
message="Counter and non-counter mutations cannot exist in the same batch"
```

```bash
C "BEGIN BATCH
   INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status) VALUES ('p1','2026-08-09 16:00:00', 88888888-8888-8888-8888-888888888888, 1, 'novo') IF NOT EXISTS;
   INSERT INTO loja.pedidos_por_cliente (cliente, criado_em, pedido_id, total, status) VALUES ('p2','2026-08-09 16:00:00', 99999999-9999-9999-9999-999999999999, 2, 'novo') IF NOT EXISTS;
   APPLY BATCH;"
```

```
InvalidRequest: Error from server: code=2200 [Invalid query]
message="Batch with conditions cannot span multiple partitions"
```

A segunda mensagem é a que fecha o assunto. "Reserve o estoque **e** crie o pedido, só se nenhum dos dois já existir" — a operação transacional mais banal que existe — **não é exprimível** quando as duas linhas estão em partições diferentes. O Paxos do LWT roda por partição; não há coordenador entre partições.

| O que o `BATCH` dá | O que ele não dá |
| --- | --- |
| todas as gravações com o mesmo timestamp | isolamento — outro cliente lê o meio do lote |
| garantia de que todas serão aplicadas, eventualmente | rollback se uma falhar |
| condição sobre **uma** partição | condição sobre várias partições |

Vale dizer o que mais causa incidente na prática: usar `BATCH` como otimização. Um lote de 500 escritas espalhadas por 500 partições obriga **um** coordenador a falar com todos os nós, em vez de 500 requisições paralelas equilibradas pelo driver. O lote fica mais lento que o laço que ele pretendia substituir.

---

## Passo 8 — Apagar também é gravar

```bash
C "DELETE FROM loja.pedidos_por_cliente WHERE cliente='nunca-existiu';
   SELECT count(*) FROM loja.pedidos_por_cliente WHERE cliente='nunca-existiu';"
```

```
 count
-------
     0

(1 rows)
```

O `DELETE` foi aceito. Não havia nada para apagar, e o comando funcionou.

Faz sentido dentro da regra do laboratório: como o Cassandra não lê antes de gravar, ele não tem como saber que a linha não existe. Então ele grava um marcador — um *tombstone* — dizendo "a partir do instante T, isto está apagado".

Duas consequências que doem em produção:

**Apagar aumenta o volume em disco.** O tombstone é um registro novo, e ele fica por `gc_grace_seconds` — **10 dias** por padrão, valor que o `DESCRIBE TABLE` mostra. Só depois disso a compactação pode removê-lo.

**Ler depois de apagar fica mais caro.** Uma leitura precisa juntar os tombstones e descartar o que eles cobrem. Uma partição onde se apaga muito fica lenta para ler *justamente porque* está vazia — o oposto da intuição.

```bash
docker exec cassandra-lab cqlsh -e "DESCRIBE TABLE loja.pedidos_por_cliente;" | grep gc_grace
```

```
    AND gc_grace_seconds = 864000
```

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir (`up -d --wait`) | 74 s |
| Passo 3 — keyspace, tabelas e `TRUNCATE` | 4 s |
| Passo 4 — upsert e `WRITETIME` | 6 s |
| Passo 5 — LWT + medição (3 execuções) | ~25 s |
| Passo 6 — TTL (com `sleep 25`) e contadores | 35 s |
| Passo 7 — `BATCH` (3 casos) | 6 s |
| Passo 8 — tombstone | 3 s |
| **Total** | **~5 min** |

A primeira execução soma o download da imagem `cassandra:4.1` (~370 MB).

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| `No such file or directory` num `/tmp/*.sh` | Git Bash converteu o caminho | prefixar `MSYS_NO_PATHCONV=1` |
| `CREATE KEYSPACE` falha logo após o `--wait` | CQL ainda não subiu | healthcheck com `cqlsh -e`, como no compose |
| Pedido sobrescrito e ninguém avisou | `INSERT` é upsert | `IF NOT EXISTS`, ciente do custo |
| `[applied] False` sem explicação | a linha existente vem no mesmo retorno | ler as colunas ao lado do `False` |
| `Batch with conditions cannot span multiple partitions` | LWT é por partição | remodelar para uma partição, ou aceitar |
| `INSERT statements are not allowed on counter tables` | contador só aceita soma | usar `UPDATE ... = ... + n` |
| Contador com valor maior que o esperado | somas não são idempotentes | é o resultado esperado ao repetir o bloco |
| Máquina fica sem memória | heap dimensionado pela RAM total | `MAX_HEAP_SIZE=512M` |
| Segunda execução com números diferentes | faltou o `TRUNCATE` do Passo 3 | rodar o bloco do Passo 3 antes |

---

## O que levar disso para o trabalho

**A tabela é a consulta.** `pedidos_por_cliente` só responde perguntas que começam por cliente. Precisar de "pedidos do dia" é criar outra tabela e gravar duas vezes. A pergunta a fazer no início não é "quais entidades existem?", e sim **"quais consultas esta aplicação vai fazer?"** — porque cada uma delas custa uma tabela e uma escrita adicional.

**Sem `IF NOT EXISTS`, você não tem chave única.** Vindo do relacional, é fácil supor que a chave primária protege contra duplicata. Ela não protege: ela apenas determina onde o dado mora. Se dois processos gravarem o mesmo pedido, um apaga o outro sem que ninguém saiba. Quando isso importa, o LWT é a única saída — e custou **~2,2×** com um nó só, mais num cluster real.

**Toda escrita é imutável, e isso muda o desenho.** Não há atualização no lugar: há uma nova versão com timestamp maior, e no caso do `DELETE`, um marcador que ocupa espaço por 10 dias. Uma tabela usada como fila — grava, lê, apaga — degrada de forma previsível. O modelo favorece dados que se acumulam e raramente somem.

Vale colocar os três laboratórios da AULA02 lado a lado, porque a mesma gravação recebe três tratamentos:

| Gravar duas vezes na mesma chave | O que acontece |
| --- | --- |
| Redis (`SET`) | sobrescreve; com `NX`, o retorno vazio avisa |
| MongoDB (`insertOne`) | cria dois documentos; com índice `unique`, lança `E11000` |
| Cassandra (`INSERT`) | **sobrescreve em silêncio**; só o LWT avisa, pagando ~2,2× |

Nos três, o comportamento seguro existe e **nenhum deles é o padrão.**

---

## Encerrar

```bash
docker compose down -v
```
