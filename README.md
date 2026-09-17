# Laboratórios de bancos distribuídos — o que acontece de verdade quando você grava

Onze laboratórios, a maioria em Docker, divididos em cinco aulas.

**AULA01 — comportamento sob falha (PACELC).** Você sobe um banco distribuído, quebra a rede de propósito e mede o que acontece. Em vez de decorar que "Cassandra é AP", você vê na tela em que momento exato ele aceita ou recusa uma escrita. Os dois estresses são sempre os mesmos:

- **Partição de rede** — desconectar nós e ver se o banco continua respondendo (eixo PAC);
- **Latência injetada** — atrasar a rede em 2 segundos e medir quanto isso custa (eixo ELC).

**AULA02 — como cada modelo de dados grava.** Sem caos e sem cluster: um nó de cada motor, e a pergunta passa a ser o que o banco faz com a escrita que você mandou. Chave-valor, documento e família de colunas tratam a mesma gravação de três formas diferentes — e nas três há um comportamento seguro disponível que **não** é o padrão.

**AULA03 — processar e gravar em escala.** Três scripts PySpark constroem um pipeline inteiro na sua própria máquina: criação do DataFrame, partições, as transformações do dia a dia e um ETL que valida, rejeita com motivo e grava Parquet particionado em disco. Aqui o banco sai de cena e entra o motor de processamento — junto com duas descobertas: o Spark não executa o que você escreveu, e sim o plano que ele reescreveu; e uma linha pode sumir de um pipeline inteiro por causa de um `NULL` numa condição de validação.

**AULA04 — o mesmo ETL, com o dado morando na nuvem.** Continuação direta da AULA03: mesma estrutura de Extract/Transform/Load, mesma validação com motivo de rejeição, só que agora a entrada e a saída vivem na nuvem — dois laboratórios, dois exercícios, duas nuvens emuladas localmente sem precisar de conta real: Azure Blob Storage (via [floci-az](https://floci.io/az/)) e AWS S3 (via [floci](https://floci.io/aws/)). A pergunta muda de "como processar" para "como o processamento conversa com o armazenamento" — e a resposta, nos dois casos, é que o Spark local nunca fala com o armazenamento remoto diretamente: um SDK baixa, o Spark processa, o mesmo SDK sobe o resultado de volta.

**AULA05 — a mesma validação, organizada em camadas.** Muda a unidade de trabalho outra vez: em vez de um script com quatro funções, a arquitetura medalhão (Bronze/Silver/Gold) vira três **jobs independentes**, cada um com sua própria sessão Spark, lendo do disco o que o job anterior gravou. O dado é qualidade do ar num trimestre nas mesmas dez cidades de SC. A pergunta desta aula não é mais "onde o dado mora", é "onde termina uma unidade de trabalho e começa a próxima" — e por que separar por camada torna possível reprocessar uma regra de negócio na Gold sem tocar na ingestão.

Todos os números publicados aqui foram medidos executando os laboratórios. Onde o resultado contrariou o esperado, o texto diz o que aconteceu e por quê.

---

## Por onde começar

### AULA01 — o banco sob falha

| # | Laboratório | Emula | Motor | Duração |
| --- | --- | --- | --- | --- |
| 1 | [RDS / PostgreSQL](AULA01/RDS/README.md) | RDS Multi-AZ + Read Replica | PostgreSQL 15 | **~3 min** |
| 2 | [ElastiCache / Redis](AULA01/elastic-cache-redis/README.md) | ElastiCache | Redis 7.0 | **~2 min** |
| 3 | [Cassandra](AULA01/cassandra/README.md) | Keyspaces | Cassandra 4.1 | **~7 min** |
| 4 | [DynamoDB / ScyllaDB](AULA01/dynamodb-scylladb/README.md) | DynamoDB | ScyllaDB 5.2 (Alternator) | **~8 min** |

**Siga essa ordem.** Ela vai do caso mais simples de entender (uma réplica síncrona, uma assíncrona — o comportamento é óbvio depois que você vê) até o mais sutil (quóruns e níveis de consistência que se sobrepõem). Os laboratórios 3 e 4 fazem referência aos anteriores.

### AULA02 — o banco recebendo a sua escrita

| # | Laboratório | Modelo | Motor | Duração |
| --- | --- | --- | --- | --- |
| 5 | [Chave-valor / Redis](AULA02/KeyValue/README.md) | chave-valor | Redis 7.0 | **~3 min** |
| 6 | [Documento / MongoDB](AULA02/Document/README.md) | documento | MongoDB 7.0 | **~4 min** |
| 7 | [Família de colunas / Cassandra](AULA02/Collumn/README.md) | família de colunas | Cassandra 4.1 | **~5 min** |

Aqui a ordem também importa, e por um motivo diferente: ela vai do modelo que **menos** promete ao que mais parece um banco relacional sem ser um. Cada laboratório se apoia no anterior para mostrar o que mudou.

A AULA02 não depende da AULA01 — dá para começar por ela. Mas o Laboratório 5 fecha um argumento que o Laboratório 2 abriu, e o 7 usa o 3 como contraste.

### AULA03 — processar e gravar em escala

| # | Laboratório | Interface | Motor | Duração |
| --- | --- | --- | --- | --- |
| 8 | [PySpark do zero: DataFrame, funcionalidades e ETL local](AULA03/PYSPARK-BASICO/README.md) | quatro scripts `.py` no terminal | PySpark 3.5.3 local, sem Docker | **~10 min** |

É o único laboratório **sem Docker**: o Spark roda no seu próprio Python, e a saída vai para uma pasta no seu disco. É também um dos dois que trazem arquivos `.py` prontos no clone — os scripts e o módulo de configuração — porque aqui o que se estuda é o código, não a infraestrutura.

É escrito para quem **nunca abriu o Spark**: começa por um script de vinte linhas comentadas uma a uma, tem glossário, uma seção sobre como ler um traceback de PySpark, [12 exercícios com gabarito](AULA03/PYSPARK-BASICO/EXERCICIOS.md) e um [exercício de ETL com notas do ENEM em cidades de SC](AULA03/PYSPARK-BASICO/EXERCICIOS_ENEM.md), com lacunas para completar.

### AULA04 — o mesmo ETL, lendo e gravando na nuvem

| # | Laboratório | Interface | Motor | Duração |
| --- | --- | --- | --- | --- |
| 9 | [ETL com PySpark e Azure Blob Storage](AULA04/PYSPARK-AZURE-BLOB/README.md) | dois scripts `.py` no terminal + floci-az em Docker | PySpark 3.5.3 local + floci-az (emulador de Blob) | **~8 min** |
| 10 | [ETL com PySpark e AWS S3](AULA04/PYSPARK-AWS-S3/README.md) | dois scripts `.py` no terminal + floci em Docker | PySpark 3.5.3 local + floci (emulador de AWS) | **~8 min** |

Os dois dependem do ambiente Python da AULA03 (Python, Java, `winutils.exe`) mais um contêiner novo — [floci-az](https://floci.io/az/) para o Laboratório 9, [floci](https://floci.io/aws/) para o 10. Nenhum dos dois precisa de conta na nuvem real: o SDK que fala com o emulador é o mesmo que falaria com uma conta real, trocando só uma variável de ambiente.

O Laboratório 9 usa temperatura de um trimestre; o 10, consumo de energia elétrica por setor no mesmo trimestre — ambos nas mesmas dez cidades de Santa Catarina. A novidade não é o Spark — é onde a entrada e a saída moram, e por que o Spark local não fala com o armazenamento remoto diretamente (veja o "por que baixar em vez de ler direto" no README de [cada](AULA04/PYSPARK-AZURE-BLOB/README.md#por-que-baixar-em-vez-de-ler-direto) [laboratório](AULA04/PYSPARK-AWS-S3/README.md#por-que-baixar-em-vez-de-ler-direto)).

Os dois são **exercícios**, no mesmo formato do [ETL com notas do ENEM](AULA03/PYSPARK-BASICO/EXERCICIOS_ENEM.md) da AULA03: o script principal de cada um vem com quatro funções de Transform incompletas, e um `EXERCICIOS_*.md` ao lado traz o enunciado, a resposta esperada e o gabarito de cada uma — [`EXERCICIOS_AZURE.md`](AULA04/PYSPARK-AZURE-BLOB/EXERCICIOS_AZURE.md) e [`EXERCICIOS_AWS.md`](AULA04/PYSPARK-AWS-S3/EXERCICIOS_AWS.md).

### AULA05 — arquitetura medalhão, um job por camada

| # | Laboratório | Interface | Motor | Duração |
| --- | --- | --- | --- | --- |
| 11 | [Arquitetura medalhão: Bronze, Silver e Gold](AULA05/PYSPARK-MEDALHAO/README.md) | três scripts `.py` no terminal | PySpark 3.5.3 local, sem Docker | **~10 min** |

Volta a ser só local, como a AULA03 — sem Docker, sem SDK de nuvem. A mudança agora é estrutural: em vez de Extract/Transform/Load como funções de um script só, cada camada da arquitetura medalhão (Bronze/Silver/Gold) é um **job independente**, que lê do disco a saída do job anterior e para com uma mensagem clara se essa saída não existir.

O dado é qualidade do ar (PM2.5, PM10, CO) de um trimestre, nas mesmas dez cidades de SC dos laboratórios da AULA04. É também um **exercício**, com cinco funções incompletas espalhadas pelos três jobs — enunciado, resposta esperada e gabarito em [`EXERCICIOS_MEDALHAO.md`](AULA05/PYSPARK-MEDALHAO/EXERCICIOS_MEDALHAO.md).

### Em qualquer um dos cinco

**Rode um laboratório de cada vez.** Antes de passar para o próximo, encerre o atual:

```bash
docker compose down -v
```

A AULA01 leva **cerca de 20 minutos**, dos quais uns 11 são só esperando cluster subir. Se você tem uma janela curta, faça o 1 e o 2 (5 minutos somados) — eles já mostram os dois eixos.

A AULA02 leva **cerca de 12 minutos**, e sobe um contêiner por vez. Some o download das imagens na primeira execução (MongoDB ~250 MB, Cassandra ~370 MB).

A AULA03 leva **cerca de 10 minutos**, dos quais 2 são preparo único do ambiente local (um venv com PySpark). Depois disso, só rodar os quatro scripts: 41 s, 55 s, 99 s e 27 s. Os exercícios são à parte, e rendem mais uma hora.

A AULA04 leva **cerca de 8 minutos por laboratório** se o ambiente da AULA03 já existe — a diferença é só subir o emulador (`floci-az` ou `floci`) e instalar o SDK correspondente (`azure-storage-blob` ou `boto3`). Do zero, some o tempo de preparo da AULA03.

A AULA05 leva **cerca de 10 minutos** se o ambiente da AULA03 já existe — é só rodar os três jobs em sequência (ou `executar_pipeline.py`). Do zero, some o tempo de preparo da AULA03: não precisa de Docker nem de SDK de nuvem.

---

## O que cada teste faz — AULA01

Cada teste tem um identificador (T1, T2...) usado no README do laboratório.

### 1. RDS / PostgreSQL — [abrir](AULA01/RDS/README.md)

Um primário com duas réplicas: uma **síncrona** (Multi-AZ) e uma **assíncrona** (Read Replica). Os dois quadrantes convivem no mesmo cluster.

| ID | O que faz | Eixo | Duração | O que você vai ver |
| --- | --- | --- | --- | --- |
| — | Sobe o cluster e espera a replicação | — | 13 s | `standby_sync`=sync, `read_replica`=async |
| T1 | Confere a topologia | — | <1 s | 2 réplicas com contratos opostos |
| T2 | Cria a tabela | — | 1 s | tabela `transacoes` |
| T3 | Atrasa a réplica síncrona, depois a assíncrona | ELC | 110 s | **2042 ms/commit** vs. **5 ms/commit** |
| T4 | Desconecta a síncrona, depois a assíncrona | PAC | ~68 s | **trava sem responder** vs. **513 ms** |

### 2. ElastiCache / Redis — [abrir](AULA01/elastic-cache-redis/README.md)

Um primário e duas réplicas. Você usa o comando `WAIT` para forçar o Redis a esperar confirmação das réplicas.

| ID | O que faz | Eixo | Duração | O que você vai ver |
| --- | --- | --- | --- | --- |
| — | Sobe o cluster | — | 60 s | `connected_slaves:2` |
| T1 | Confere a topologia | — | 1 s | 1 primário + 2 réplicas |
| T2 | Compara `WAIT 0`, `WAIT 1` e `WAIT 2` com uma réplica lenta | ELC | ~10 s | 15 ms / 15 ms / **2024 ms** |
| T3 | Vai tirando réplicas: 2 → 1 → 0 | PAC | 58 s | `SET` **sempre aceito**; `WAIT` estoura o timeout |

### 3. Cassandra — [abrir](AULA01/cassandra/README.md)

Três nós, fator de replicação 3. Aqui você escolhe o nível de consistência **em cada consulta**.

| ID | O que faz | Eixo | Duração | O que você vai ver |
| --- | --- | --- | --- | --- |
| — | Sobe o anel (um nó por vez) | — | 251 s | 3 nós `UN` |
| T1 | Espera o anel fechar | — | 2 s | 3× `UN` |
| T2 | Cria keyspace com RF=3 | — | 3 s | keyspace `pacelc_lab` |
| T3 | Roda `ONE`, `QUORUM` e `ALL` com 3 → 2 → 1 nós | PAC | 78 s | tabela completa de quem responde e quem falha |
| T4 | Compara `ONE` e `ALL` com um nó lento | ELC | ~50 s | 9 ms vs. **2002 ms** por escrita |

### 4. DynamoDB / ScyllaDB — [abrir](AULA01/dynamodb-scylladb/README.md)

Três nós ScyllaDB expondo a API do DynamoDB. Você usa o `aws` CLI normalmente, como se fosse a AWS.

| ID | O que faz | Eixo | Duração | O que você vai ver |
| --- | --- | --- | --- | --- |
| — | Sobe o anel | — | 321 s | 3 nós `UN`, RF=3 |
| T1 | Espera o anel fechar | — | 2 s | 3× `UN` |
| T2 | Cria a tabela e grava um item | — | 3 s | tabela `Pagamentos` |
| T3 | Compara leitura eventual e forte com dois nós lentos | ELC | 76 s | 15 ms vs. **2012 ms** |
| T4 | Vai tirando nós: 3 → 2 → 1 | PAC | 80 s | responde na minoria, **para** ao perder o quórum |

---

## O que os quatro, juntos, mostram — AULA01

Rodando um por um, cada laboratório parece só confirmar um rótulo conhecido. Colocando lado a lado aparece algo mais útil: **os quatro sistemas colocam a decisão em lugares diferentes**.

| Sistema | Onde a decisão é tomada | Quem decide na prática |
| --- | --- | --- |
| PostgreSQL / RDS | No contrato de cada réplica (`synchronous_standby_names`) | Quem provisiona a infra |
| Redis / ElastiCache | Fixa no motor (sempre disponível); `WAIT` só observa | Ninguém — o motor não dá a opção |
| Cassandra | Em cada consulta (`CONSISTENCY`) | Quem escreve a query |
| ScyllaDB / Alternator | Na flag do servidor + na flag da requisição | Configuração e chamada, combinadas |

É por isso que rotular um banco como "AP" ou "CP" ajuda pouco. A pergunta útil é: **quem, no seu time, tem o poder de mudar isso — e essa pessoa sabe que tem?** Um dev que escreve `CONSISTENCY ALL` numa query quente está tomando uma decisão de arquitetura, provavelmente sem saber.

### O que acontece com a escrita quando a rede quebra

Mesmo estresse, quatro respostas bem diferentes:

| Sistema | Escrita quando as réplicas somem |
| --- | --- |
| Redis | **Aceita e não avisa** — mesmo com zero réplicas alcançáveis |
| PostgreSQL (réplica assíncrona fora) | Aceita — não há compromisso com aquele nó |
| Cassandra com `CONSISTENCY ONE` | Aceita — o nível pedido é atendido localmente |
| Cassandra com `QUORUM` / `ALL` | **Recusa** com erro `Unavailable` |
| ScyllaDB / Alternator | **Recusa** com erro `unavailable_exception` |
| PostgreSQL (standby síncrono fora) | **Trava sem responder** — nem aceita nem recusa |

Essas três formas de "não deu certo" exigem código cliente diferente:

- **Recusa explícita** é a melhor: você pega o erro e decide se tenta de novo ou cai num plano B.
- **Travar sem responder** é pior: consome a conexão e, sem `timeout` configurado, pendura a aplicação inteira.
- **Aceitar e não avisar** é o mais perigoso: não tem erro para tratar. A aplicação segue achando que gravou, e o dado some no próximo failover.

### Consistência forte não é cara — até a rede piorar

Em rede saudável, exigir consistência forte custou praticamente o mesmo que não exigir, nos quatro laboratórios:

| Laboratório | Rede saudável | Rede degradada (2000 ms) | Diferença |
| --- | --- | --- | --- |
| ScyllaDB (eventual → forte) | 12 → 13 ms | 15 → 2012 ms | **134×** |
| Cassandra (`ONE` → `ALL`) | ~0 → ~0 ms | 9 → 2002 ms | **222×** |
| Redis (`WAIT 0` → `WAIT 2`) | 16 → 18 ms | 15 → 2024 ms | **135×** |
| PostgreSQL (async → sync) | ~1 → ~1 ms | 5 → 2042 ms | **~500×** |

A conclusão prática: **o custo da consistência só aparece quando a infraestrutura está com problema** — ou seja, exatamente no pior momento. Um teste de carga em ambiente saudável não mostra isso. É por isso que esse custo costuma ser descoberto em produção, durante um incidente.

---

## O que cada laboratório faz — AULA02

Aqui não há caos injetado. Cada laboratório é uma sequência de passos numerados, e a rede fica intacta o tempo todo.

### 5. Chave-valor / Redis — [abrir](AULA02/KeyValue/README.md)

Um nó. Você grava nas cinco estruturas do Redis e depois mata o processo de duas formas diferentes.

| Passo | O que faz | Duração | O que você vai ver |
| --- | --- | --- | --- |
| — | Sobe o nó | 9 s | `PONG`, `save 3600 1 ...`, `appendonly no` |
| 3 | Grava em string, hash, lista, conjunto e conjunto ordenado | 8 s | `SADD` com 3 valores retorna **`2`** |
| 4 | `DECRBY`, `INCR` e `SET NX` — gravar sem ler antes | 6 s | segundo `SET NX` volta **vazio**, não erro |
| 5 | `MULTI/EXEC` com erro de tipo e com erro de sintaxe | 5 s | um aborta tudo, o outro **grava metade** |
| 6 | `restart` vs. `kill` vs. `SAVE` + `kill` | 45 s | escrita confirmada **some**, e `DBSIZE` volta 1 |

### 6. Documento / MongoDB — [abrir](AULA02/Document/README.md)

Um nó. Inserção, atualização, upsert, lote e o custo de cada nível de confirmação.

| Passo | O que faz | Duração | O que você vai ver |
| --- | --- | --- | --- |
| — | Sobe o nó | 11 s | `7.0.39` (75 s na primeira vez, com download) |
| 3 | Grava sem criar banco nem coleção | 8 s | `loja` passa a existir; `total` texto e número convivem |
| 4 | `updateOne` com `$set`, sem operador, e `replaceOne` | 7 s | `replaceOne` **apaga campos** com o mesmo retorno |
| 5 | Upsert duas vezes | 3 s | `upsertedCount: 1`, depois `modifiedCount: 0` |
| 6 | Índice `unique` e `insertMany` ordenado vs. não ordenado | 9 s | mesmo `E11000`, **estados finais diferentes** |
| 7 | Mede `w:0`, `w:1` e `w:1, j:true` | ~6 s | 0,79 / 1,23 / **3,35 ms** por escrita |
| 8 | Tenta uma transação | 2 s | recusada — nó único não tem oplog |

### 7. Família de colunas / Cassandra — [abrir](AULA02/Collumn/README.md)

Um nó, RF=1. Chave de partição, chave de agrupamento, e a gravação que sobrescreve sem avisar.

| Passo | O que faz | Duração | O que você vai ver |
| --- | --- | --- | --- |
| — | Sobe o nó | 74 s | 1 nó `UN` |
| 3 | Declara `PRIMARY KEY ((cliente), criado_em, pedido_id)` | 4 s | a consulta define a tabela, não o contrário |
| 4 | `INSERT` duas vezes na mesma chave; `UPDATE` em linha inexistente | 6 s | **sobrescreve em silêncio**; `UPDATE` cria a linha |
| 5 | `IF NOT EXISTS` e quanto ele custa | ~25 s | `[applied] False`; **~2,2×** mais caro |
| 6 | `USING TTL` e colunas `counter` | 35 s | a linha some sozinha; contador **não é idempotente** |
| 7 | `BATCH` normal, com contador e com condição | 6 s | condição em duas partições é **recusada** |
| 8 | `DELETE` de algo que nunca existiu | 3 s | aceito — e grava um marcador por 10 dias |

---

## O que os três, juntos, mostram — AULA02

A AULA01 comparou os bancos por como eles falham. A AULA02 os compara por uma pergunta mais banal, que aparece em qualquer sistema com uma fila que reentrega mensagem: **o que acontece se a mesma gravação chegar duas vezes?**

| Motor | Gravar duas vezes na mesma chave | Como obter o comportamento seguro | O que ele custa |
| --- | --- | --- | --- |
| Redis | sobrescreve | `SET ... NX` | nada |
| MongoDB | cria **dois** documentos | índice `unique` → `E11000` | um índice |
| Cassandra | **sobrescreve em silêncio** | `INSERT ... IF NOT EXISTS` | **~2,2×** por escrita |

As três colunas da direita têm algo em comum: **nenhuma delas é o padrão.** Em qualquer um dos três motores, o código que não pede nada de especial aceita a repetição sem reclamar — e cada um perde uma coisa diferente. O Redis perde a gravação anterior, o MongoDB duplica, o Cassandra apaga o pedido original sem deixar rastro.

### Confirmado não quer dizer gravado

Os três confirmam a escrita antes de ela estar segura em disco, e cada um oferece uma saída diferente:

| Motor | O padrão confirma quando | Como exigir o disco | Custo medido |
| --- | --- | --- | --- |
| Redis | o dado está em memória | `--appendonly yes` | não medido aqui |
| MongoDB | o servidor aplicou (`w: 1`) | `w: 1, j: true` | 1,23 → **3,35 ms** (~2,6×) |
| Cassandra | o commit log recebeu | já é o padrão | — |

O Laboratório 5 mostra a versão mais crua disso: uma escrita que respondeu `OK` desaparece num `docker kill`, e o banco volta com um estado antigo e perfeitamente plausível — sem erro, sem arquivo corrompido, sem alerta.

### O contrato não sumiu, mudou de lugar

É a diferença que mais confunde quem chega do relacional. Nenhum dos três tem `CREATE TABLE` fazendo o trabalho todo:

| Motor | Onde o contrato mora | O que acontece se ninguém escrever |
| --- | --- | --- |
| Redis | na estrutura escolhida (`SADD` deduplica, `INCR` só soma) | o retorno avisa — se alguém ler |
| MongoDB | em índices e validadores, criados à parte | nada é recusado |
| Cassandra | na `PRIMARY KEY`, que decide **onde** o dado mora | duplicata vira sobrescrita |

Nos três, quem não escreve o contrato não fica sem contrato: fica com o contrato implícito do motor, que é sempre o mais permissivo.

---

## O que o laboratório faz — AULA03

### 8. PySpark do zero: DataFrame, funcionalidades e ETL local — [abrir](AULA03/PYSPARK-BASICO/README.md)

Três scripts Python rodando com PySpark em `local[4]`, na sua máquina, sem contêiner nenhum. Cada um responde a uma pergunta diferente.

| Script | O que faz | O que você vai ver |
| --- | --- | --- |
| `00_primeiro_contato.py` | o menor programa PySpark possível, linha por linha | sessão, DataFrame, transformação, ação |
| `01_dataframe.py` | seis formas de criar um DataFrame, inspeção, transformação × ação | o `filter` não dispara nada; o `collect` dispara |
| `02_funcionalidades.py` | select, filter, nulos, `groupBy`, join, `Window`, SQL, partição, UDF | UDF **13×** mais lenta — e uma medição em que ela não roda |
| `03_etl_local.py` | ETL completo: CSV bruto → validação → Parquet particionado | 48 508 aprovadas, 1 492 rejeitadas **com o motivo de cada uma** |

### O que ele acrescenta às duas primeiras aulas

A AULA01 e a AULA02 tratam do banco recebendo uma escrita por vez. A AULA03 troca a unidade: aqui a escrita é um conjunto de arquivos, e o que decide o desempenho não é o motor — é como você dividiu o trabalho.

**Duas perguntas passam a valer mais que as configurações:**

| Pergunta | O que ela governa | Errar custa |
| --- | --- | --- |
| Quantas tarefas em paralelo? | `repartition(n)` | *skew* — uma tarefa segura o job inteiro |
| Por qual coluna vou filtrar? | `partitionBy("col")` | milhares de arquivos pequenos |

São perguntas diferentes, e o laboratório mostra, medindo, o que acontece ao responder uma com a outra: a mesma tabela sai com **24 arquivos** ou com **6**, dependendo de uma única linha antes do `write`.

E há um fio que liga as três primeiras aulas. Na AULA01, o banco aceitava escrita sem réplica alcançável; na AULA02, aceitava sem nada em disco; aqui, o Spark aceita uma medição que não mediu nada — e um filtro de validação deixa 505 linhas evaporarem sem erro nenhum. **Nos três casos o sistema devolve um resultado plausível, e a única defesa é conferir o que ele de fato fez** — o retorno do `WAIT`, o `DBSIZE` depois da queda, o plano do `explain()`, a conta de entradas e saídas do ETL.

---

## O que o laboratório faz — AULA04

### 9. ETL com PySpark e Azure Blob Storage — [abrir](AULA04/PYSPARK-AZURE-BLOB/README.md)

O mesmo formato Extract/Transform/Load da AULA03, com a entrada e a saída vivendo num container de Blob Storage (floci-az local) em vez do disco.

| Script | O que faz | O que você vai ver |
| --- | --- | --- |
| `00_conectar_blob.py` | conectar, subir, listar, baixar e apagar um blob, sem Spark | as quatro operações que o ETL usa, isoladas |
| `01_etl_temperaturas_sc.py` | **exercício**: Fonte/Extract/Load prontos, quatro funções de Transform para completar ([gabarito](AULA04/PYSPARK-AZURE-BLOB/EXERCICIOS_AZURE.md)) | 886 aprovadas, 34 rejeitadas **com o motivo de cada uma** |

### O que ele acrescenta à AULA03

A pergunta muda de "como processar" para "como o processamento conversa com o armazenamento". A resposta medida aqui: o Spark local em `local[4]` não fala com o Blob Storage diretamente — não há cluster distribuído para justificar um conector de sistema de arquivos (`wasb://`/`abfss://`), então o padrão é baixar com o SDK do Python, processar local, subir o resultado de volta com o mesmo SDK.

**A mesma armadilha do `NULL` da AULA03 aparece de novo, com outra cara**: a validação usa o mesmo `F.coalesce(regra, F.lit(False))` para não deixar uma condição indefinida apagar linha dos dois lados — só que agora o gatilho é uma leitura de sensor com o município vazio, não uma UF ausente. E aparece uma variante nova: das 11 leituras rejeitadas por `temperatura_min > temperatura_max`, 7 **também** violam a faixa física de temperatura — duas regras batendo na mesma linha, resolvidas pela ordem do `F.when(...)`, não por acaso.

A lição de portabilidade fica mais visível aqui do que em qualquer laboratório anterior: trocar o floci-az local por uma conta Azure real é mudar uma variável de ambiente (`AZURE_STORAGE_CONNECTION_STRING`) — nenhum script é reescrito, porque todo acesso ao Blob passa por quatro funções isoladas em `comum.py`.

### 10. ETL com PySpark e AWS S3 — [abrir](AULA04/PYSPARK-AWS-S3/README.md)

O mesmo ETL, terceira vez: agora a nuvem é AWS (S3, emulado pelo [floci](https://floci.io/aws/)), e o dado é consumo de energia elétrica por setor (residencial, comercial, industrial, rural) no mesmo trimestre das outras aulas, nas mesmas dez cidades de SC.

| Script | O que faz | O que você vai ver |
| --- | --- | --- |
| `00_conectar_s3.py` | conectar, subir, listar, baixar e apagar um objeto, sem Spark | as quatro operações que o ETL usa, isoladas |
| `01_etl_energia_sc.py` | **exercício**: Fonte/Extract/Load prontos, quatro funções de Transform para completar ([gabarito](AULA04/PYSPARK-AWS-S3/EXERCICIOS_AWS.md)) | 3538 aprovadas, 142 rejeitadas **com o motivo de cada uma** |

A estrutura é idêntica à do Laboratório 9 — normalizar, validar com `F.coalesce(regra, F.lit(False))`, enriquecer, resumir — porque essa é exatamente a lição: **a regra de negócio não muda com o provedor de nuvem.** O que muda é só a convenção de cada SDK para "usar o serviço de verdade": o Blob troca ao **definir** uma connection string; o S3 troca ao **esvaziar** `AWS_ENDPOINT_URL` (`""`, não ausente) e deixar o boto3 resolver o endpoint da AWS real sozinho. Duas nuvens, duas convenções — e nenhuma das duas é intuitiva sem ler a documentação do respectivo SDK uma vez.

Uma das cinco regras de validação (`unidades_consumidoras > 0`) rejeita **zero** linhas no dado gerado. Fica registrada de propósito: uma regra de validação com contagem zero não é uma regra inútil — é a que vai pegar o dia em que o sistema upstream mudar de comportamento.

---

## O que o laboratório faz — AULA05

### 11. Arquitetura medalhão: Bronze, Silver e Gold — [abrir](AULA05/PYSPARK-MEDALHAO/README.md)

Três jobs independentes, cada um com sua própria `SparkSession`, cada um lendo do disco o que o anterior gravou. O dado é qualidade do ar (PM2.5, PM10, CO) de um trimestre nas mesmas dez cidades de SC.

| Job | Camada | O que faz | O que você vai ver |
| --- | --- | --- | --- |
| `01_bronze_ingestao.py` | Bronze | gera o dado, acrescenta proveniência ([exercício 1](AULA05/PYSPARK-MEDALHAO/EXERCICIOS_MEDALHAO.md)), grava sem filtrar nada | 923 leituras — idêntico ao CSV bruto |
| `02_silver_limpeza.py` | Silver | normaliza, valida com motivo ([exercícios 2–3](AULA05/PYSPARK-MEDALHAO/EXERCICIOS_MEDALHAO.md)) | 889 aprovadas, 31 rejeitadas **com o motivo de cada uma** |
| `03_gold_agregados.py` | Gold | classifica, junta com região, resume ([exercícios 4–5](AULA05/PYSPARK-MEDALHAO/EXERCICIOS_MEDALHAO.md)) | média de PM2.5 e dias moderados-ou-piores por região |

### O que ele acrescenta às aulas anteriores

A AULA03 e a AULA04 resolveram Extract/Transform/Load como funções de um script só. Aqui a unidade muda para o **job**: cada camada é um programa que sobe sua própria JVM, lê a saída física do job anterior e para com uma mensagem clara (`Bronze nao encontrada em ...`) se essa saída não existir — em vez de um `AnalysisException` sem contexto tentando ler um caminho que não foi escrito.

A Bronze guarda **as 923 leituras geradas, sem exceção** — inclusive as fisicamente impossíveis (PM2.5 maior que PM10) e as duplicatas. Só a Silver decide o que é válido. É uma escolha deliberada: se a Bronze já filtrasse, a pergunta "esse valor absurdo veio de onde e quando" perderia a resposta, porque o dado que a originou nunca teria sido gravado em lugar nenhum.

O preço da separação por camada é medido, não hipotético: cada job paga o custo de subir uma JVM (a mesma fração de segundos que já aparecia nos "~10 minutos" do laboratório da AULA03). Para um pipeline deste tamanho, três inicializações de Spark custam pouco perto do que se ganha — poder reprocessar só a Gold quando uma regra de negócio muda, sem tocar na ingestão nem na validação.

---

## Antes de começar

| O que você precisa | Versão testada |
| --- | --- |
| Docker Engine | 28.4.0 |
| Docker Compose | v2.39.2 |
| Pumba (injeta a latência) | `gaiaadm/pumba` — é uma imagem, não precisa instalar; só a AULA01 usa |
| floci-az (emulador de Azure) | `floci/floci-az:latest` — imagem, não precisa instalar; só o Laboratório 9 usa |
| floci (emulador de AWS) | `floci/floci:latest` — imagem, não precisa instalar; só o Laboratório 10 usa |

Não instale mais nada. Os clientes de linha de comando (`aws`, `cqlsh`, `redis-cli`, `psql`, `mongosh`) rodam dentro dos contêineres — exceto na AULA04, onde é o próprio script Python (via SDK) que fala com o emulador.

### Os arquivos de configuração não vêm no clone

O `.gitignore` exclui os `docker-compose.yml` e os scripts `.sh`. Isso é intencional, mas significa que **clonar o repositório não é suficiente para rodar**.

O conteúdo completo de cada arquivo está no README do laboratório. Antes de executar:

1. Abra o README do laboratório;
2. Copie o bloco de código indicado;
3. Salve com o nome que o README indica, na pasta do laboratório.

Cada README diz exatamente qual arquivo criar e onde.

### Se você usa Git Bash no Windows

O Git Bash converte caminhos como `/var/run/docker.sock` para caminho Windows, e o Pumba quebra:

```
docker: Error response from daemon: mkdir C:\Program Files\Git\var: Acesso negado.
```

A correção é colocar `MSYS_NO_PATHCONV=1` na frente de todo comando `docker` que tenha caminho absoluto:

```bash
MSYS_NO_PATHCONV=1 docker run -d --rm -v /var/run/docker.sock:/var/run/docker.sock gaiaadm/pumba ...
```

**Isso não vale só para o Pumba.** Rodar um script dentro do contêiner esbarra no mesmo problema, e o erro engana porque parece que o arquivo não foi copiado:

```
bash: C:/Users/.../AppData/Local/Temp/cass-write.sh: No such file or directory
```

```bash
MSYS_NO_PATHCONV=1 docker exec cassandra-lab bash /tmp/cass-write.sh
```

O arquivo está lá dentro do contêiner; quem se perdeu foi o caminho. No PowerShell, WSL, Linux e macOS não precisa desse prefixo. Os comandos nos READMEs já vêm com ele — se você não usa Git Bash, pode ignorar.

---

## Três regras que valem para todos os laboratórios

Não são preferências de estilo. Cada uma corrige um erro que aconteceu de verdade durante a validação.

### 1. Você pode rodar tudo de novo sem dar erro

Todos os comandos são idempotentes: dá para repetir a sequência inteira quantas vezes quiser. Isso exige proteção em três lugares.

Ao criar schema, use `IF NOT EXISTS` (ou teste antes de criar) — senão a segunda execução falha com "já existe".

Ao gravar dado de exemplo, comece limpando. Sem isso a segunda execução não dá erro, mas produz números diferentes dos publicados — o que é pior, porque parece que o laboratório falhou. Cada motor tem seu comando:

```bash
docker exec redis-lab     redis-cli FLUSHALL                                  # Laboratório 5
docker exec mongo-lab     mongosh --quiet loja --eval 'db.dropDatabase()'     # Laboratório 6
docker exec cassandra-lab cqlsh -e "TRUNCATE loja.pedidos_por_cliente;"       # Laboratório 7
```

No Laboratório 8 isso já está embutido: as escritas usam `mode("overwrite")`, a entrada é gerada com semente fixa e só na primeira execução. Os três scripts foram executados duas vezes seguidas, e a saída do ETL saiu idêntica nas duas.

Ao mexer na rede, use as funções abaixo. Sem o `2>/dev/null`, desconectar um nó já desconectado dá erro e o script para:

```bash
net_out() { docker network disconnect <rede> "$1" 2>/dev/null; }
net_in()  { docker network connect    <rede> "$1" 2>/dev/null; }
```

### 2. `--wait` não significa que o cluster está pronto

`docker compose up -d --wait` só garante que os processos responderam. Não garante que o cluster se formou. Na prática:

- Um nó Cassandra ou ScyllaDB pode passar no healthcheck ainda em estado `UJ` (entrando no anel);
- No PostgreSQL, as sessões de cópia inicial (`pg_basebackup`) aparecem como se fossem réplicas, antes de existir replicação.

Se você começar a medir nesse momento, os números não querem dizer nada. Cada README traz o comando de espera correto para o seu motor — use antes de seguir.

Nos laboratórios de nó único da AULA02 o `--wait` **é** suficiente, mas só porque os healthchecks foram escritos para isso: o do MongoDB grava um documento em vez de dar `ping`, e o do Cassandra exige `cqlsh` respondendo, não apenas `nodetool status`. Um healthcheck que só testa o processo devolve o controle antes de o banco aceitar escrita.

### 3. Espere 30 segundos entre um teste de caos e o próximo

Quando você para o Pumba, a regra de latência (`netem`) **não some na hora**. Se emendar o próximo teste, ele herda o atraso do anterior.

Foi assim que a primeira rodada do laboratório do RDS produziu um resultado completamente errado: a réplica assíncrona apareceu com 1392 ms/commit, quando o valor real é 5 ms. O atraso era resíduo do teste anterior.

```bash
docker stop pumba-<lab> 2>/dev/null
sleep 30
```

---

## Como medir latência sem medir a ferramenta errada

Isso derruba muita medição. Os clientes de linha de comando demoram para iniciar — às vezes mais do que a operação que você quer medir:

| Cliente | Custo só para iniciar |
| --- | --- |
| `mongosh` (Node.js) via `docker exec` | ~1200 ms |
| `aws` (AWS CLI v2, escrito em Python) | ~1001 ms |
| `cqlsh` (Python) | ~700 – 800 ms |
| `psql` via `docker exec` | ~500 ms |
| `redis-cli` (C) | desprezível |

Ou seja: rodar `time aws dynamodb get-item` mede principalmente o tempo de subir o interpretador Python, não o banco. A operação real leva ~12 ms. No `mongosh` a distorção é ainda maior: ~1200 ms de startup para uma escrita de ~1,2 ms — **mil vezes** o que se pretendia medir.

Os laboratórios contornam isso de duas formas, ambas explicadas nos READMEs:

- **Amortizar**: medir o custo fixo uma vez e rodar N operações numa única sessão, dividindo o resultado;
- **Descer um nível**: falar HTTP direto com `curl`, sem cliente intermediário.

Se você adaptar os testes, mantenha uma das duas. Medição de latência com cliente pesado por chamada não serve para comparar nada.

### Uma rodada só não é uma medição

Amortizar o startup resolve metade do problema. A outra metade apareceu no Laboratório 6: medindo os três níveis de `writeConcern` **uma vez cada**, uma pausa de ~7 s caiu dentro da janela de um deles e o transformou no mais lento — em execuções diferentes, num nível diferente. A conclusão se invertia conforme a rodada.

A correção é intercalar as rodadas e ficar com a mediana. O sinal só é confiável quando as rodadas de um nível **não se sobrepõem** às do outro:

```
w:0          rodadas=[114, 79, 79, 75, 70]      mediana=79 ms
w:1 j:false  rodadas=[181, 146, 123, 114, 117]  mediana=123 ms
w:1 j:true   rodadas=[366, 362, 335, 279, 322]  mediana=335 ms
```

Publicar a lista de rodadas junto com a mediana não é excesso de zelo: é o que permite a quem lê distinguir um resultado de um acaso.

---

## Ao terminar

Encerre o laboratório e, se for da AULA01, remova o injetor de caos:

```bash
docker rm -f $(docker ps -aq --filter name=pumba) 2>/dev/null
docker compose down -v
```

Para conferir que não ficou nada rodando antes de começar o próximo:

```bash
docker ps --format "{{.Names}}"
docker network ls --filter name=pacelc --format "{{.Name}}"
docker network ls --filter name=aula0 --format "{{.Name}}"
```

As três listas devem sair vazias. As redes da AULA01 são prefixadas `pacelc-`; as da AULA02 e da AULA03, `aula02-` e `aula03-`.
