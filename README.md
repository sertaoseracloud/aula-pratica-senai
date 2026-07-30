# Laboratórios PACELC — testando na prática como bancos distribuídos se comportam sob falha

Quatro laboratórios em Docker. Em cada um você sobe um banco distribuído, quebra a rede de propósito e mede o que acontece. O objetivo é sair da teoria: em vez de decorar que "Cassandra é AP", você vai ver na tela em que momento exato ele aceita ou recusa uma escrita.

Os dois estresses são sempre os mesmos:

- **Partição de rede** — desconectar nós e ver se o banco continua respondendo (eixo PAC);
- **Latência injetada** — atrasar a rede em 2 segundos e medir quanto isso custa (eixo ELC).

Todos os números publicados aqui foram medidos executando os laboratórios. Onde o resultado contrariou o esperado, o texto diz o que aconteceu e por quê.

---

## Por onde começar

| # | Laboratório | Emula | Motor | Duração |
| --- | --- | --- | --- | --- |
| 1 | [RDS / PostgreSQL](AULA01/RDS/README.md) | RDS Multi-AZ + Read Replica | PostgreSQL 15 | **~3 min** |
| 2 | [ElastiCache / Redis](AULA01/elastic-cache-redis/README.md) | ElastiCache | Redis 7.0 | **~2 min** |
| 3 | [Cassandra](AULA01/cassandra/README.md) | Keyspaces | Cassandra 4.1 | **~7 min** |
| 4 | [DynamoDB / ScyllaDB](AULA01/dynamodb-scylladb/README.md) | DynamoDB | ScyllaDB 5.2 (Alternator) | **~8 min** |

**Siga essa ordem.** Ela vai do caso mais simples de entender (uma réplica síncrona, uma assíncrona — o comportamento é óbvio depois que você vê) até o mais sutil (quóruns e níveis de consistência que se sobrepõem). Os laboratórios 3 e 4 fazem referência aos anteriores.

**Rode um de cada vez.** Cada laboratório sobe 3 ou 4 contêineres, e Cassandra e ScyllaDB consomem bastante memória. Antes de passar para o próximo, encerre o atual:

```bash
docker compose down -v
```

Executar os quatro leva **cerca de 20 minutos**, dos quais uns 11 são só esperando cluster subir. Se você tem uma janela curta, faça o 1 e o 2 (5 minutos somados) — eles já mostram os dois eixos.

---

## O que cada teste faz

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

## O que os quatro, juntos, mostram

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

## Antes de começar

| O que você precisa | Versão testada |
| --- | --- |
| Docker Engine | 28.4.0 |
| Docker Compose | v2.39.2 |
| Pumba (injeta a latência) | `gaiaadm/pumba` — é uma imagem, não precisa instalar |

Não instale mais nada. Os clientes de linha de comando (`aws`, `cqlsh`, `redis-cli`, `psql`) rodam dentro dos contêineres.

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

No PowerShell, WSL, Linux e macOS não precisa desse prefixo. Os comandos nos READMEs já vêm com ele — se você não usa Git Bash, pode ignorar.

---

## Três regras que valem para os quatro laboratórios

Não são preferências de estilo. Cada uma corrige um erro que aconteceu de verdade durante a validação.

### 1. Você pode rodar tudo de novo sem dar erro

Todos os comandos são idempotentes: dá para repetir a sequência inteira quantas vezes quiser. Isso exige proteção em dois lugares.

Ao criar schema, use `IF NOT EXISTS` (ou teste antes de criar) — senão a segunda execução falha com "já existe".

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
| `aws` (AWS CLI v2, escrito em Python) | ~1001 ms |
| `cqlsh` (Python) | ~800 ms |
| `psql` via `docker exec` | ~500 ms |
| `redis-cli` (C) | desprezível |

Ou seja: rodar `time aws dynamodb get-item` mede principalmente o tempo de subir o interpretador Python, não o banco. A operação real leva ~12 ms.

Os laboratórios contornam isso de duas formas, ambas explicadas nos READMEs:

- **Amortizar**: medir o custo fixo uma vez e rodar N operações numa única sessão, dividindo o resultado;
- **Descer um nível**: falar HTTP direto com `curl`, sem cliente intermediário.

Se você adaptar os testes, mantenha uma das duas. Medição de latência com cliente pesado por chamada não serve para comparar nada.

---

## Ao terminar

Encerre o laboratório e remova o injetor de caos:

```bash
docker rm -f $(docker ps -aq --filter name=pumba) 2>/dev/null
docker compose down -v
```

Para conferir que não ficou nada rodando antes de começar o próximo:

```bash
docker ps --format "{{.Names}}"
docker network ls --filter name=pacelc
```

As duas listas devem sair vazias.
