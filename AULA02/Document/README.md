# Laboratório 6 — Documento / MongoDB: o schema não sumiu, mudou de lugar

**Duração: ~4 minutos** (11 s subindo + ~3 min de operações).

## O que você vai fazer

Subir um MongoDB de nó único e gravar pedidos de uma loja: inserir, atualizar, fazer upsert, gravar em lote e medir o que cada nível de confirmação custa.

## O que você vai descobrir

**O MongoDB aceita quase tudo que você mandar — e é por isso que a decisão de modelagem fica mais pesada, não mais leve.** Você não declara colunas, não declara tipos e não declara a coleção. Grave um documento e o banco, a coleção e o `_id` passam a existir.

Só que "sem schema" não é "sem contrato". O contrato continua existindo; ele só saiu do `CREATE TABLE` e foi parar em três lugares menos visíveis:

| Onde o contrato vive | O que ele cobra | O que acontece se você não escrever |
| --- | --- | --- |
| Índices (`unique`) | unicidade | duplicatas entram em silêncio |
| Operadores (`$set` vs. documento cru) | preservar os campos existentes | `replaceOne` apaga o resto sem avisar |
| `writeConcern` | durabilidade da escrita | `acknowledged: true` sem nada em disco |

Os três aparecem neste laboratório. O segundo é o que mais dá prejuízo.

---

## Passo 1 — Criar o docker-compose.yml

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

```yaml
services:
  mongo-lab:
    image: mongo:7
    container_name: mongo-lab
    networks:
      - document-net
    healthcheck:
      # ping so prova que o processo respondeu. O que interessa e o servidor
      # aceitar comando de escrita, entao o teste roda um insert descartavel.
      test: ["CMD-SHELL", "mongosh --quiet --eval 'db.getSiblingDB(\"health\").probe.insertOne({t:new Date()}).acknowledged' | grep -q true"]
      interval: 5s
      timeout: 10s
      retries: 20
      start_period: 5s

networks:
  document-net:
    name: aula02-document-network
    driver: bridge
```

**Por que o healthcheck grava em vez de dar `ping`.** Um `ping` responde assim que o processo aceita conexão — inclusive durante a recuperação do journal, quando o servidor ainda recusa escrita. Como este laboratório é sobre gravar, o healthcheck grava: `insertOne(...).acknowledged` só devolve `true` quando o servidor está de fato aceitando escrita. O banco `health` existe só para isso e não interfere no resto.

## Passo 2 — Subir

```bash
docker compose up -d --wait
docker exec mongo-lab mongosh --quiet --eval 'db.version()'
```

```
7.0.39
```

**Na primeira execução isso leva ~75 s**, porque inclui o download da imagem `mongo:7` (~250 MB). Com a imagem em cache, são **11 s**.

---

## Passo 3 — Gravar sem criar nada antes

Não há `CREATE DATABASE`, não há `CREATE COLLECTION`. Comece do zero e grave:

```bash
M() { docker exec mongo-lab mongosh --quiet loja --eval "$1"; }

M 'db.dropDatabase()'

M 'db.pedidos.insertOne({ cliente: "ana", itens: [{ sku: "caneca", qtd: 2 }], total: 250, status: "novo" })'

M 'db.pedidos.insertMany([
  { cliente: "bruno", itens: [{ sku: "teclado", qtd: 1 }], total: 320, status: "novo" },
  { cliente: "ana",   itens: [{ sku: "mouse",   qtd: 1 }], total:  90, status: "novo" }
])'

M 'db.getMongo().getDBNames()'
```

O `dropDatabase()` na primeira linha é o que torna este laboratório repetível — sem ele, cada execução acumula documentos e os números dos passos seguintes deixam de bater.

### O que você vai ver

```
{
  acknowledged: true,
  insertedId: ObjectId('6a790ecb151a43e3729be616')
}
```

```
{
  acknowledged: true,
  insertedIds: { '0': ObjectId('...'), '1': ObjectId('...') }
}
```

```
[ 'admin', 'config', 'health', 'local', 'loja' ]
```

> Os valores de `ObjectId` são diferentes a cada execução — eles carregam o instante da criação. Só o formato importa aqui.

O banco `loja` aparece na listagem **porque você gravou nele**. Ele não existia antes do `insertOne`, e o `_id` também não: quem o gerou foi o driver, do lado do cliente, antes de a requisição sair.

Agora grave um documento com outra forma na mesma coleção:

```bash
M 'db.pedidos.insertOne({ cliente: "carla", total: "quatrocentos", entrega: { cidade: "Recife" }, cupom: null })'
```

`total` é número nos três primeiros documentos e texto neste. Há um campo novo (`entrega`), um campo nulo (`cupom`) e nenhum `itens`. O retorno é `acknowledged: true`.

**Nada nesse documento é inválido para o MongoDB.** Ele vira problema depois — no `$inc` que falha, na soma que devolve resultado errado, no relatório que ignora a linha. O banco aceitou; quem vai recusar é o seu código, meses depois.

---

## Passo 4 — As três formas de atualizar

Aqui está a armadilha mais cara deste laboratório.

### A) `updateOne` com operadores — o caso correto

```bash
M() { docker exec mongo-lab mongosh --quiet loja --eval "$1"; }

M 'db.pedidos.updateOne(
     { cliente: "ana", total: 250 },
     { $set: { status: "pago" }, $inc: { total: 10 }, $push: { itens: { sku: "adesivo", qtd: 1 } } }
   )'

M 'db.pedidos.findOne({ cliente: "ana", status: "pago" })'
```

```
{ acknowledged: true, insertedId: null, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 }
```

```
{
  _id: ObjectId('...'),
  cliente: 'ana',
  itens: [ { sku: 'caneca', qtd: 2 }, { sku: 'adesivo', qtd: 1 } ],
  total: 260,
  status: 'pago'
}
```

Três operações — trocar o status, somar 10 ao total, anexar um item ao array — numa única gravação atômica, sem ler o documento antes.

### B) Sem operador — recusado

```bash
M 'db.pedidos.updateOne({ cliente: "bruno" }, { status: "pago" })'
```

```
MongoInvalidArgumentError: Update document requires atomic operators
```

Erro claro, na sua cara. Ótimo.

### C) `replaceOne` — aceito, e destrói o resto

```bash
M 'db.pedidos.replaceOne({ cliente: "bruno" }, { cliente: "bruno", status: "pago" })'
M 'db.pedidos.findOne({ cliente: "bruno" })'
```

```
{ acknowledged: true, insertedId: null, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 }
```

```
{ _id: ObjectId('...'), cliente: 'bruno', status: 'pago' }
```

Compare com o que o `bruno` era antes: tinha `itens` e `total: 320`. **Sumiram os dois.**

E olhe o retorno: `matchedCount: 1, modifiedCount: 1`. É **idêntico** ao do caso A, que preservou tudo. Não há aviso, não há contagem de campos removidos, não há nada no retorno que distinga "atualizei um campo" de "apaguei o documento e escrevi outro no lugar".

> **Por que isso escapa em revisão de código.** `updateOne` e `replaceOne` têm a mesma assinatura e retornam o mesmo objeto. A diferença inteira está em o segundo argumento começar ou não com `$`. Um `$set` esquecido vira `replaceOne` na prática — e o caso B mostra que o driver protege você disso no `updateOne`, mas ninguém protege você de chamar `replaceOne` achando que atualiza.

---

## Passo 5 — Upsert: a gravação que pode repetir

```bash
M 'db.pedidos.updateOne({ cliente: "diana" }, { $set: { status: "novo", total: 500 } }, { upsert: true })'
M 'db.pedidos.updateOne({ cliente: "diana" }, { $set: { status: "novo", total: 500 } }, { upsert: true })'
```

```
{ acknowledged: true, insertedId: ObjectId('...'), matchedCount: 0, modifiedCount: 0, upsertedCount: 1 }
{ acknowledged: true, insertedId: null,            matchedCount: 1, modifiedCount: 0, upsertedCount: 0 }
```

A mesma chamada, dois efeitos diferentes — e **um único documento** no fim.

Vale ler o `modifiedCount: 0` da segunda linha com atenção: o documento foi encontrado e o `$set` foi aplicado, mas os valores já eram esses, então nada mudou no disco. `matchedCount: 1, modifiedCount: 0` é a assinatura de uma gravação idempotente que já convergiu.

É esse par de números que permite responder "o consumidor processou esta mensagem duas vezes?" sem consultar mais nada.

---

## Passo 6 — O índice único e a gravação em lote

Até aqui nada foi recusado por duplicidade, porque não havia contrato nenhum. Vamos criar um:

```bash
M() { docker exec mongo-lab mongosh --quiet loja --eval "$1"; }

M 'db.notas.drop()'
M 'db.notas.createIndex({ numero: 1 }, { unique: true, name: "uniq_numero" })'
M 'db.notas.insertOne({ numero: 1, cliente: "ana" })'
M 'db.notas.insertOne({ numero: 1, cliente: "bruno" })'
```

```
MongoServerError: E11000 duplicate key error collection: loja.notas index: uniq_numero dup key: { numero: 1 }
```

O `E11000` é o contrato cobrando. Sem o índice, os dois documentos entrariam, e a duplicata só apareceria no fechamento do mês.

### Lote ordenado — para no primeiro erro

```bash
M 'db.notas.insertMany([{numero:2},{numero:3},{numero:1},{numero:4}])'
M 'db.notas.find({}, {numero:1, _id:0}).sort({numero:1}).toArray()'
```

```
MongoBulkWriteError: E11000 duplicate key error collection: loja.notas index: uniq_numero dup key: { numero: 1 }
```

```
[ { numero: 1 }, { numero: 2 }, { numero: 3 } ]
```

O `2` e o `3` **entraram**. O `1` falhou. O `4` — que era válido — **nunca foi tentado**.

### Lote não ordenado — tenta todos

```bash
M 'db.notas.insertMany([{numero:4},{numero:1},{numero:5}], { ordered: false })'
M 'db.notas.find({}, {numero:1, _id:0}).sort({numero:1}).toArray()'
```

```
MongoBulkWriteError: E11000 duplicate key error collection: loja.notas index: uniq_numero dup key: { numero: 1 }
```

```
[ { numero: 1 }, { numero: 2 }, { numero: 3 }, { numero: 4 }, { numero: 5 } ]
```

### O que comparar

| Modo | Erro lançado | Documentos válidos gravados | Documentos válidos perdidos |
| --- | --- | --- | --- |
| `ordered: true` (padrão) | `E11000` | 2 e 3 | **4 — nem tentado** |
| `ordered: false` | `E11000` | 4 e 5 | nenhum |

**Os dois lançam a mesma exceção e deixam o banco em estados diferentes.** Um código que só faz `try/catch` em volta do `insertMany` não distingue os dois casos — e a diferença é quantos documentos válidos ficaram de fora.

`insertMany` não é transação. É um laço de inserções que o servidor executa por você, e a única coisa que `ordered` decide é se o laço para no primeiro tropeço.

---

## Passo 7 — Quanto custa confirmar a escrita

### A medição que não reproduz (leia antes de rodar)

A primeira versão deste passo mediu 200 escritas por nível, uma vez cada. Resultado:

```
  w:0 (sem resposta)     151 ms   (0.76 ms/escrita)
  w:1 j:false            373 ms   (1.86 ms/escrita)
  w:1 j:true (fsync)     8501 ms   (42.51 ms/escrita)
```

Parecia ótimo: o `fsync` custaria 23× mais. Rodando **de novo, sem mudar nada**:

```
  w:0 (sem resposta)     206 ms   (1.03 ms/escrita)
  w:1 j:false            7534 ms   (37.67 ms/escrita)
  w:1 j:true (fsync)     714 ms   (3.57 ms/escrita)
```

Os 7-8 segundos pularam para **outra linha**. Não é o nível de confirmação que custa caro — é uma pausa de ~7 s que cai em cima de qualquer janela de medição, provavelmente o *checkpoint* do WiredTiger somado ao disco virtualizado do Docker Desktop.

Se a medição tivesse parado na primeira rodada, este README publicaria "`j: true` custa 23×" — um número que a segunda execução desmente.

A correção é intercalar as rodadas e ficar com a mediana, para que uma pausa isolada não escolha o vencedor:

```bash
docker exec mongo-lab mongosh --quiet loja --eval '
const N = 100, ROUNDS = 5;
const wcs = { "w:0": {w:0}, "w:1 j:false": {w:1,j:false}, "w:1 j:true": {w:1,j:true} };
const res = { "w:0": [], "w:1 j:false": [], "w:1 j:true": [] };
db.bench.drop();
for (let r = 0; r < ROUNDS; r++) {
  for (const tag of Object.keys(wcs)) {
    const t = Date.now();
    for (let i = 0; i < N; i++) db.bench.insertOne({ i, tag }, { writeConcern: wcs[tag] });
    res[tag].push(Date.now() - t);
  }
}
for (const tag of Object.keys(res)) {
  const v = res[tag].slice().sort((a,b) => a-b);
  print(`  ${tag.padEnd(14)} rodadas=[${res[tag].join(", ")}]  mediana=${v[2]} ms  (${(v[2]/N).toFixed(2)} ms/escrita)`);
}'
```

### O que você vai ver

```
  w:0            rodadas=[114, 79, 79, 75, 70]      mediana=79 ms   (0.79 ms/escrita)
  w:1 j:false    rodadas=[181, 146, 123, 114, 117]  mediana=123 ms  (1.23 ms/escrita)
  w:1 j:true     rodadas=[366, 362, 335, 279, 322]  mediana=335 ms  (3.35 ms/escrita)
```

Segunda execução, para confirmar que agora reproduz:

```
  w:0            mediana=82 ms   (0.82 ms/escrita)
  w:1 j:false    mediana=121 ms  (1.21 ms/escrita)
  w:1 j:true     mediana=309 ms  (3.09 ms/escrita)
```

| Nível | Custo medido | O que você recebe de volta |
| --- | --- | --- |
| `w: 0` | ~0,8 ms | `acknowledged: false` — o driver não esperou resposta |
| `w: 1` | ~1,2 ms | o servidor aplicou em memória |
| `w: 1, j: true` | ~3,2 ms | está no journal, em disco |

A primeira coluna da tabela é a lista de rodadas: repare que **as rodadas de cada nível não se sobrepõem** — o pior `w:0` (114 ms) ainda é melhor que o melhor `j:true` (279 ms). É isso que separa um resultado de um ruído.

O `w: 0` merece um teste à parte, porque o retorno é diferente de tudo o que você viu até aqui:

```bash
M 'db.pedidos.insertOne({ cliente: "gabi", total: 60 }, { writeConcern: { w: 0 } })'
```

```
{ acknowledged: false, insertedId: ObjectId('...') }
```

**`acknowledged: false` com um `insertedId` preenchido.** O `_id` existe porque o driver o gerou localmente. Ele não é prova de nada: com `w: 0` o cliente não esperou o servidor dizer se gravou.

> **A armadilha maior:** o `mongosh` custa **~1200 ms só para abrir**, medido por `docker exec mongo-lab mongosh --quiet --eval '1'`. Rodar `time docker exec mongo-lab mongosh --eval 'db.x.insertOne({})'` mede o interpretador, não o banco — 1200 ms contra os ~1,2 ms da escrita, uma diferença de mil vezes. É por isso que o laço de medição roda **dentro** de uma única sessão `mongosh`. Mesma disciplina do `cqlsh` e do `aws` na [AULA01](../../README.md#como-medir-latência-sem-medir-a-ferramenta-errada).

---

## Passo 8 — O limite da atomicidade

Todas as gravações até aqui foram atômicas **em um documento**. Para mais de um:

```bash
docker exec mongo-lab mongosh --quiet loja --eval '
const s = db.getMongo().startSession();
try {
  s.startTransaction();
  s.getDatabase("loja").pedidos.insertOne({ cliente: "transacao" });
  s.commitTransaction();
  print("commit OK");
} catch (e) {
  print("FALHOU: " + e.message.split("\n")[0]);
} finally { s.endSession(); }'
```

```
FALHOU: This MongoDB deployment does not support retryable writes. Please add retryWrites=false to your connection string.
```

Transação multi-documento no MongoDB exige **replica set**, mesmo que de um nó só — ela é construída sobre o oplog, que um servidor avulso não mantém.

Repare que a mensagem **não fala em transação**. Ela fala em *retryable writes*, um recurso diferente que esbarra no mesmo pré-requisito. Quem topa com isso pela primeira vez costuma perder um tempo bom acrescentando `retryWrites=false` na string de conexão — o que não resolve, porque a causa é a topologia.

Enquanto o seu MongoDB for de nó único, **o documento é a maior unidade que você consegue gravar atomicamente.** É por isso que a modelagem por documento embute o que seria "linha filha" no relacional: os `itens` dentro do pedido não são conveniência de leitura, são a única forma de gravar pedido e itens juntos, sem risco de gravar metade.

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir (`up -d --wait`, imagem em cache) | 11 s |
| Subir na primeira vez (com download) | 75 s |
| Passo 3 — inserções e schema flexível | 8 s |
| Passo 4 — três formas de atualizar | 7 s |
| Passo 5 — upsert | 3 s |
| Passo 6 — índice único e lotes | 9 s |
| Passo 7 — medição (5 rodadas × 3 níveis) | ~6 s |
| Passo 8 — transação | 2 s |
| **Total** | **~4 min** |

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| `Update document requires atomic operators` | faltou `$set` no `updateOne` | é o driver te protegendo — acrescente o operador |
| Documento perdeu campos e não houve erro | `replaceOne` substitui o documento inteiro | usar `updateOne` com `$set` |
| Números do Passo 6 não batem | coleção acumulou entre execuções | rodar `db.notas.drop()` antes |
| Contagens do Passo 3 crescendo a cada rodada | faltou `db.dropDatabase()` | rodar a primeira linha do bloco |
| `j: true` ora rápido ora 40× mais lento | pausa de checkpoint caindo na janela | intercalar rodadas e usar a mediana |
| Escrita "instantânea" com `w: 0` | o cliente não esperou o servidor | conferir `acknowledged` no retorno |
| Latência de ~1200 ms em qualquer escrita | startup do `mongosh` | medir dentro de uma única sessão |
| `does not support retryable writes` | nó único não tem oplog | esperado; exige replica set |

---

## O que levar disso para o trabalho

**"Sem schema" descreve o servidor, não o sistema.** O MongoDB não recusa formatos, então a validação vai para outro lugar — e o risco é ela não ir para lugar nenhum. As duas perguntas que valem antes de subir uma coleção nova: *quais campos precisam ser únicos?* (vira índice `unique`) e *quais precisam existir?* (vira `$jsonSchema` de validação, ou código, ou nada — mas que seja decisão, não esquecimento).

**Retorno igual não significa efeito igual.** `updateOne` com `$set` e `replaceOne` devolvem o mesmo `matchedCount: 1, modifiedCount: 1`, e um deles apagou o documento. `insertMany` ordenado e não ordenado lançam o mesmo `E11000`, e deixam o banco diferente. Em nenhum dos casos o tipo da exceção te diz o que aconteceu — só a leitura de qual API você chamou.

**Confirmação tem preço, e ele é estável.** Diferente do que a [AULA01](../../README.md#consistência-forte-não-é-cara--até-a-rede-piorar) mostrou para consistência — barata em rede boa, proibitiva em rede ruim — o custo da durabilidade aqui é constante: ~2,6× entre `w:1` e `w:1, j:true`, sem rede envolvida. É `fsync`, e `fsync` custa o que custa. A escolha é por operação: `j: true` na confirmação de pagamento, `w: 0` no registro de clique.

**E o desenho do documento é o desenho da sua transação.** Sem replica set não existe gravação atômica de dois documentos. Isso não é limitação a contornar — é o que explica por que a modelagem por documento agrupa o que muda junto. Ao decidir se `itens` fica dentro do pedido ou numa coleção à parte, você está decidindo se conseguirá gravar os dois de uma vez.

---

## Encerrar

```bash
docker compose down -v
```
