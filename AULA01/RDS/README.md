# Laboratório 1 — RDS / PostgreSQL: réplica síncrona vs. assíncrona

**Duração: ~3 minutos** (13 s subindo o cluster + 179 s de testes).

## O que você vai fazer

Subir um PostgreSQL primário com **duas réplicas configuradas de formas opostas**:

- `rds-standby-sync` — replicação **síncrona**. É o que a AWS chama de **Multi-AZ**. O primário só confirma um commit depois que essa réplica avisar que recebeu.
- `rds-read-replica` — replicação **assíncrona**. É o que a AWS chama de **Read Replica**. O primário confirma na hora e manda o dado depois.

Depois você vai atrasar a rede de cada uma e desconectar cada uma, medindo o efeito.

A graça do arranjo é que os dois comportamentos convivem no mesmo cluster, sob a mesma carga. Quando os resultados derem diferente, não vai ter dúvida sobre a causa: a única coisa que muda entre as duas réplicas é o contrato de replicação.

## O que você vai descobrir

- Atrasar a réplica **síncrona** em 2 segundos faz cada commit custar **2042 ms** em vez de 1 ms.
- Atrasar a réplica **assíncrona** com o mesmo atraso **não muda nada** (5 ms).
- Desconectar a réplica **síncrona** faz o `INSERT` **travar sem responder**.
- Desconectar a réplica **assíncrona** não afeta nada.

Todos esses números foram medidos, não estimados.

---

## Passo 1 — Criar o script de inicialização

O PostgreSQL oficial sobe como banco isolado. Para virar primário de replicação, precisa de configuração no primeiro boot.

> Este arquivo **não vem no clone** (está no `.gitignore`). Crie `01-init.sh` nesta pasta com o conteúdo abaixo.

```bash
#!/bin/bash
set -e

# Criação do usuário dedicado para o transporte de logs
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE repl_user WITH REPLICATION PASSWORD 'repl_pass' LOGIN;
EOSQL

# Autorização de conexões externas exclusivas para o processo de replicação
echo "host replication repl_user all md5" >> "$PGDATA/pg_hba.conf"

# Configuração do motor para operar no quadrante PC/EC (Multi-AZ Simulator)
cat >> "$PGDATA/postgresql.conf" <<-EOF
wal_level = replica
max_wal_senders = 10
synchronous_commit = on
synchronous_standby_names = 'standby_sync'
EOF
```

**A linha que faz o laboratório funcionar é esta:**

```
synchronous_standby_names = 'standby_sync'
```

Ela diz: "a réplica que se conectar usando o nome `standby_sync` é síncrona". Qualquer outra réplica que conectar com nome diferente continua assíncrona. É só isso que separa os dois comportamentos — e é por isso que dá para ter os dois no mesmo cluster.

Dê permissão de execução:

```bash
chmod +x 01-init.sh
```

## Passo 2 — Criar o docker-compose.yml

> Também **não vem no clone**. Crie `docker-compose.yml` nesta pasta com o conteúdo abaixo.

```yaml
services:
  rds-primary:
    image: postgres:15
    container_name: rds-primary
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin_pass
      POSTGRES_DB: pacelc_rds
    volumes:
      - ./01-init.sh:/docker-entrypoint-initdb.d/01-init.sh
    ports:
      - "5432:5432"
    networks:
      - rds-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d pacelc_rds"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 10s

  rds-standby-sync:
    image: postgres:15
    container_name: rds-standby-sync
    environment:
      PGPASSWORD: repl_pass
    command: >
      bash -c "
      set -e;
      until pg_isready -h rds-primary -p 5432; do sleep 2; done;
      rm -rf /var/lib/postgresql/data/*;
      pg_basebackup -h rds-primary -D /var/lib/postgresql/data -U repl_user -vP -w;
      touch /var/lib/postgresql/data/standby.signal;
      echo \"primary_conninfo = 'host=rds-primary port=5432 user=repl_user password=repl_pass application_name=standby_sync'\" >> /var/lib/postgresql/data/postgresql.conf;
      exec docker-entrypoint.sh postgres
      "
    networks:
      - rds-network
    depends_on:
      rds-primary:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U repl_user -d postgres || pg_isready"]
      interval: 5s
      timeout: 5s
      retries: 40
      start_period: 20s

  rds-read-replica:
    image: postgres:15
    container_name: rds-read-replica
    environment:
      PGPASSWORD: repl_pass
    command: >
      bash -c "
      set -e;
      until pg_isready -h rds-primary -p 5432; do sleep 2; done;
      rm -rf /var/lib/postgresql/data/*;
      pg_basebackup -h rds-primary -D /var/lib/postgresql/data -U repl_user -vP -w;
      touch /var/lib/postgresql/data/standby.signal;
      echo \"primary_conninfo = 'host=rds-primary port=5432 user=repl_user password=repl_pass application_name=read_replica'\" >> /var/lib/postgresql/data/postgresql.conf;
      exec docker-entrypoint.sh postgres
      "
    networks:
      - rds-network
    depends_on:
      rds-primary:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U repl_user -d postgres || pg_isready"]
      interval: 5s
      timeout: 5s
      retries: 40
      start_period: 20s

networks:
  rds-network:
    name: pacelc-rds-network
    driver: bridge
```

Repare no `application_name` dentro do `primary_conninfo` de cada réplica: uma conecta como `standby_sync` (e vira síncrona, porque bate com o nome no `01-init.sh`), a outra como `read_replica` (e fica assíncrona).

## Passo 3 — Subir o cluster

```bash
docker compose up -d --wait
```

**Não comece a medir ainda.** O `--wait` confirma que os processos responderam, mas a replicação pode não ter sido estabelecida. Rode o comando de espera abaixo:

```bash
for i in $(seq 1 60); do
  n=$(docker exec rds-primary psql -U admin -d pacelc_rds -tAc \
      "SELECT count(*) FROM pg_stat_replication WHERE state='streaming';" | tr -d '\r ')
  [ "$n" = "2" ] && { echo "2 replicas em streaming"; break; }
  sleep 5
done
```

> **Por que `WHERE state='streaming'` importa.** Enquanto as réplicas copiam os dados iniciais (`pg_basebackup`), essas sessões de cópia também aparecem em `pg_stat_replication`, com `state='backup'`. Se você contar sem filtrar, vai encontrar 2 e achar que está pronto — quando na verdade ainda não existe replicação nenhuma. Esse erro aconteceu na validação.

Confirme a topologia:

```bash
docker exec rds-primary psql -U admin -d pacelc_rds \
  -c "SELECT application_name, state, sync_state FROM pg_stat_replication ORDER BY 1;"
```

Você deve ver exatamente isto:

```
 application_name |   state   | sync_state
------------------+-----------+------------
 read_replica     | streaming | async
 standby_sync     | streaming | sync
```

Se `sync_state` vier `async` nas duas, o `application_name` não bateu com o nome no `01-init.sh`. Confira a grafia.

## Passo 4 — Criar a tabela

```bash
docker exec rds-primary psql -U admin -d pacelc_rds \
  -c "CREATE TABLE IF NOT EXISTS transacoes (id serial PRIMARY KEY, carga varchar(100));"
```

O `IF NOT EXISTS` deixa você repetir o laboratório sem erro.

---

## Teste T3 — Quanto custa a replicação síncrona (eixo ELC)

### Primeiro, o script de medição

Cada `docker exec ... psql` gasta **~500 ms** só para criar o processo e conectar. Se você medir um `INSERT` sozinho, vai medir principalmente isso. O script abaixo mede esse custo fixo à parte e roda 10 commits numa única sessão:

```bash
cat > /tmp/rds-elc.sh <<'EOF'
#!/bin/bash
N=${1:-10}
gen() { for i in $(seq 1 $N); do echo "INSERT INTO transacoes (carga) VALUES ('$2');"; done; }
ms() { echo $(( ( $(date +%s%N) - $1 ) / 1000000 )); }
S=$(date +%s%N); docker exec rds-primary psql -U admin -d pacelc_rds -tAc "SELECT 1;" >/dev/null 2>&1; BASE=$(ms $S)
S=$(date +%s%N); gen "$@" | docker exec -i rds-primary psql -U admin -d pacelc_rds -q >/dev/null 2>&1; T=$(ms $S)
echo "  sessao psql (custo fixo) ... ${BASE}ms"
echo "  $N commits ................. ${T}ms  (~$(( (T-BASE)/N ))ms/commit)"
EOF
```

Cada `INSERT` é um commit separado. Isso é proposital: **é o commit que a replicação síncrona bloqueia**, não o `INSERT` em si.

### As funções de caos

```bash
chaos_on()  { docker rm -f pumba-rds 2>/dev/null
              MSYS_NO_PATHCONV=1 docker run -d --name pumba-rds --rm \
                -v /var/run/docker.sock:/var/run/docker.sock \
                gaiaadm/pumba netem --duration 120s delay --time 2000 "$1" >/dev/null
              sleep 10; }

chaos_off() { docker stop pumba-rds 2>/dev/null; sleep 30; }
```

> **Os 30 segundos do `chaos_off` são obrigatórios.** A regra de latência não é removida no instante em que o Pumba para. Sem essa pausa, o próximo teste herda o atraso do anterior. Na primeira validação deste laboratório, a réplica assíncrona apareceu com 1392 ms/commit por causa disso — um resultado totalmente falso.
>
> `MSYS_NO_PATHCONV=1` só é necessário no Git Bash do Windows.

### Rodar os três cenários

```bash
bash /tmp/rds-elc.sh 10 ctrl                              # sem caos
chaos_on rds-standby-sync ; bash /tmp/rds-elc.sh 10 sync  ; chaos_off
chaos_on rds-read-replica ; bash /tmp/rds-elc.sh 10 async ; chaos_off
```

### O que você vai ver

| Cenário | Custo por commit | Quadrante |
| --- | --- | --- |
| Sem caos | **1–6 ms** | — |
| Réplica **síncrona** atrasada em 2000 ms | **2042 ms** | **EC** |
| Réplica **assíncrona** atrasada em 2000 ms | **5 ms** | **EL** |

Atrasar a réplica síncrona multiplica o custo do commit por **~500×**. Atrasar a assíncrona não faz diferença mensurável — e o atraso estava lá: medimos 6046 ms de ida e volta contra ela, versus 64 ms contra o nó saudável.

Mesmo cluster, mesma carga, mesmo atraso. **A única variável é o contrato de replicação, e ela decide sozinha se a lentidão da rede chega ou não na aplicação.**

---

## Teste T4 — O que acontece quando a réplica some (eixo PAC)

### T4a — Desconectando a réplica síncrona

```bash
docker network disconnect pacelc-rds-network rds-standby-sync ; sleep 5

timeout 15 docker exec rds-primary psql -U admin -d pacelc_rds \
  -tAc "INSERT INTO transacoes (carga) VALUES ('part_sync');"
```

**O `INSERT` trava e não volta.** O `timeout 15` corta depois de 15 segundos; sem ele, ficaria travado indefinidamente.

O motivo: o primário se comprometeu a não confirmar nenhum commit sem a confirmação do standby síncrono. Como esse standby sumiu, ele espera — para sempre, se preciso. Diante da partição, o sistema **abre mão de disponibilidade para não perder consistência**. É o quadrante **PC**.

> Use o `timeout` mesmo em teste manual. Sem ele o terminal fica pendurado, e interromper com `Ctrl+C` deixa a transação num estado ambíguo: gravada localmente, nunca confirmada ao cliente.

Reconecte:

```bash
docker network connect pacelc-rds-network rds-standby-sync ; sleep 20
```

### T4b — Desconectando a read replica

```bash
docker network disconnect pacelc-rds-network rds-read-replica ; sleep 5

timeout 15 docker exec rds-primary psql -U admin -d pacelc_rds \
  -tAc "INSERT INTO transacoes (carga) VALUES ('part_async');"

docker network connect pacelc-rds-network rds-read-replica ; sleep 20
```

**Confirma em 513 ms**, como se nada tivesse acontecido. O contrato assíncrono não cria obrigação nenhuma com esse nó: o primário ignora a queda e segue atendendo. É o quadrante **PA** — ao custo de a réplica de leitura ficar desatualizada sem ninguém perceber.

### Resumo do T4

| Réplica desconectada | Contrato | Escrita no primário | Quadrante |
| --- | --- | --- | --- |
| `rds-standby-sync` | síncrono | **Trava sem responder** | **PC** |
| `rds-read-replica` | assíncrono | OK em 513 ms | **PA** |

---

## Tempos medidos

| Etapa | Tempo |
| --- | --- |
| Subir o cluster até 2 réplicas em streaming | 13 s |
| T1 — conferir topologia | <1 s |
| T2 — criar tabela | 1 s |
| T3 — eixo ELC (3 cenários, com as pausas de limpeza) | 110 s |
| T4 — eixo PAC (2 partições + restauração) | ~68 s |
| **Total** | **~3 min** |

---

## Se algo der errado

| O que você vê | Por que acontece | Como resolver |
| --- | --- | --- |
| Espera libera mas não há replicação | `pg_basebackup` também aparece em `pg_stat_replication` | Filtrar por `state='streaming'` |
| `up -d` volta em 2 s com cluster não pronto | O compose original não tinha healthcheck | Usar o compose deste README + a espera do Passo 3 |
| As duas réplicas aparecem como `async` | `application_name` não bate com `synchronous_standby_names` | Conferir a grafia nos dois arquivos |
| Réplica assíncrona "atrasa" os commits | Latência residual do teste anterior | Esperar 30 s no `chaos_off` |
| Baseline de ~3500 ms sem caos nenhum | Custo do `docker exec` + psql e aquecimento do WAL | Usar o script que amortiza N commits |
| Terminal travado sem retorno no T4a | É o resultado esperado do commit síncrono | Envolver em `timeout 15` |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash converte `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |

---

## O que levar disso para o trabalho

Marcar "Multi-AZ" no console da AWS não é só ligar redundância. É escolher um quadrante do PACELC para a aplicação inteira, e este laboratório mostra o preço nos dois eixos: **2042 ms vs. 5 ms por commit** quando a rede piora, e **travar vs. responder em 513 ms** quando um nó cai.

Na prática isso significa que uma implantação Multi-AZ amarra a disponibilidade de escrita da sua aplicação à saúde da rede entre zonas. É exatamente o que você quer quando perder uma transação confirmada é inaceitável — pagamento, emissão fiscal, movimentação de saldo. E é exatamente o que destrói a disponibilidade de um serviço que teria aguentado perder alguns segundos de dados sem consequência.

A Read Replica é o oposto: nunca atrapalha a escrita e nunca garante estar em dia. Se você lê dela, precisa assumir que o dado pode estar velho.

### Para explorar depois

O PostgreSQL tem posições intermediárias que este laboratório não cobre. Vale reexecutar o Teste T3 mudando só uma linha no `01-init.sh`:

- `synchronous_commit = remote_write` — espera a réplica receber, mas não gravar em disco;
- `synchronous_commit = local` — não espera réplica nenhuma;
- `synchronous_standby_names = 'ANY 1 (standby_sync, read_replica)'` — espera qualquer uma das duas, o que dá tolerância a falha sem perder a garantia.

O custo por commit se move ao longo do eixo conforme você troca essas opções.

---

## Encerrar

```bash
docker rm -f pumba-rds 2>/dev/null
docker compose down -v
```
