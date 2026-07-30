# Tutorial Analítico: Simulando AWS RDS e o Teorema PACELC com PostgreSQL Puro

Este laboratório constrói uma topologia RDS a partir da imagem oficial do PostgreSQL, sem abstrações de orquestração gerenciada. Um nó primário sustenta **duas** réplicas com contratos deliberadamente opostos:

- `rds-standby-sync` — replicação **síncrona**, emulando uma implantação **Multi-AZ**;
- `rds-read-replica` — replicação **assíncrona**, emulando uma **Read Replica**.

A virtude do arranjo é que os dois quadrantes do PACELC coexistem no mesmo cluster, sob o mesmo tráfego. A diferença de comportamento não vem do produto nem da carga: vem exclusivamente do contrato de replicação de cada nó.

> Todos os números abaixo foram obtidos executando este laboratório de ponta a ponta.

**Tempo total estimado: ~3 minutos** (bootstrap 13 s + testes 179 s).

---

## Fase 1: Script de Inicialização do Primário

A imagem oficial do PostgreSQL nasce isolada. Para que o motor assuma o papel de primário é necessário intervir em seu primeiro ciclo de vida.

> Arquivos `.sh` são ignorados pelo Git (ver [.gitignore](../../.gitignore)). Copie o conteúdo abaixo para `01-init.sh` neste diretório.

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

A linha decisiva é `synchronous_standby_names = 'standby_sync'`. Ela nomeia **um** `application_name` como síncrono. Toda réplica que se conecte com outro nome permanece assíncrona — é assim que o mesmo cluster hospeda os dois regimes.

```bash
chmod +x 01-init.sh
```

## Fase 2: Orquestração

> O `docker-compose.yml` é ignorado pelo Git. Copie o conteúdo abaixo para `docker-compose.yml` neste diretório.

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

### Inicialização e portão de prontidão

```bash
docker compose up -d --wait
```

`--wait` confirma que os processos respondem, mas **não** que a replicação foi estabelecida. Use o portão abaixo — ele filtra por `state='streaming'`, e essa filtragem é essencial:

```bash
for i in $(seq 1 60); do
  n=$(docker exec rds-primary psql -U admin -d pacelc_rds -tAc \
      "SELECT count(*) FROM pg_stat_replication WHERE state='streaming';" | tr -d '\r ')
  [ "$n" = "2" ] && { echo "2 replicas em streaming"; break; }
  sleep 5
done

docker exec rds-primary psql -U admin -d pacelc_rds \
  -c "SELECT application_name, state, sync_state FROM pg_stat_replication ORDER BY 1;"
```

> **Por que filtrar por `streaming`.** Durante o `pg_basebackup`, as sessões de clonagem também aparecem em `pg_stat_replication`, com `state='backup'`. Um `count(*)` sem filtro retorna 2 enquanto ainda não existe replicação alguma, e o portão libera cedo demais.

Saída esperada:

```
 application_name |   state   | sync_state
------------------+-----------+------------
 read_replica     | streaming | async
 standby_sync     | streaming | sync
```

## Fase 3: Schema

```bash
docker exec rds-primary psql -U admin -d pacelc_rds \
  -c "CREATE TABLE IF NOT EXISTS transacoes (id serial PRIMARY KEY, carga varchar(100));"
```

---

## Fase 4: Eixo ELC — Latência vs. Consistência

### Medição com custo fixo amortizado

Cada `docker exec ... psql` custa **~500 ms** entre criação do processo e handshake — comparável ou superior ao que se quer medir. O script abaixo mede o custo fixo separadamente e roda N commits numa única sessão:

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

Cada `INSERT` é seu próprio commit — e é o commit, não o `INSERT`, que a replicação síncrona bloqueia.

### Os três cenários

```bash
chaos_on()  { docker rm -f pumba-rds 2>/dev/null
              MSYS_NO_PATHCONV=1 docker run -d --name pumba-rds --rm \
                -v /var/run/docker.sock:/var/run/docker.sock \
                gaiaadm/pumba netem --duration 120s delay --time 2000 "$1" >/dev/null
              sleep 10; }
chaos_off() { docker stop pumba-rds 2>/dev/null; sleep 30; }

bash /tmp/rds-elc.sh 10 ctrl                              # controle
chaos_on rds-standby-sync ; bash /tmp/rds-elc.sh 10 sync  ; chaos_off
chaos_on rds-read-replica ; bash /tmp/rds-elc.sh 10 async ; chaos_off
```

> **A espera de 30 s em `chaos_off` não é decorativa.** O `netem` não é removido no instante em que o Pumba para. Sem ela, o experimento seguinte herda o atraso do anterior — na primeira execução deste laboratório o cenário assíncrono acusou 1392 ms/commit, um resultado inteiramente espúrio, produzido pelo atraso residual no standby síncrono.

### Resultado medido

| Cenário | ms/commit | Quadrante |
| --- | --- | --- |
| Malha saudável | **1–6 ms** | — |
| `rds-standby-sync` (síncrono) com 2000 ms | **2042 ms** | **EC** |
| `rds-read-replica` (assíncrono) com 2000 ms | **5 ms** | **EL** |

Degradar a réplica síncrona multiplica o custo do commit por **~500×**. Degradar a assíncrona — com atraso idêntico, confirmado em 6046 ms de ida e volta contra 64 ms do nó saudável — **não produz efeito mensurável**.

Este é o teorema em sua forma mais nítida que este repositório consegue produzir: mesmo cluster, mesma carga, mesmo atraso injetado. A única variável é o contrato de replicação, e ela determina integralmente se a latência da rede chega ou não à aplicação cliente.

---

## Fase 5: Eixo PAC — Disponibilidade sob Partição

### 5a — Particionando o standby síncrono (Multi-AZ)

```bash
docker network disconnect pacelc-rds-network rds-standby-sync ; sleep 5
timeout 15 docker exec rds-primary psql -U admin -d pacelc_rds \
  -tAc "INSERT INTO transacoes (carga) VALUES ('part_sync');"
```

A operação **bloqueia indefinidamente** — o `timeout 15` a interrompe. O primário se recusa a confirmar um commit que não pode ser replicado ao standby síncrono. Diante da partição, o sistema abdica de **A** para preservar **C**: quadrante **PC**.

O uso de `timeout` aqui não é conveniência de script. Sem ele o terminal trava sem retorno, e um `Ctrl+C` deixa a transação em estado ambíguo: gravada localmente, jamais confirmada ao cliente.

```bash
docker network connect pacelc-rds-network rds-standby-sync ; sleep 20
```

### 5b — Particionando a read replica

```bash
docker network disconnect pacelc-rds-network rds-read-replica ; sleep 5
timeout 15 docker exec rds-primary psql -U admin -d pacelc_rds \
  -tAc "INSERT INTO transacoes (carga) VALUES ('part_async');"
docker network connect pacelc-rds-network rds-read-replica ; sleep 20
```

Confirmada em **513 ms**. O contrato assíncrono não estabelece obrigação alguma com este nó: o primário ignora a ruptura e segue atendendo. Quadrante **PA**, ao custo da desatualização silenciosa do nó leitor.

### Matriz consolidada

| Nó particionado | Contrato | Escrita no primário | Quadrante |
| --- | --- | --- | --- |
| `rds-standby-sync` | síncrono | **Bloqueada indefinidamente** | **PC** |
| `rds-read-replica` | assíncrono | OK em 513 ms | **PA** |

---

## Resultados medidos

| Etapa | Tempo |
| --- | --- |
| Bootstrap até 2 réplicas em streaming | 13 s |
| T1 — topologia de replicação | <1 s |
| T2 — schema | 1 s |
| T3 — eixo ELC (3 cenários, com esperas de limpeza) | 110 s |
| T4 — eixo PAC (2 partições + restauração) | ~68 s |
| **Total** | **~3 min** |

---

## Erros e armadilhas verificados em execução

| Sintoma | Causa | Correção |
| --- | --- | --- |
| Portão libera sem replicação existir | `pg_basebackup` também aparece em `pg_stat_replication` | Filtrar por `state='streaming'` |
| `up -d` retorna em 2 s com cluster não pronto | Compose original sem healthchecks | Healthchecks + portão de streaming |
| Réplica assíncrona "atrasa" os commits | `netem` residual do experimento anterior | Aguardar ~30 s em `chaos_off` |
| Baselines de ~3500 ms sem caos | Custo de `docker exec` + psql e aquecimento do WAL | Amortizar N commits numa sessão |
| Terminal travado sem retorno na Fase 5a | Commit síncrono aguarda standby ausente | Envolver em `timeout 15` |
| `mkdir C:\Program Files\Git\var: Acesso negado` | Git Bash reescreve `/var/run/docker.sock` | Prefixar `MSYS_NO_PATHCONV=1` |

---

## Implicações Arquiteturais

Marcar "Multi-AZ" no console da AWS não é uma opção de redundância: é a escolha de um quadrante do PACELC para toda a aplicação. O laboratório mede as duas faces desse contrato com atraso idêntico aplicado a nós de papéis distintos — **2042 ms/commit contra 5 ms/commit** no eixo ELC, e **bloqueio indefinido contra 513 ms** no eixo PAC.

A implicação de projeto é que uma implantação Multi-AZ acopla a disponibilidade de escrita da aplicação à saúde da rede entre zonas. É exatamente o que se deseja quando a perda de uma transação confirmada é inaceitável — e é exatamente o que arruína a disponibilidade percebida de um serviço que teria tolerado a perda de alguns segundos de dados. A Read Replica ocupa o extremo oposto: nunca atrapalha a escrita, e nunca garante estar em dia.

O PostgreSQL permite habitar posições intermediárias que este laboratório não explora — `synchronous_commit = remote_write` ou `local`, e listas de múltiplos standbys síncronos com quórum (`ANY 1 (s1, s2)`). Vale reexecutar a Fase 4 alterando apenas `synchronous_commit` no `01-init.sh` para ver o custo por commit se mover ao longo do eixo.

---

## Encerramento

```bash
docker rm -f pumba-rds 2>/dev/null
docker compose down -v
```
