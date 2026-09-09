"""Confere, um a um, os pre-requisitos do laboratorio.

    python verificar_ambiente.py

Cada uma das quatro dependencias falha de um jeito que NAO aponta para a
causa -- e por isso este script existe. Ele checa as quatro antes de voce
gastar tempo lendo um traceback de sessenta linhas:

    Falta                       Mensagem que voce receberia
    -------------------------   ------------------------------------------
    interpretador errado        ModuleNotFoundError: No module named 'pyspark'
    Python 3.13 ou 3.14         a instalacao funciona, a execucao nao
    Java                        erro de gateway do py4j, sem citar Java
    winutils.exe (Windows)      HADOOP_HOME and hadoop.home.dir are unset

Nao sobe sessao Spark: roda em ~1 s e nao precisa de nada configurado.
"""

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent

OK = "  OK   "
ERRO = " FALTA "
AVISO = " AVISO "

problemas: list[str] = []


def relatar(marca: str, titulo: str, detalhe: str, correcao: str = "") -> None:
    print(f"[{marca}] {titulo:<22} {detalhe}")
    if correcao:
        print(f"{'':<9}-> {correcao}")
        problemas.append(titulo)


# ---------------------------------------------------------------------------
print(f"\nVerificando o ambiente em {RAIZ}\n")

# 1. Interpretador Python -----------------------------------------------------
versao = sys.version_info
descricao = f"{versao.major}.{versao.minor}.{versao.micro}"
dentro_do_venv = str(RAIZ / ".venv").lower() in sys.executable.lower()

if (3, 8) <= (versao.major, versao.minor) <= (3, 12):
    relatar(OK, "Python", f"{descricao}  ({sys.executable})")
else:
    relatar(
        ERRO,
        "Python",
        f"{descricao} -- o PySpark 3.5 suporta apenas 3.8 a 3.12",
        "recrie o ambiente: py -3.11 -m venv .venv",
    )

if not dentro_do_venv:
    relatar(
        AVISO,
        "Ambiente virtual",
        "voce NAO esta usando o .venv desta pasta",
        r"rode com .\.venv\Scripts\python.exe verificar_ambiente.py",
    )

# 2. PySpark ------------------------------------------------------------------
versao_pyspark = None
try:
    import pyspark

    versao_pyspark = int(pyspark.__version__.split(".")[0])
    relatar(OK, "PySpark", f"{pyspark.__version__}")
except ImportError:
    relatar(
        ERRO,
        "PySpark",
        "nao instalado neste interpretador",
        'pip install "pyspark==3.5.3"',
    )

# 3. Java ---------------------------------------------------------------------
# `java -version` escreve em stderr, nao em stdout -- detalhe que confunde
# quem tenta capturar a saida pela primeira vez.
executavel_java = shutil.which("java")
if executavel_java is None:
    relatar(
        ERRO,
        "Java",
        "nao encontrado no PATH",
        "instale um JDK 8, 11 ou 17 (o Spark roda em JVM)",
    )
else:
    saida = subprocess.run(
        [executavel_java, "-version"], capture_output=True, text=True
    ).stderr.splitlines()
    primeira = saida[0] if saida else executavel_java
    relatar(OK, "Java", primeira)

    # "1.8.0_461" e Java 8; "17.0.9" e Java 17. A numeracao mudou no Java 9,
    # e a forma antiga (1.x) ainda aparece em toda instalacao do 8.
    import re

    achado = re.search(r'"(\d+)(?:\.(\d+))?', primeira)
    versao_java = None
    if achado:
        maior = int(achado.group(1))
        versao_java = int(achado.group(2) or 0) if maior == 1 else maior

    if versao_java is not None and versao_pyspark is not None:
        if versao_pyspark >= 4 and versao_java < 17:
            relatar(
                ERRO,
                "Java x PySpark",
                f"PySpark {versao_pyspark}.x exige JDK 17+, e aqui ha o {versao_java}",
                'use pyspark==3.5.3 (roda em Java 8) ou instale um JDK 17',
            )
        elif versao_pyspark == 3 and versao_java > 17:
            relatar(
                AVISO,
                "Java x PySpark",
                f"PySpark 3.5 e testado ate o JDK 17; aqui ha o {versao_java}",
                "se a sessao nao subir, instale um JDK 17",
            )

# 4. winutils (so no Windows) -------------------------------------------------
if os.name == "nt":
    binarios = {"winutils.exe": 112_640, "hadoop.dll": 84_992}
    faltando = [n for n in binarios if not (RAIZ / "hadoop" / "bin" / n).exists()]
    if faltando:
        relatar(
            ERRO,
            "winutils (Windows)",
            f"falta {', '.join(faltando)} em hadoop/bin/",
            "veja o Passo 4 do SETUP.md",
        )
    else:
        pequenos = [
            n
            for n, tamanho in binarios.items()
            if (RAIZ / "hadoop" / "bin" / n).stat().st_size < tamanho // 2
        ]
        if pequenos:
            # 470 bytes = pagina de erro do CDN salva com nome de executavel.
            relatar(
                ERRO,
                "winutils (Windows)",
                f"{', '.join(pequenos)} tem tamanho suspeito",
                "apague hadoop/bin/ e baixe de novo (Passo 4 do SETUP.md)",
            )
        else:
            relatar(OK, "winutils (Windows)", "hadoop/bin/ completo")
else:
    relatar(OK, "winutils", f"desnecessario em {platform.system()}")

# 5. Opcionais ----------------------------------------------------------------
try:
    import pandas

    relatar(OK, "pandas (opcional)", pandas.__version__)
except ImportError:
    relatar(AVISO, "pandas (opcional)", "ausente -- a secao 6 do script 01 sera pulada")

# ---------------------------------------------------------------------------
print()
if problemas:
    print(f"{len(problemas)} item(ns) a resolver: {', '.join(problemas)}")
    sys.exit(1)

print("Ambiente pronto. Comece por: python 00_primeiro_contato.py")
