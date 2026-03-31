from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
NOME_CONTAINER = "pessoas_oracle"


def _executar(comando: list[str], cwd: Path | None = None) -> None:
    print(f"\n$ {' '.join(shlex.quote(c) for c in comando)}")
    subprocess.run(comando, cwd=str(cwd or RAIZ), check=True)


def _detectar_compose() -> list[str]:
    tentativas = (["docker", "compose"], ["docker-compose"])
    for base in tentativas:
        try:
            subprocess.run(base + ["version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            return base
        except Exception:
            continue
    raise SystemExit("Docker Compose nao encontrado. Instale Docker Desktop e tente novamente.")


def _aguardar_oracle(timeout_segundos: int = 900) -> None:
    print("\nAguardando Oracle ficar saudavel...")
    inicio = time.time()
    while True:
        try:
            proc = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Health.Status}}", NOME_CONTAINER],
                capture_output=True,
                text=True,
                check=True,
            )
            status = proc.stdout.strip().lower()
        except Exception:
            status = ""
        if status == "healthy":
            print("Oracle pronto (healthy).")
            return
        if (time.time() - inicio) > timeout_segundos:
            raise SystemExit("Timeout aguardando Oracle ficar healthy.")
        time.sleep(5)


def principal() -> None:
    parser = argparse.ArgumentParser(description="Executa ETL completo ate a carga no Oracle.")
    parser.add_argument("--sample", type=int, default=None, help="Limita N linhas por fonte na ETL e na carga.")
    parser.add_argument("--resume", action="store_true", help="Retoma pipeline da ultima etapa gravada.")
    parser.add_argument("--clear-checkpoint", action="store_true", help="Limpa checkpoint antes de executar.")
    parser.add_argument("--no-truncate", action="store_true", help="Nao esvazia tabelas antes da carga.")
    parser.add_argument(
        "--gerar-dados",
        choices=("nenhum", "200", "completo"),
        default="nenhum",
        help="Gera dados antes da ETL: nenhum, 200 ou completo.",
    )
    parser.add_argument("--skip-docker", action="store_true", help="Nao sobe docker nem aguarda health.")
    args = parser.parse_args()

    py = sys.executable
    compose = _detectar_compose()

    if not args.skip_docker:
        _executar(compose + ["up", "-d"], cwd=RAIZ)
        _aguardar_oracle()

    if args.gerar_dados == "200":
        _executar([py, "scripts/generate_servidores_200.py"], cwd=RAIZ)
    elif args.gerar_dados == "completo":
        _executar([py, "scripts/generate_servidores.py"], cwd=RAIZ)

    cmd_main = [py, "main.py"]
    if args.sample is not None:
        cmd_main += ["--sample", str(args.sample)]
    if args.resume:
        cmd_main.append("--resume")
    if args.clear_checkpoint:
        cmd_main.append("--clear-checkpoint")
    _executar(cmd_main, cwd=RAIZ)

    cmd_carga = [py, "scripts/load_oracle.py"]
    if args.sample is not None:
        cmd_carga += ["--sample", str(args.sample)]
    if args.no_truncate:
        cmd_carga.append("--no-truncate")
    _executar(cmd_carga, cwd=RAIZ)

    print("\nFluxo completo finalizado com sucesso.")


if __name__ == "__main__":
    principal()
