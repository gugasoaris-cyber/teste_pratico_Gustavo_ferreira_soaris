from __future__ import annotations
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

@dataclass
class ExecutionMetrics:
    started_at: float = field(default_factory=time.perf_counter)
    rows_read_rh: int = 0
    rows_read_esocial: int = 0
    rows_read_gestao: int = 0
    rows_accepted_rh: int = 0
    rows_accepted_esocial: int = 0
    rows_accepted_gestao: int = 0
    rows_rejected_total: int = 0
    rows_consolidado: int = 0
    stage_seconds: dict[str, float] = field(default_factory=dict)

    def segundos_totais(self) -> float:
        return time.perf_counter() - self.started_at

    def registrar_etapa(self, name: str, start: float) -> None:
        self.stage_seconds[name] = round(time.perf_counter() - start, 3)

    def para_dicionario(self) -> dict[str, Any]:
        d = asdict(self)
        d['elapsed_seconds_total'] = round(self.segundos_totais(), 3)
        d['throughput_rows_per_sec'] = round(self.rows_consolidado / d['elapsed_seconds_total'], 1) if d['elapsed_seconds_total'] > 0 else 0.0
        return d

    def salvar_json(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.para_dicionario(), ensure_ascii=False, indent=2), encoding='utf-8')
        return path
