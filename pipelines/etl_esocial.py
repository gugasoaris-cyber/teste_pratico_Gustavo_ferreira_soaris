from __future__ import annotations
from pathlib import Path
import pandas as pd
from src.extract import carregar_csv
from src.transform import padronizar_colunas

def executar(data_dir: str | Path, **read_kwargs) -> pd.DataFrame:
    path = Path(data_dir) / 'esocial.csv'
    df = carregar_csv(path, **read_kwargs)
    return padronizar_colunas(df, 'esocial')
