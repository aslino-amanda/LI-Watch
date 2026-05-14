"""
LI Watch — Score Engine v2
Calcula score de risco para todas as lojas, incluindo ativas em declínio.

Resolve o "bug da Arco Íris LED": lojas com GMV > 0 mas em queda
acelerada recebiam score 5 e desapareciam da fila. Agora recebem
status "QUEDA CRÍTICA" e score proporcional à gravidade do declínio.

Uso:
    from score_engine import calcular_scores_df
    df = calcular_scores_df(df_base, tendencias_map)
"""

import pandas as pd
import numpy as np
from typing import Optional


# ── THRESHOLDS DE QUEDA ───────────────────────────────────────────────────────
QUEDA_CRITICA  = 0.50   # > 50% → score 85
QUEDA_ALTA     = 0.30   # > 30% → score 70
QUEDA_RISCO    = 0.20   # > 20% → score 55
QUEDA_ATENCAO  = 0.10   # > 10% → score 35


def calcular_score_tendencia(var_gmv_pct: Optional[float], is_pago: bool = False) -> tuple:
    """
    Dado a variação de GMV (float negativo, ex: -0.45 para -45%),
    retorna (score, status_queda, label_severidade).
    """
    if var_gmv_pct is None:
        return 5, "LOJA ATIVA", "🟢 SAUDÁVEL"

    queda = abs(var_gmv_pct) if var_gmv_pct < 0 else 0

    if queda == 0:
        base_score = 5
        status = "LOJA ATIVA"
        label  = "🟢 SAUDÁVEL"
    elif queda >= QUEDA_CRITICA:
        base_score = 85
        status = "QUEDA CRÍTICA"
        label  = "🔴 QUEDA CRÍTICA"
    elif queda >= QUEDA_ALTA:
        base_score = 70
        status = "QUEDA CRÍTICA"
        label  = "🔴 QUEDA ALTA"
    elif queda >= QUEDA_RISCO:
        base_score = 55
        status = "QUEDA ATENÇÃO"
        label  = "🟡 QUEDA EM RISCO"
    elif queda >= QUEDA_ATENCAO:
        base_score = 35
        status = "QUEDA ATENÇÃO"
        label  = "🟡 QUEDA ATENÇÃO"
    else:
        base_score = 5
        status = "LOJA ATIVA"
        label  = "🟢 SAUDÁVEL"

    # Agravante: loja paga em queda é prioridade máxima
    if is_pago and base_score >= 35:
        base_score = min(base_score + 10, 100)

    return base_score, status, label


def calcular_scores_df(
    df: pd.DataFrame,
    tendencias_map: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Recebe o DataFrame base de lojas e um mapa opcional de tendências.
    Retorna o DataFrame com colunas adicionadas:
      - score          : int 0-100
      - status_queda   : str (LOJA ATIVA / QUEDA ATENÇÃO / QUEDA CRÍTICA / outros)
      - label_risco    : str com emoji
      - var_gmv_pct    : float ou None
      - gargalo        : str (diagnóstico de onboarding)
      - janela         : str (urgência temporal)

    tendencias_map: { loja_id (int) -> var_gmv_pct (float) }
    """
    df = df.copy()

    # ── Tipos seguros ─────────────────────────────────────────────────────────
    df["dias_cadastro"] = pd.to_numeric(df.get("dias_cadastro", 0), errors="coerce").fillna(0).astype(int)
    df["status_loja"]   = df.get("status_loja", pd.Series(dtype=str)).fillna("").astype(str)
    df["status_plano"]  = df.get("status_plano", pd.Series(dtype=str)).fillna("").astype(str)

    _s = df["status_loja"]
    _d = df["dias_cadastro"]
    _p = df["status_plano"].str.upper()

    # ── Score base (lógica existente para não-ativas) ─────────────────────────
    _score_base = np.where(
        _s == "ONBOARDING INCOMPLETO", np.where(_d >= 7, 70, 50),
        np.where(
            _s == "NUNCA VENDEU", np.where(_d >= 20, 45, 25),
            np.where(_s == "SEM VENDAS RECENTES", 55, 5)
        )
    )
    _score_base = np.where(_p == "PAGO", np.minimum(_score_base + 10, 100), _score_base)

    df["score"]        = _score_base.astype(int)
    df["status_queda"] = df["status_loja"].copy()
    df["label_risco"]  = np.where(_s == "ONBOARDING INCOMPLETO", "🔴 ONBOARDING",
                         np.where(_s == "NUNCA VENDEU",           "🟠 NUNCA VENDEU",
                         np.where(_s == "SEM VENDAS RECENTES",    "🟡 SEM VENDAS",
                                                                   "🟢 SAUDÁVEL")))
    df["var_gmv_pct"]  = None

    # ── Sobrescreve lojas ATIVAS com dados de tendência ───────────────────────
    if tendencias_map:
        for idx, row in df.iterrows():
            if row["status_loja"] != "LOJA ATIVA":
                continue
            lid = int(row.get("loja_id", row.get("conta_id", 0)))
            var = tendencias_map.get(lid)
            if var is None:
                continue
            is_pago = str(row.get("status_plano", "")).upper() == "PAGO"
            score, status, label = calcular_score_tendencia(float(var), is_pago)
            df.at[idx, "score"]        = score
            df.at[idx, "status_queda"] = status
            df.at[idx, "label_risco"]  = label
            df.at[idx, "var_gmv_pct"]  = var

    # ── Gargalo de onboarding ─────────────────────────────────────────────────
    def _gargalo(row):
        sl = row.get("status_loja", "")
        if sl == "LOJA ATIVA":
            var = row.get("var_gmv_pct")
            if var is not None and var <= -0.30:
                pct = abs(var) * 100
                return f"📉 Queda de {pct:.0f}% no GMV"
            return "✅ Ativa"
        if str(row.get("data_primeira_config_produto", "")) in ("", "None", "nan", "NaT"):
            return "🔴 Sem produto"
        if str(row.get("data_primeira_config_pagamento", "")) in ("", "None", "nan", "NaT"):
            return "🔴 Sem pagamento"
        if str(row.get("data_primeira_config_logistica", "")) in ("", "None", "nan", "NaT"):
            return "🟡 Sem frete"
        if str(row.get("data_primeira_venda", "")) in ("", "None", "nan", "NaT"):
            return "🟠 Nunca vendeu"
        return "✅ Configurada"

    df["gargalo"] = df.apply(_gargalo, axis=1)

    # ── Janela de urgência ────────────────────────────────────────────────────
    def _janela(row):
        sl = row.get("status_loja", "")
        if sl == "LOJA ATIVA":
            var = row.get("var_gmv_pct")
            if var is not None and var <= -0.30:
                return "🔴 Intervenção urgente"
            if var is not None and var <= -0.20:
                return "🕐 Monitorar de perto"
            return "🟢 OK"
        d = int(row.get("dias_cadastro") or 0)
        if d >= 15: return "⚠️ Janela vencida"
        if d >= 7:  return "🕐 Janela crítica"
        return "🟢 Janela aberta"

    df["janela"] = df.apply(_janela, axis=1)

    return df.sort_values("score", ascending=False).reset_index(drop=True)
