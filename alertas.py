"""
LI Watch — Sistema de Alertas Proativos
Detecta eventos críticos e exibe no topo do app em tempo real.
"""

import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Optional

# ── REGRAS DE ALERTA ──────────────────────────────────────────────────────────

def detectar_alertas(df: pd.DataFrame) -> list:
    """
    Analisa o DataFrame de lojas e retorna lista de alertas.
    Cada alerta tem: tipo, severidade, titulo, descricao, qtde, acao
    """
    alertas = []
    hoje = date.today()

    if df.empty:
        return alertas

    def safe_int(val):
        try: return int(float(val or 0))
        except: return 0

    def safe_float(val):
        try: return float(val or 0)
        except: return 0.0

    def dias_desde(data_str) -> Optional[int]:
        if not data_str or str(data_str) in ("None","nan",""):
            return None
        try:
            d = datetime.strptime(str(data_str)[:10], "%Y-%m-%d").date()
            return (hoje - d).days
        except:
            return None

    # ── ALERTA 1: Lojas críticas sem pagamento há mais de 7 dias ─────────────
    sem_pag_criticas = df[
        (df["status_loja"] == "ONBOARDING INCOMPLETO") &
        (df["data_primeira_config_produto"].notna()) &
        (df["data_primeira_config_pagamento"].isna()) &
        (df["data_cadastro_loja"].apply(
            lambda x: (dias_desde(x) or 0) >= 7
        ))
    ]
    if len(sem_pag_criticas) > 0:
        alertas.append({
            "tipo":       "PAGAMENTO",
            "severidade": "CRÍTICO",
            "emoji":      "🔴",
            "titulo":     f"{len(sem_pag_criticas)} loja(s) com produto cadastrado mas sem pagamento há 7+ dias",
            "descricao":  "Estas lojas já passaram da janela crítica. Risco alto de abandono permanente.",
            "qtde":       len(sem_pag_criticas),
            "acao":       "Acionar CS imediatamente",
            "cor_bg":     "#FEF2F2",
            "cor_borda":  "#E24B4A",
            "cor_texto":  "#991B1B",
        })

    # ── ALERTA 2: Pico de novas lojas nas últimas 24h ────────────────────────
    novas_24h = df[
        df["data_cadastro_loja"].apply(lambda x: (dias_desde(x) or 999) <= 1)
    ]
    if len(novas_24h) >= 10:
        alertas.append({
            "tipo":       "VOLUME",
            "severidade": "INFO",
            "emoji":      "📈",
            "titulo":     f"{len(novas_24h)} novas lojas criadas nas últimas 24h",
            "descricao":  "Volume acima do normal. Janela de ouro para intervenção de onboarding.",
            "qtde":       len(novas_24h),
            "acao":       "Priorizar e-mail de boas-vindas hoje",
            "cor_bg":     "#EEEDFE",
            "cor_borda":  "#1ABCB0",
            "cor_texto":  "#0D4F4A",
        })

    # ── ALERTA 3: Lojas configuradas sem nenhuma visita há 5+ dias ───────────
    config_sem_visita = df[
        (df["status_loja"] == "NUNCA VENDEU") &
        (df["qtde_visitas_ultimos_30d"].apply(safe_int) == 0) &
        (df["data_cadastro_loja"].apply(
            lambda x: (dias_desde(x) or 0) >= 5
        ))
    ]
    if len(config_sem_visita) > 0:
        alertas.append({
            "tipo":       "TRAFEGO",
            "severidade": "ATENÇÃO",
            "emoji":      "🟡",
            "titulo":     f"{len(config_sem_visita)} loja(s) configurada(s) sem nenhuma visita há 5+ dias",
            "descricao":  "Lojas prontas para vender mas invisíveis. Problema de divulgação.",
            "qtde":       len(config_sem_visita),
            "acao":       "E-mail com checklist de divulgação",
            "cor_bg":     "#FFFBEB",
            "cor_borda":  "#F59E0B",
            "cor_texto":  "#92400E",
        })

    # ── ALERTA 4: Lojas pagas sem configuração após 3 dias ───────────────────
    pagas_sem_config = df[
        (df["status_loja"] == "ONBOARDING INCOMPLETO") &
        (df["status_plano"].apply(lambda x: str(x).upper() == "PAGO")) &
        (df["data_cadastro_loja"].apply(
            lambda x: (dias_desde(x) or 0) >= 3
        ))
    ]
    if len(pagas_sem_config) > 0:
        alertas.append({
            "tipo":       "RECEITA",
            "severidade": "CRÍTICO",
            "emoji":      "💸",
            "titulo":     f"{len(pagas_sem_config)} loja(s) PAGAS sem configuração após 3 dias",
            "descricao":  "Clientes pagantes sem configurar. Risco de churn com impacto direto em receita.",
            "qtde":       len(pagas_sem_config),
            "acao":       "CS acionar com prioridade máxima",
            "cor_bg":     "#FEF2F2",
            "cor_borda":  "#E24B4A",
            "cor_texto":  "#991B1B",
        })

    # ── ALERTA 5: Lojas com queda de faturamento ─────────────────────────────
    queda_fat = df[
        (df["status_loja"] == "SEM VENDAS RECENTES") &
        (df["vlr_gmv_ultimos_30d"].apply(safe_float) == 0) &
        (df["data_primeira_venda"].notna())
    ]
    if len(queda_fat) > 0:
        alertas.append({
            "tipo":       "FATURAMENTO",
            "severidade": "ATENÇÃO",
            "emoji":      "📉",
            "titulo":     f"{len(queda_fat)} loja(s) com queda total de faturamento",
            "descricao":  "Lojas que vendiam e zeraram GMV nos últimos 30 dias.",
            "qtde":       len(queda_fat),
            "acao":       "Disparar análise de diagnóstico de queda",
            "cor_bg":     "#FFFBEB",
            "cor_borda":  "#F59E0B",
            "cor_texto":  "#92400E",
        })

    # Ordena por severidade
    ordem = {"CRÍTICO": 0, "ATENÇÃO": 1, "INFO": 2}
    alertas.sort(key=lambda x: ordem.get(x["severidade"], 3))

    return alertas


def exibir_alertas(df: pd.DataFrame):
    """
    Exibe o painel de alertas no topo do app.
    Chame no início de cada página.
    """
    alertas = detectar_alertas(df)

    if not alertas:
        st.markdown("""
        <div style='background:#F0FDF4;border:1px solid #86EFAC;border-radius:10px;
                    padding:.8rem 1.2rem;margin-bottom:1rem;font-size:13px;color:#166534'>
            ✅ <strong>Sem alertas críticos agora</strong> — todas as lojas monitoradas estão dentro do esperado.
        </div>
        """, unsafe_allow_html=True)
        return

    # Header do painel de alertas
    n_criticos = sum(1 for a in alertas if a["severidade"] == "CRÍTICO")
    n_atencao  = sum(1 for a in alertas if a["severidade"] == "ATENÇÃO")

    st.markdown(f"""
    <div style='background:#0D4F4A;border-radius:12px;padding:1rem 1.2rem;margin-bottom:1rem'>
        <div style='display:flex;justify-content:space-between;align-items:center'>
            <div>
                <div style='font-size:13px;font-weight:700;color:#D4F53C;margin-bottom:2px'>
                    👁️ LI Watch — {len(alertas)} alerta(s) detectado(s)
                </div>
                <div style='font-size:12px;color:#9DCFCC'>
                    {n_criticos} crítico(s) · {n_atencao} atenção · Atualizado agora
                </div>
            </div>
            <div style='font-size:24px'>{"🔴" if n_criticos > 0 else "🟡"}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Exibe cada alerta
    for alerta in alertas:
        col_alert, col_acao = st.columns([4, 1])
        with col_alert:
            st.markdown(f"""
            <div style='background:{alerta["cor_bg"]};border:1px solid {alerta["cor_borda"]};
                        border-left:4px solid {alerta["cor_borda"]};border-radius:8px;
                        padding:.7rem 1rem;margin-bottom:6px'>
                <div style='font-size:13px;font-weight:600;color:{alerta["cor_texto"]}'>
                    {alerta["emoji"]} {alerta["titulo"]}
                </div>
                <div style='font-size:12px;color:#555;margin-top:3px'>
                    {alerta["descricao"]}
                </div>
            </div>
            """, unsafe_allow_html=True)
        with col_acao:
            st.markdown(f"""
            <div style='background:white;border:1px solid {alerta["cor_borda"]};border-radius:8px;
                        padding:.7rem .8rem;margin-bottom:6px;font-size:11px;
                        color:{alerta["cor_texto"]};font-weight:600;text-align:center'>
                → {alerta["acao"]}
            </div>
            """, unsafe_allow_html=True)

    st.divider()
