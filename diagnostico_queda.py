"""
LI Watch — Diagnóstico de Queda em Vendas (Lógica Semanal)
Compara últimas 2 semanas vs mesmo período 30 dias atrás.
Gatilho: queda de 20%+ no GMV.

conta_id == loja_id (confirmado via metabase_connector)
"""

import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Optional


# ══════════════════════════════════════════════════════════════════════════════
# THRESHOLDS
# ══════════════════════════════════════════════════════════════════════════════

QUEDA_CRITICA  = 0.50   # queda > 50% = CRÍTICO
QUEDA_RISCO    = 0.20   # queda > 20% = RISCO (gatilho principal)
QUEDA_ATENCAO  = 0.10   # queda > 10% = ATENÇÃO


# ══════════════════════════════════════════════════════════════════════════════
# DIAGNÓSTICO INDIVIDUAL — lógica semanal
# ══════════════════════════════════════════════════════════════════════════════

def diagnosticar_queda(conta_id: int, nome_loja: str, connector) -> dict:
    """
    Compara últimas 2 semanas vs mesmo período 30 dias atrás.
    Retorna diagnóstico com causa raiz, severidade e sinais.
    """
    resultado = {
        "conta_id":        conta_id,
        "nome_loja":       nome_loja,
        "causa_raiz":      "—",
        "causas":          [],
        "sinais":          {},
        "severidade":      "INVESTIGAR",
        "variacao_gmv_pct": None,
        "erro":            None,
    }

    try:
        # ── 1. TENDÊNCIA SEMANAL ──────────────────────────────────────────────
        semanal = connector.buscar_tendencia_semanal(conta_id)

        if not semanal or not semanal.get("gmv_anterior"):
            resultado["causa_raiz"] = "📊 Sem histórico suficiente para comparar"
            return resultado

        gmv_atual    = float(semanal.get("gmv_atual") or 0)
        gmv_ant      = float(semanal.get("gmv_anterior") or 0)
        ped_atual    = float(semanal.get("pedidos_atual") or 0)
        ped_ant      = float(semanal.get("pedidos_anterior") or 0)
        tick_atual   = float(semanal.get("ticket_atual") or 0)
        tick_ant     = float(semanal.get("ticket_anterior") or 0)
        gmv_em_risco = float(semanal.get("gmv_em_risco") or 0)
        var_gmv      = float(semanal.get("var_gmv_pct") or 0) / 100
        var_ped      = float(semanal.get("var_pedidos_pct") or 0) / 100
        var_tick     = float(semanal.get("var_ticket_pct") or 0) / 100

        resultado["variacao_gmv_pct"] = var_gmv
        resultado["sinais"] = {
            "gmv_atual":       gmv_atual,
            "gmv_anterior":    gmv_ant,
            "gmv_em_risco":    gmv_em_risco,
            "var_gmv_pct":     round(var_gmv * 100, 1),
            "var_ped_pct":     round(var_ped * 100, 1),
            "var_tick_pct":    round(var_tick * 100, 1),
            "atual_de":        str(semanal.get("atual_de", "")),
            "atual_ate":       str(semanal.get("atual_ate", "")),
            "ref_de":          str(semanal.get("ref_de", "")),
            "ref_ate":         str(semanal.get("ref_ate", "")),
        }

        causas = []
        score  = 0

        # ── 2. CAUSA RAIZ PELA COMBINAÇÃO DE SINAIS ───────────────────────────

        # Queda de volume com ticket estável = menos tráfego/conversão
        if var_ped <= -0.20 and abs(var_tick) < 0.10:
            causas.append({
                "tipo":      "QUEDA_TRAFEGO",
                "emoji":     "📉",
                "descricao": f"Volume de pedidos caiu {abs(var_ped*100):.0f}% com ticket estável — queda de tráfego ou conversão",
                "peso":      3,
            })
            score += 3

        # Queda de ticket com volume mantido = cupom ou reprecificação
        if var_tick <= -0.15 and var_ped >= -0.10:
            causas.append({
                "tipo":      "QUEDA_TICKET",
                "emoji":     "🏷️",
                "descricao": f"Ticket médio caiu {abs(var_tick*100):.0f}% mantendo volume — verificar cupons ou preços",
                "peso":      2,
            })
            score += 2

        # Queda de ambos = problema mais sério
        if var_ped <= -0.20 and var_tick <= -0.10:
            causas.append({
                "tipo":      "QUEDA_GERAL",
                "emoji":     "🔴",
                "descricao": f"Queda combinada: {abs(var_ped*100):.0f}% em pedidos e {abs(var_tick*100):.0f}% no ticket — investigar mix de produto e pagamento",
                "peso":      4,
            })
            score += 4

        # GMV em risco absoluto alto
        if gmv_em_risco >= 50000:
            causas.append({
                "tipo":      "GMV_RISCO_ALTO",
                "emoji":     "💰",
                "descricao": f"R${gmv_em_risco:,.0f} em GMV abaixo do período de referência nas últimas 2 semanas",
                "peso":      2,
            })
            score += 2

        # ── 3. VERIFICA MIX DE PAGAMENTO (causa comum) ───────────────────────
        try:
            from datetime import date
            from dateutil.relativedelta import relativedelta
            hoje = date.today()
            data_ini = (hoje - relativedelta(months=3)).strftime("%Y-%m-%d")
            data_fim = hoje.strftime("%Y-%m-%d")
            m_ant = (hoje.replace(day=1) - relativedelta(months=1)).strftime("%Y-%m")
            m_ref = (hoje.replace(day=1) - relativedelta(months=2)).strftime("%Y-%m")

            df_pag = connector.buscar_mix_pagamento(conta_id, data_ini, data_fim)
            if not df_pag.empty and "mes" in df_pag.columns:
                formas_ant = set(df_pag[df_pag["mes"] == m_ref]["forma_pagamento"].tolist())
                formas_ref = set(df_pag[df_pag["mes"] == m_ant]["forma_pagamento"].tolist())
                removidas  = formas_ant - formas_ref
                criticas   = [f for f in removidas if any(k in str(f).upper() for k in ["PIX", "CART", "CRED", "DEBIT"])]

                resultado["sinais"]["formas_removidas"] = list(removidas)
                resultado["sinais"]["formas_ativas"]    = list(formas_ref)

                if criticas:
                    causas.append({
                        "tipo":      "PAGAMENTO_REMOVIDO",
                        "emoji":     "💳",
                        "descricao": f"Forma de pagamento crítica removida recentemente: {', '.join(criticas)}",
                        "peso":      5,
                    })
                    score += 5
                elif removidas:
                    causas.append({
                        "tipo":      "PAGAMENTO_REMOVIDO",
                        "emoji":     "💳",
                        "descricao": f"Forma de pagamento removida: {', '.join(removidas)}",
                        "peso":      2,
                    })
                    score += 2
        except:
            pass

        # ── 4. SEVERIDADE FINAL ───────────────────────────────────────────────
        if var_gmv <= -QUEDA_CRITICA or score >= 7:
            severidade = "CRÍTICO"
        elif var_gmv <= -QUEDA_RISCO or score >= 3:
            severidade = "RISCO"
        else:
            severidade = "ATENÇÃO"

        causas.sort(key=lambda x: x["peso"], reverse=True)

        if not causas:
            causa_raiz = f"📊 GMV {var_gmv*100:+.0f}% vs 2 semanas atrás — análise manual recomendada"
        else:
            top = causas[0]
            causa_raiz = f"{top['emoji']} {top['descricao']}"
            if len(causas) >= 2:
                causa_raiz += f" (+{len(causas)-1} fator{'es' if len(causas)>2 else ''})"

        resultado["causa_raiz"] = causa_raiz
        resultado["causas"]     = causas
        resultado["severidade"] = severidade

    except Exception as e:
        resultado["causa_raiz"] = f"⚠️ Erro ao diagnosticar: {str(e)[:80]}"
        resultado["erro"]       = str(e)

    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# BATCH
# ══════════════════════════════════════════════════════════════════════════════

def diagnosticar_queda_batch(
    df_pipeline: pd.DataFrame,
    connector,
    status_alvo: str = "SEM VENDAS RECENTES",
    progress_callback=None,
) -> dict:
    lojas = df_pipeline if status_alvo not in df_pipeline.get("status_loja", pd.Series()).values else             df_pipeline[df_pipeline["status_loja"] == status_alvo]

    # Aceita df com ou sem coluna status_loja (top sellers vêm sem esse filtro)
    if "status_loja" in df_pipeline.columns and status_alvo in df_pipeline["status_loja"].values:
        lojas = df_pipeline[df_pipeline["status_loja"] == status_alvo]
    else:
        lojas = df_pipeline

    resultados = {}
    total = len(lojas)
    for i, (_, row) in enumerate(lojas.iterrows()):
        loja_id   = int(row.get("loja_id", row.get("conta_id", 0)))
        nome_loja = str(row.get("nome_loja", "—"))
        resultados[loja_id] = diagnosticar_queda(loja_id, nome_loja, connector)
        if progress_callback:
            progress_callback(i + 1, total)

    return resultados


# ══════════════════════════════════════════════════════════════════════════════
# FORMATADORES PARA TABELA
# ══════════════════════════════════════════════════════════════════════════════

def formatar_causa_curta(resultado: dict) -> str:
    if resultado.get("erro"):
        return "⚠️ Erro no diagnóstico"
    causas = resultado.get("causas", [])
    if not causas:
        var = resultado.get("sinais", {}).get("var_gmv_pct")
        if var is not None:
            return f"📊 GMV {var:+.0f}% vs 2sem atrás"
        return "📊 Análise manual recomendada"
    top = causas[0]
    texto = top["descricao"]
    return f"{top['emoji']} {texto[:60]}{'…' if len(texto) > 60 else ''}"


def formatar_variacao_gmv(resultado: dict) -> str:
    var = resultado.get("variacao_gmv_pct")
    if var is None:
        return "—"
    sinal = "+" if var > 0 else ""
    return f"{sinal}{var*100:.0f}%"
