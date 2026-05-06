"""
LI Watch — Diagnóstico de Queda em Vendas
Analisa lojas que já venderam mas reduziram ou zeraram GMV.
Identifica causa raiz usando as queries do supervisor.

conta_id == loja_id (confirmado via metabase_connector.buscar_top_lojas)
"""

import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Optional


# ══════════════════════════════════════════════════════════════════════════════
# THRESHOLDS DE DIAGNÓSTICO
# Ajuste conforme calibração futura com dados reais
# ══════════════════════════════════════════════════════════════════════════════

QUEDA_GMV_CRITICA     = 0.50   # GMV caiu mais de 50% vs mês anterior
QUEDA_GMV_ATENCAO     = 0.25   # GMV caiu mais de 25%
QUEDA_PEDIDOS_CRITICA = 0.40   # Volume de pedidos caiu mais de 40%
AUMENTO_CANCEL        = 0.15   # Cancelamentos > 15% do total = sinal de alerta
CUPOM_DOMINIO         = 0.60   # Se >60% dos pedidos usam cupom = possível dependência
CHURN_BASE_CRITICO    = 0.50   # Se >50% dos pedidos recorrentes sumiram


# ══════════════════════════════════════════════════════════════════════════════
# HELPER — JANELA DE ANÁLISE
# Compara M-1 (mês de referência) com M-2 (mês anterior)
# ══════════════════════════════════════════════════════════════════════════════

def _janela_analise() -> tuple[str, str, str, str]:
    """
    Retorna (data_inicio_6m, data_fim, mes_anterior, mes_referencia) no formato YYYY-MM-DD / YYYY-MM.
    Usa últimos 6 meses para ter contexto histórico.
    M_ref  = mês completo mais recente (mês passado)
    M_ant  = mês anterior a M_ref
    """
    hoje      = date.today()
    fim       = hoje.replace(day=1) - relativedelta(days=1)          # último dia do mês passado
    inicio    = (fim.replace(day=1) - relativedelta(months=5))        # 6 meses atrás

    m_ref     = fim.strftime("%Y-%m")                                  # ex: "2026-04"
    m_ant     = (fim.replace(day=1) - relativedelta(months=1)).strftime("%Y-%m")  # ex: "2026-03"

    return inicio.strftime("%Y-%m-%d"), fim.strftime("%Y-%m-%d"), m_ant, m_ref


def _janela_churn(data_inicio: str, data_fim: str) -> tuple[str, str, str]:
    """
    Define ref_inicio, ref_fim (período em que o cliente comprava)
    e corte (a partir de quando sumiu) para a query de churn.
    Usa os 2 primeiros meses do período como referência e o último mês como corte.
    """
    from datetime import datetime
    d_ini = datetime.strptime(data_inicio, "%Y-%m-%d").date()
    d_fim = datetime.strptime(data_fim, "%Y-%m-%d").date()

    ref_inicio = data_inicio
    ref_fim    = (d_ini + relativedelta(months=2)).strftime("%Y-%m-%d")
    corte      = (d_fim - relativedelta(months=1)).strftime("%Y-%m-%d")

    return ref_inicio, ref_fim, corte


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE DE DIAGNÓSTICO
# ══════════════════════════════════════════════════════════════════════════════

def _extrair_mes(df: pd.DataFrame, mes: str) -> Optional[dict]:
    """Retorna linha de um mês específico ou None se não existir."""
    if df.empty or "mes" not in df.columns:
        return None
    row = df[df["mes"] == mes]
    return row.iloc[0].to_dict() if not row.empty else None


def _pct_variacao(atual, anterior) -> Optional[float]:
    """Variação percentual entre dois valores. Retorna None se anterior == 0."""
    try:
        a, b = float(atual or 0), float(anterior or 0)
        if b == 0:
            return None
        return (a - b) / b
    except:
        return None


def diagnosticar_queda(
    conta_id: int,
    nome_loja: str,
    connector,          # instância ou módulo com as funções buscar_*
) -> dict:
    """
    Orquestra as queries e retorna diagnóstico de queda com:
      - causa_raiz: string resumida (para coluna da tabela)
      - causas: lista detalhada
      - sinais: dados brutos usados
      - severidade: CRÍTICO / ATENÇÃO / INVESTIGAR
      - variacao_gmv_pct: variação do GMV M-1 vs M-2 (para ordenação)
    """

    resultado = {
        "conta_id":         conta_id,
        "nome_loja":        nome_loja,
        "causa_raiz":       "—",
        "causas":           [],
        "sinais":           {},
        "severidade":       "INVESTIGAR",
        "variacao_gmv_pct": None,
        "erro":             None,
    }

    try:
        data_inicio, data_fim, m_ant, m_ref = _janela_analise()

        # ── 1. TENDÊNCIA MENSAL (Q1) ──────────────────────────────────────────
        df_tend = connector.buscar_tendencia(conta_id, data_inicio, data_fim)

        m_ref_data  = _extrair_mes(df_tend, m_ref)
        m_ant_data  = _extrair_mes(df_tend, m_ant)

        gmv_ref  = float(m_ref_data["receita_total"] if m_ref_data else 0)
        gmv_ant  = float(m_ant_data["receita_total"] if m_ant_data else 0)
        ped_ref  = int(m_ref_data["total_pedidos"]   if m_ref_data else 0)
        ped_ant  = int(m_ant_data["total_pedidos"]   if m_ant_data else 0)
        tick_ref = float(m_ref_data["ticket_medio"]  if m_ref_data else 0)
        tick_ant = float(m_ant_data["ticket_medio"]  if m_ant_data else 0)

        var_gmv  = _pct_variacao(gmv_ref, gmv_ant)
        var_ped  = _pct_variacao(ped_ref, ped_ant)
        var_tick = _pct_variacao(tick_ref, tick_ant)

        resultado["variacao_gmv_pct"] = var_gmv
        resultado["sinais"]["gmv_mes_ref"]  = gmv_ref
        resultado["sinais"]["gmv_mes_ant"]  = gmv_ant
        resultado["sinais"]["var_gmv_pct"]  = round(var_gmv * 100, 1) if var_gmv is not None else None
        resultado["sinais"]["var_ped_pct"]  = round(var_ped * 100, 1) if var_ped is not None else None
        resultado["sinais"]["var_tick_pct"] = round(var_tick * 100, 1) if var_tick is not None else None

        causas = []
        severidade_score = 0  # acumula para definir severidade final

        # ── 2. NOVOS VS RECORRENTES (Q5) ─────────────────────────────────────
        df_cli = connector.buscar_novos_recorrentes(conta_id, data_inicio, data_fim)

        if not df_cli.empty and "mes" in df_cli.columns:
            ref_novos = df_cli[(df_cli["mes"] == m_ref) & (df_cli["tipo_cliente"] == "Novo")]
            ref_rec   = df_cli[(df_cli["mes"] == m_ref) & (df_cli["tipo_cliente"] == "Recorrente")]
            ant_novos = df_cli[(df_cli["mes"] == m_ant) & (df_cli["tipo_cliente"] == "Novo")]
            ant_rec   = df_cli[(df_cli["mes"] == m_ant) & (df_cli["tipo_cliente"] == "Recorrente")]

            ped_novos_ref = int(ref_novos["total_pedidos"].sum() if not ref_novos.empty else 0)
            ped_rec_ref   = int(ref_rec["total_pedidos"].sum()   if not ref_rec.empty else 0)
            ped_novos_ant = int(ant_novos["total_pedidos"].sum() if not ant_novos.empty else 0)
            ped_rec_ant   = int(ant_rec["total_pedidos"].sum()   if not ant_rec.empty else 0)

            var_novos = _pct_variacao(ped_novos_ref, ped_novos_ant)
            var_rec   = _pct_variacao(ped_rec_ref, ped_rec_ant)

            resultado["sinais"]["novos_mes_ref"]   = ped_novos_ref
            resultado["sinais"]["rec_mes_ref"]     = ped_rec_ref
            resultado["sinais"]["var_novos_pct"]   = round(var_novos * 100, 1) if var_novos is not None else None
            resultado["sinais"]["var_rec_pct"]     = round(var_rec * 100, 1) if var_rec is not None else None

            if var_rec is not None and var_rec <= -CHURN_BASE_CRITICO:
                causas.append({
                    "tipo":      "CHURN_BASE",
                    "emoji":     "👥",
                    "descricao": f"Base fiel em queda: pedidos recorrentes caíram {abs(var_rec*100):.0f}% vs mês anterior",
                    "peso":      3,
                })
                severidade_score += 3

            if var_novos is not None and var_novos <= -QUEDA_PEDIDOS_CRITICA and ped_novos_ref == 0:
                causas.append({
                    "tipo":      "SEM_NOVOS_CLIENTES",
                    "emoji":     "🚫",
                    "descricao": f"Zero novos clientes no mês. Loja parou de atrair tráfego novo.",
                    "peso":      2,
                })
                severidade_score += 2

        # ── 3. MIX DE PAGAMENTO (Q6) ──────────────────────────────────────────
        df_pag = connector.buscar_mix_pagamento(conta_id, data_inicio, data_fim)

        if not df_pag.empty and "mes" in df_pag.columns:
            formas_ant = set(df_pag[df_pag["mes"] == m_ant]["forma_pagamento"].tolist())
            formas_ref = set(df_pag[df_pag["mes"] == m_ref]["forma_pagamento"].tolist())
            formas_removidas = formas_ant - formas_ref

            resultado["sinais"]["formas_removidas"] = list(formas_removidas)
            resultado["sinais"]["formas_ativas"]    = list(formas_ref)

            if formas_removidas:
                # Prioriza se Pix ou cartão foram removidos
                criticas = [f for f in formas_removidas if any(k in f.upper() for k in ["PIX", "CART", "CRED", "DEBIT"])]
                if criticas:
                    causas.append({
                        "tipo":      "PAGAMENTO_REMOVIDO",
                        "emoji":     "💳",
                        "descricao": f"Forma de pagamento crítica removida: {', '.join(criticas)}. Clientes não conseguem finalizar compra.",
                        "peso":      4,
                    })
                    severidade_score += 4
                else:
                    causas.append({
                        "tipo":      "PAGAMENTO_REMOVIDO",
                        "emoji":     "💳",
                        "descricao": f"Forma de pagamento removida: {', '.join(formas_removidas)}.",
                        "peso":      2,
                    })
                    severidade_score += 2

        # ── 4. ANÁLISE DE TENDÊNCIA (Q1 — lógica adicional) ──────────────────
        if var_gmv is not None:
            if var_gmv <= -QUEDA_GMV_CRITICA:
                causas.append({
                    "tipo":      "QUEDA_GMV_CRITICA",
                    "emoji":     "📉",
                    "descricao": f"GMV caiu {abs(var_gmv*100):.0f}% vs mês anterior (R${gmv_ant:,.0f} → R${gmv_ref:,.0f})",
                    "peso":      3,
                })
                severidade_score += 3
            elif var_gmv <= -QUEDA_GMV_ATENCAO:
                causas.append({
                    "tipo":      "QUEDA_GMV_ATENCAO",
                    "emoji":     "⚠️",
                    "descricao": f"GMV caiu {abs(var_gmv*100):.0f}% vs mês anterior (R${gmv_ant:,.0f} → R${gmv_ref:,.0f})",
                    "peso":      1,
                })
                severidade_score += 1

        # Queda de ticket com volume mantido = possível abuso de cupom ou preço errado
        if var_tick is not None and var_tick <= -0.20 and var_ped is not None and var_ped >= -0.10:
            causas.append({
                "tipo":      "TICKET_CAINDO",
                "emoji":     "🏷️",
                "descricao": f"Ticket médio caiu {abs(var_tick*100):.0f}% mantendo volume. Verificar cupons ou reprecificação.",
                "peso":      2,
            })
            severidade_score += 2

        # Volume zerou — caso mais crítico
        if ped_ref == 0 and ped_ant > 0:
            causas.append({
                "tipo":      "ZERO_VENDAS",
                "emoji":     "🔴",
                "descricao": f"Loja zerou vendas no mês (tinha {ped_ant} pedidos em {m_ant})",
                "peso":      5,
            })
            severidade_score += 5

        # ── 5. CHURN DE COMPRADORES (Q7) — só para lojas com queda severa ────
        if var_gmv is not None and var_gmv <= -QUEDA_GMV_CRITICA:
            try:
                ref_ini, ref_fim, corte = _janela_churn(data_inicio, data_fim)
                df_churn = connector.buscar_clientes_churned(conta_id, ref_ini, ref_fim, corte)
                n_churned = len(df_churn)
                receita_perdida = float(df_churn["receita_total_historico"].sum()) if not df_churn.empty else 0

                resultado["sinais"]["clientes_churned"]   = n_churned
                resultado["sinais"]["receita_perdida_R$"] = round(receita_perdida, 2)

                if n_churned >= 5:
                    causas.append({
                        "tipo":      "CHURN_COMPRADORES",
                        "emoji":     "🚪",
                        "descricao": f"{n_churned} compradores recorrentes sumiram (R${receita_perdida:,.0f} em risco histórico)",
                        "peso":      3,
                    })
                    severidade_score += 3
            except:
                pass  # Query de churn é opcional — não bloqueia o diagnóstico

        # ── 6. MONTA CAUSA RAIZ RESUMIDA ─────────────────────────────────────
        causas.sort(key=lambda x: x["peso"], reverse=True)

        if not causas:
            # Sem sinal claro — mas loja está na lista de queda
            causa_raiz = "📊 Queda gradual — análise manual recomendada"
            if var_gmv is not None:
                causa_raiz = f"📊 GMV {'+' if var_gmv>0 else ''}{var_gmv*100:.0f}% vs mês ant. — sem causa isolada clara"
        else:
            # Top causa (emoji + texto curto para caber na tabela)
            top = causas[0]
            causa_raiz = f"{top['emoji']} {top['descricao']}"
            # Se há 2+ causas, adiciona indicativo
            if len(causas) >= 2:
                causa_raiz += f" (+{len(causas)-1} fator{'es' if len(causas)>2 else ''})"

        # ── 7. SEVERIDADE FINAL ───────────────────────────────────────────────
        if severidade_score >= 6:
            severidade = "CRÍTICO"
        elif severidade_score >= 3:
            severidade = "ATENÇÃO"
        else:
            severidade = "INVESTIGAR"

        resultado["causa_raiz"] = causa_raiz
        resultado["causas"]     = causas
        resultado["severidade"] = severidade

    except Exception as e:
        resultado["causa_raiz"] = f"⚠️ Erro ao diagnosticar: {str(e)[:80]}"
        resultado["erro"]       = str(e)

    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# BATCH — roda para todas as lojas "SEM VENDAS RECENTES" do pipeline
# ══════════════════════════════════════════════════════════════════════════════

def diagnosticar_queda_batch(
    df_pipeline: pd.DataFrame,
    connector,
    status_alvo: str = "SEM VENDAS RECENTES",
    progress_callback=None,
) -> dict[int, dict]:
    """
    Recebe o DataFrame do pipeline e roda diagnóstico de queda
    para todas as lojas com status_alvo.

    Retorna dict: { loja_id → resultado_diagnostico }

    progress_callback: função (atual, total) chamada a cada loja processada (para barra de progresso)
    """
    lojas = df_pipeline[df_pipeline["status_loja"] == status_alvo]
    resultados = {}

    total = len(lojas)
    for i, (_, row) in enumerate(lojas.iterrows()):
        loja_id   = int(row["loja_id"])
        nome_loja = str(row.get("nome_loja", "—"))

        resultados[loja_id] = diagnosticar_queda(loja_id, nome_loja, connector)

        if progress_callback:
            progress_callback(i + 1, total)

    return resultados


# ══════════════════════════════════════════════════════════════════════════════
# FORMATAR CAUSA RAIZ PARA TABELA
# Versão curta (max ~60 chars) para caber na coluna
# ══════════════════════════════════════════════════════════════════════════════

def formatar_causa_curta(resultado: dict) -> str:
    """Retorna string curta da causa raiz para exibir na coluna da tabela."""
    if resultado.get("erro"):
        return "⚠️ Erro no diagnóstico"
    causas = resultado.get("causas", [])
    if not causas:
        sinais = resultado.get("sinais", {})
        var = sinais.get("var_gmv_pct")
        if var is not None:
            return f"📊 GMV {'+' if var > 0 else ''}{var:.0f}% vs mês ant."
        return "📊 Análise manual recomendada"
    top = causas[0]
    texto = top["descricao"]
    return f"{top['emoji']} {texto[:55]}{'…' if len(texto) > 55 else ''}"


def formatar_variacao_gmv(resultado: dict) -> str:
    """Formata variação de GMV para exibir na tabela (ex: '-42%')."""
    var = resultado.get("variacao_gmv_pct")
    if var is None:
        return "—"
    sinal = "+" if var > 0 else ""
    return f"{sinal}{var*100:.0f}%"
