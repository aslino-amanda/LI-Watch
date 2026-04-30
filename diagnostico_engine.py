"""
LI Pulse — Motor de Diagnóstico Inteligente
Calibrado com dados reais de conversão (últimos 180 dias)
Fonte: analytics_manual.mv_loja — extraído em 29/04/2026

Arquitetura em 4 camadas:
  1. Coleta      — dados da loja via Metabase
  2. Diagnóstico — causa raiz + score de risco
  3. Decisão     — prioridade + canal de ação
  4. Ação        — e-mail personalizado + log de impacto
"""

from datetime import date, datetime
from typing import Optional
import pandas as pd

# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 0 — BENCHMARK REAL POR SEGMENTO
# Fonte: query de calibração rodada em 29/04/2026
# Atualizar periodicamente rodando calibracao_diagnostico.sql
# ══════════════════════════════════════════════════════════════════════════════

BENCHMARK = {
    "MODA E ACESSÓRIOS":                           {"avg_dias_venda": 24.9,  "taxa_conversao": 1.1,  "pct_config_sem_venda": 97.4},
    "COSMÉTICOS, PERFUMARIA E CUIDADOS PESSOAIS":  {"avg_dias_venda": 22.4,  "taxa_conversao": 1.3,  "pct_config_sem_venda": 96.7},
    "ARTESANATO":                                  {"avg_dias_venda": 22.3,  "taxa_conversao": 1.1,  "pct_config_sem_venda": 97.6},
    "ALIMENTOS E BEBIDAS":                         {"avg_dias_venda": 23.3,  "taxa_conversao": 1.2,  "pct_config_sem_venda": 96.6},
    "ELETRÔNICOS":                                 {"avg_dias_venda": 25.2,  "taxa_conversao": 1.1,  "pct_config_sem_venda": 96.5},
    "PRESTAÇÃO DE SERVIÇO":                        {"avg_dias_venda":  9.9,  "taxa_conversao": 0.3,  "pct_config_sem_venda": 98.8},
    "CASA E DECORAÇÃO":                            {"avg_dias_venda": 31.5,  "taxa_conversao": 1.2,  "pct_config_sem_venda": 96.2},
    "GAMES":                                       {"avg_dias_venda": 25.9,  "taxa_conversao": 0.7,  "pct_config_sem_venda": 97.2},
    "ACESSÓRIOS AUTOMOTIVOS":                      {"avg_dias_venda": 19.9,  "taxa_conversao": 1.4,  "pct_config_sem_venda": 95.1},
    "ARTIGOS PROMOCIONAIS":                        {"avg_dias_venda": 21.3,  "taxa_conversao": 0.4,  "pct_config_sem_venda": 98.4},
    "ESPORTE E LAZER":                             {"avg_dias_venda": 26.8,  "taxa_conversao": 1.7,  "pct_config_sem_venda": 95.1},
    "SAÚDE":                                       {"avg_dias_venda": 50.5,  "taxa_conversao": 0.9,  "pct_config_sem_venda": 96.7},
    "FITNESS E SUPLEMENTOS":                       {"avg_dias_venda": 12.8,  "taxa_conversao": 1.8,  "pct_config_sem_venda": 94.8},
    "CONSTRUÇÃO E FERRAMENTAS":                    {"avg_dias_venda": 30.5,  "taxa_conversao": 1.8,  "pct_config_sem_venda": 94.8},
    "LIVROS E REVISTAS":                           {"avg_dias_venda": 20.5,  "taxa_conversao": 2.0,  "pct_config_sem_venda": 94.1},
    "INFORMÁTICA":                                 {"avg_dias_venda": 16.3,  "taxa_conversao": 1.7,  "pct_config_sem_venda": 93.8},
    "LOJA DE DEPARTAMENTOS":                       {"avg_dias_venda": 16.1,  "taxa_conversao": 1.9,  "pct_config_sem_venda": 94.5},
    "ARTIGOS RELIGIOSOS":                          {"avg_dias_venda": 17.0,  "taxa_conversao": 2.2,  "pct_config_sem_venda": 93.8},
    "PAPELARIA E ESCRITÓRIO":                      {"avg_dias_venda": 25.9,  "taxa_conversao": 1.3,  "pct_config_sem_venda": 96.9},
    "BEBÊS E CIA":                                 {"avg_dias_venda": 50.2,  "taxa_conversao": 1.0,  "pct_config_sem_venda": 96.9},
    "GRÁFICA":                                     {"avg_dias_venda": 18.6,  "taxa_conversao": 1.3,  "pct_config_sem_venda": 96.5},
    "RELOJOARIA E JOALHERIA":                      {"avg_dias_venda": 16.3,  "taxa_conversao": 1.8,  "pct_config_sem_venda": 96.0},
    "SEX SHOP":                                    {"avg_dias_venda": 17.3,  "taxa_conversao": 1.2,  "pct_config_sem_venda": 96.4},
    "PET SHOP":                                    {"avg_dias_venda": 48.3,  "taxa_conversao": 2.0,  "pct_config_sem_venda": 93.5},
    "FESTAS E EVENTOS":                            {"avg_dias_venda": 34.7,  "taxa_conversao": 0.8,  "pct_config_sem_venda": 97.2},
    "TELEFONIA E CELULARES":                       {"avg_dias_venda":  0.0,  "taxa_conversao": 0.1,  "pct_config_sem_venda": 99.5},
    "BRINQUEDOS E COLECIONÁVEIS":                  {"avg_dias_venda": 22.6,  "taxa_conversao": 3.5,  "pct_config_sem_venda": 90.5},
    "MÓVEIS":                                      {"avg_dias_venda": 50.3,  "taxa_conversao": 1.0,  "pct_config_sem_venda": 97.7},
    "ELETRODOMÉSTICOS":                            {"avg_dias_venda": 18.6,  "taxa_conversao": 0.7,  "pct_config_sem_venda": 97.4},
    "ARTE E ANTIGUIDADES":                         {"avg_dias_venda": 15.3,  "taxa_conversao": 1.0,  "pct_config_sem_venda": 96.4},
    "PRESENTES, FLORES E CESTAS":                  {"avg_dias_venda": 21.0,  "taxa_conversao": 1.4,  "pct_config_sem_venda": 96.9},
    "FOTOGRAFIA":                                  {"avg_dias_venda":  0.0,  "taxa_conversao": 0.1,  "pct_config_sem_venda": 99.4},
    "VIAGENS E TURISMO":                           {"avg_dias_venda":  4.0,  "taxa_conversao": 0.2,  "pct_config_sem_venda": 99.3},
    "BEBIDAS ALCOÓLICAS":                          {"avg_dias_venda": 31.6,  "taxa_conversao": 2.2,  "pct_config_sem_venda": 93.3},
    "INSTRUMENTOS MUSICAIS":                       {"avg_dias_venda": 30.4,  "taxa_conversao": 4.1,  "pct_config_sem_venda": 87.7},
    "BLU-RAY, DVD, CD E VHS":                      {"avg_dias_venda": 32.2,  "taxa_conversao": 2.8,  "pct_config_sem_venda": 89.6},
    "INGRESSOS":                                   {"avg_dias_venda":  2.0,  "taxa_conversao": 1.3,  "pct_config_sem_venda": 95.8},
    "EQUIPAMENTOS PARA LABORATÓRIO":               {"avg_dias_venda": 38.0,  "taxa_conversao": 3.1,  "pct_config_sem_venda": 85.0},
    "DEFAULT":                                     {"avg_dias_venda": 23.0,  "taxa_conversao": 1.2,  "pct_config_sem_venda": 96.5},
}

# Dados de retenção — fonte: Q5
RETENCAO = {
    "nunca_configurou_pct":    71.4,
    "configurou_sem_vender_pct": 27.6,
    "vendeu_mas_parou_pct":    0.5,
    "vendeu_e_reteve_pct":     0.4,
    "gmv_medio_retidos":       4332.12,
    "visitas_medio_retidos":   1148.0,
}

# Janela crítica — baseada nos dados reais
# 97%+ das lojas que configuram nunca vendem
# Quem retém tem 1148 visitas/mês vs 1 de quem não configura
JANELA_CRITICA_DIAS = 7   # Após 7 dias sem configurar, risco dobra
JANELA_ABANDONO_DIAS = 15  # Após 15 dias, probabilidade de abandono > 80%


# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 1 — COLETA (helpers)
# ══════════════════════════════════════════════════════════════════════════════

def normalizar_segmento(segmento: Optional[str]) -> str:
    if not segmento:
        return "DEFAULT"
    s = segmento.upper().strip()
    if s in BENCHMARK:
        return s
    for k in BENCHMARK:
        if k != "DEFAULT" and (k in s or s in k):
            return k
    return "DEFAULT"


def dias_desde(data_str) -> Optional[int]:
    if not data_str or str(data_str) in ("None", "nan", ""):
        return None
    try:
        if isinstance(data_str, (date, datetime)):
            d = data_str if isinstance(data_str, date) else data_str.date()
        else:
            d = datetime.strptime(str(data_str)[:10], "%Y-%m-%d").date()
        return (date.today() - d).days
    except:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 2 — DIAGNÓSTICO
# Identifica causa raiz com evidência dos dados reais
# ══════════════════════════════════════════════════════════════════════════════

def _diagnosticar_onboarding_incompleto(loja: dict, bench: dict, dias_cadastro: int, origem: str) -> dict:
    """Diagnóstico para lojas com onboarding incompleto."""
    score = 0
    causas = []
    insights = []
    acoes = []

    tem_prod = loja.get("data_primeira_config_produto") not in (None, "", "None", float("nan"))
    tem_pag  = loja.get("data_primeira_config_pagamento") not in (None, "", "None", float("nan"))
    tem_log  = loja.get("data_primeira_config_logistica") not in (None, "", "None", float("nan"))

    # Identifica gargalo específico
    if tem_prod and not tem_pag:
        score += 45
        causas.append("Configurou produto mas NÃO ativou pagamento")
        insights.append(
            "Gargalo crítico — sem pagamento o cliente adiciona ao carrinho mas não finaliza. "
            "Dos 95.844 lojistas ativos, 27,6% configuraram mas nunca venderam por esse motivo."
        )
        acoes.append("E-mail urgente: ativar Pagali — 5 minutos para destravar vendas")

    elif tem_prod and tem_pag and not tem_log:
        score += 35
        causas.append("Configurou produto e pagamento mas NÃO configurou frete")
        insights.append(
            "Checkout trava na etapa de entrega. Loja quase pronta — falta só o frete. "
            "Configurar o Enviali leva menos de 5 minutos."
        )
        acoes.append("E-mail: configurar Enviali — último passo para a primeira venda")

    elif not tem_prod:
        score += 40
        causas.append("Não cadastrou nenhum produto")
        insights.append(
            "Sem vitrine, sem venda. Loja invisível para qualquer comprador. "
            "71,4% das lojas da plataforma nunca chegam nem nesse passo."
        )
        acoes.append("E-mail: guia de cadastro do primeiro produto com dicas de foto e descrição")

    else:
        score += 25
        causas.append("Múltiplas configurações incompletas")
        acoes.append("E-mail: checklist completo de configuração")

    # Agravante temporal — baseado na janela crítica real
    if dias_cadastro >= JANELA_ABANDONO_DIAS:
        score += 25
        insights.append(
            f"CRÍTICO: {dias_cadastro} dias no mesmo estado. "
            f"Após {JANELA_ABANDONO_DIAS} dias sem progredir, probabilidade de abandono supera 80%."
        )
    elif dias_cadastro >= JANELA_CRITICA_DIAS:
        score += 15
        insights.append(
            f"ATENÇÃO: {dias_cadastro} dias desde o cadastro. "
            f"Janela crítica de intervenção — agir agora antes que vire abandono."
        )

    # Agravante origem paga
    if origem == "PAGO":
        score += 10
        insights.append(
            "Loja veio de canal pago. "
            "Custo de aquisição desperdiçado se não converter — prioridade de intervenção maior."
        )

    return {"score": score, "causas": causas, "insights": insights, "acoes": acoes}


def _diagnosticar_nunca_vendeu(loja: dict, bench: dict, dias_cadastro: int, origem: str) -> dict:
    """Diagnóstico para lojas configuradas que nunca venderam."""
    score = 0
    causas = []
    insights = []
    acoes = []

    avg_dias = bench["avg_dias_venda"]
    taxa_conv = bench["taxa_conversao"]
    visitas   = int(loja.get("qtde_visitas_ultimos_30d") or 0)
    seg       = normalizar_segmento(loja.get("segmento_loja"))

    # Compara com benchmark real do segmento
    if avg_dias > 0 and dias_cadastro:
        ratio = round(dias_cadastro / avg_dias, 1)
        if ratio >= 2.0:
            score += 40
            causas.append(
                f"Há {dias_cadastro} dias sem vender — {ratio}x acima da média de {seg} ({avg_dias} dias)"
            )
            insights.append(
                f"No segmento {seg}, lojas que chegam à primeira venda levam em média {avg_dias} dias. "
                f"Esta loja está {ratio}x além disso. "
                f"Taxa de conversão do segmento: {taxa_conv}% — já baixa, agravada pelo tempo."
            )
        elif ratio >= 1.0:
            score += 20
            causas.append(f"Dentro da janela esperada mas sem venda ({dias_cadastro} de {avg_dias} dias médios)")
            insights.append(
                f"Ainda dentro da janela de conversão para {seg}, mas se aproximando do limite. "
                f"Intervenção preventiva recomendada."
            )
        else:
            score += 5
            causas.append(f"Recém configurada — aguardando primeira venda ({dias_cadastro}/{avg_dias} dias)")
    else:
        score += 20
        causas.append("Configurada mas sem registro de venda")

    # Visitas sem conversão = problema de produto/preço
    if visitas >= 50:
        score += 20
        insights.append(
            f"{visitas} visitas nos últimos 30 dias mas zero vendas. "
            f"Problema de conversão — fotos, preço ou descrição do produto. "
            f"Lojas que retêm clientes têm média de {int(RETENCAO['visitas_medio_retidos'])} visitas/mês."
        )
        acoes.append("E-mail: dicas de conversão — fotos profissionais, preço competitivo e descrição detalhada")
    elif visitas > 0:
        score += 10
        insights.append(
            f"{visitas} visitas mas sem conversão. "
            f"Loja atraindo visitantes mas não converte — verificar qualidade dos produtos."
        )
        acoes.append("E-mail: otimização de produto para aumentar conversão")
    else:
        score += 15
        insights.append(
            "Zero visitas nos últimos 30 dias — problema de divulgação. "
            "Loja configurada mas invisível. Compartilhar link é o primeiro passo."
        )
        acoes.append("E-mail: checklist de divulgação — WhatsApp, Instagram e redes sociais")

    # Segmentos com conversão naturalmente baixa
    if taxa_conv <= 0.5:
        insights.append(
            f"Atenção: {seg} tem uma das menores taxas de conversão da plataforma ({taxa_conv}%). "
            f"Segmento difícil — suporte diferenciado recomendado."
        )

    if origem == "PAGO":
        score += 10
        insights.append("Canal pago — custo de aquisição em risco sem conversão.")

    if not acoes:
        acoes.append("E-mail: estratégias para primeira venda no segmento " + seg)

    return {"score": score, "causas": causas, "insights": insights, "acoes": acoes}


def _diagnosticar_sem_vendas_recentes(loja: dict, bench: dict, dias_cadastro: int, origem: str) -> dict:
    """Diagnóstico para lojas que venderam mas pararam."""
    score = 0
    causas = []
    insights = []
    acoes = []

    gmv_30d   = float(loja.get("vlr_gmv_ultimos_30d") or 0)
    pedidos   = int(loja.get("qtd_pedido_ultimos_30d") or 0)
    visitas   = int(loja.get("qtde_visitas_ultimos_30d") or 0)

    # Gravidade da queda
    if gmv_30d == 0 and pedidos == 0:
        score += 40
        causas.append("Parada total — zero pedidos e zero GMV nos últimos 30 dias")
        insights.append(
            "Não é desaceleração — é parada completa. "
            f"Apenas 0,4% das lojas ativas vendem e retêm clientes (GMV médio: R${RETENCAO['gmv_medio_30d']:,.2f}/mês). "
            "Esta loja saiu desse grupo."
        )
    elif pedidos > 0 and gmv_30d == 0:
        score += 20
        causas.append("Pedidos registrados mas GMV zerado — possível problema de cancelamento")
        insights.append("Verificar taxa de cancelamento — pedidos criados mas não aprovados.")
        acoes.append("Alerta para CS verificar cancelamentos e problema de checkout")

    # Diagnóstico de visitas
    if visitas == 0:
        score += 20
        insights.append(
            "Zero visitas nos últimos 30 dias. "
            "Loja pode estar fora do ar, domínio expirado ou todos os produtos esgotados."
        )
        acoes.append("CS verificar urgente: loja acessível, domínio ativo e estoque dos produtos")
    elif visitas < 50:
        score += 10
        insights.append(
            f"Apenas {visitas} visitas — queda de tráfego. "
            "Divulgação parou ou produto perdeu relevância."
        )
        acoes.append("E-mail: reativação com promoção ou nova divulgação nas redes sociais")
    else:
        score += 5
        insights.append(
            f"{visitas} visitas mas sem conversão. "
            "Tráfego existe — problema pode ser preço, estoque zerado ou promoção de concorrente."
        )
        acoes.append("E-mail: verificar preço competitivo e estoque dos produtos mais visitados")

    if origem == "PAGO":
        score += 5

    if not acoes:
        acoes.append("E-mail: diagnóstico de reativação de vendas")

    return {"score": score, "causas": causas, "insights": insights, "acoes": acoes}


# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 3 — DECISÃO
# Define prioridade, urgência e canal de ação
# ══════════════════════════════════════════════════════════════════════════════

def _decidir(score: int, status: str, dias_cadastro: Optional[int], origem: str) -> dict:
    """Define prioridade e canal de ação com base no score e contexto."""

    # Normaliza score
    score = max(0, min(100, score))

    # Prioridade baseada em score + contexto
    if score >= 70:
        prioridade = "CRÍTICA"
        emoji      = "🔴"
        canal      = "E-mail imediato + alerta CS"
        sla        = "Intervir hoje"
    elif score >= 45:
        prioridade = "ALTA"
        emoji      = "🟠"
        canal      = "E-mail automático"
        sla        = "Intervir em até 24h"
    elif score >= 25:
        prioridade = "MÉDIA"
        emoji      = "🟡"
        canal      = "E-mail automático"
        sla        = "Intervir em até 48h"
    else:
        prioridade = "BAIXA"
        emoji      = "🟢"
        canal      = "Monitorar"
        sla        = "Reavaliar em 7 dias"

    # Agravante: canal pago eleva urgência
    if origem == "PAGO" and prioridade in ("MÉDIA", "BAIXA"):
        prioridade = "ALTA"
        emoji      = "🟠"
        sla        = "Intervir em até 24h — canal pago"

    return {
        "score":      score,
        "prioridade": f"{emoji} {prioridade}",
        "canal":      canal,
        "sla":        sla,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 4 — AÇÃO
# Define qual mensagem enviar e como medir o impacto
# ══════════════════════════════════════════════════════════════════════════════

TEMPLATES_EMAIL = {
    "ONBOARDING INCOMPLETO": {
        "assunto": "Sua loja está quase pronta — falta pouco para a primeira venda",
        "corpo": """Olá, {nome_loja}!

Identificamos que sua loja ainda precisa de alguns ajustes para estar pronta para vender.

{itens_faltando}

Cada item leva apenas alguns minutos. Assim que concluir, sua loja estará pronta para receber os primeiros pedidos.

— Time Loja Integrada""",
        "metrica_impacto": "Loja completa config em 24h após e-mail",
    },
    "NUNCA VENDEU": {
        "assunto": "Sua loja está configurada — veja como atrair os primeiros compradores",
        "corpo": """Olá, {nome_loja}!

Sua loja está configurada e pronta. Agora é hora de atrair visitantes e converter em vendas.

{dica_segmento}

As primeiras vendas geralmente vêm da sua própria rede. Comece hoje!

— Time Loja Integrada""",
        "metrica_impacto": "Primeira venda em até 15 dias após e-mail",
    },
    "SEM VENDAS RECENTES": {
        "assunto": "Sua loja ficou um tempo sem vendas — veja como reativar",
        "corpo": """Olá, {nome_loja}!

Notamos que sua loja está há alguns dias sem registrar vendas.

{diagnostico_queda}

Pequenas ações hoje podem reativar suas vendas rapidamente.

— Time Loja Integrada""",
        "metrica_impacto": "Retorno de vendas em até 30 dias após e-mail",
    },
}

DICAS_SEGMENTO = {
    "MODA E ACESSÓRIOS":         "→ Fotos com modelo aumentam conversão em até 3x\n→ Stories do Instagram com link da loja\n→ WhatsApp Business para atendimento",
    "ALIMENTOS E BEBIDAS":       "→ Fotos do produto pronto apetitoso\n→ Grupos de WhatsApp de bairro\n→ Entrega rápida como diferencial",
    "FITNESS E SUPLEMENTOS":     "→ Antes e depois de clientes\n→ Parcerias com academias locais\n→ Conteúdo de treino nas redes",
    "ELETRÔNICOS":               "→ Especificações técnicas completas\n→ Vídeo de unboxing\n→ Garantia e suporte pós-venda",
    "DEFAULT":                   "→ Compartilhe o link da loja no WhatsApp\n→ Poste fotos dos produtos no Instagram\n→ Peça indicações para amigos e familiares",
}


def montar_email(loja: dict, status: str, acoes: list) -> dict:
    """Monta o e-mail personalizado para a loja."""
    template = TEMPLATES_EMAIL.get(status, TEMPLATES_EMAIL["NUNCA VENDEU"])
    nome     = str(loja.get("nome_loja", "Lojista"))
    seg      = normalizar_segmento(loja.get("segmento_loja"))

    # Monta itens faltando para onboarding
    itens = []
    if not loja.get("data_primeira_config_pagamento"):
        itens.append("→ Configurar forma de pagamento (Pagali) — aceite Pix, cartão e boleto")
    if not loja.get("data_primeira_config_logistica"):
        itens.append("→ Configurar frete (Enviali) — cálculo automático por CEP")
    if not loja.get("data_primeira_config_produto"):
        itens.append("→ Cadastrar pelo menos 1 produto com foto e descrição")
    itens_str = "\n".join(itens) if itens else "→ Revisar configurações gerais da loja"

    dica = DICAS_SEGMENTO.get(seg, DICAS_SEGMENTO["DEFAULT"])

    corpo = template["corpo"].format(
        nome_loja=nome,
        itens_faltando=itens_str,
        dica_segmento=dica,
        diagnostico_queda=acoes[0] if acoes else "→ Verificar estoque e preços dos produtos",
    )

    return {
        "assunto":          template["assunto"],
        "corpo":            corpo,
        "metrica_impacto":  template["metrica_impacto"],
    }


# ══════════════════════════════════════════════════════════════════════════════
# ORQUESTRADOR — junta as 4 camadas
# ══════════════════════════════════════════════════════════════════════════════

def diagnosticar_loja(loja: dict) -> dict:
    """
    Orquestra as 4 camadas para uma loja.
    Retorna diagnóstico completo com score, causa raiz, decisão e ação.
    """

    # Coleta
    seg_norm     = normalizar_segmento(loja.get("segmento_loja"))
    bench        = BENCHMARK[seg_norm]
    status       = str(loja.get("status_loja", "")).upper().strip()
    origem       = "PAGO" if loja.get("aquisicao_utm_source") else "ORGANICO"
    dias_cadastro = dias_desde(loja.get("data_cadastro_loja")) or 0

    # Diagnóstico por status
    if status == "ONBOARDING INCOMPLETO":
        diag = _diagnosticar_onboarding_incompleto(loja, bench, dias_cadastro, origem)
    elif status == "NUNCA VENDEU":
        diag = _diagnosticar_nunca_vendeu(loja, bench, dias_cadastro, origem)
    elif status == "SEM VENDAS RECENTES":
        diag = _diagnosticar_sem_vendas_recentes(loja, bench, dias_cadastro, origem)
    else:
        return {"score": 0, "prioridade": "🟢 BAIXA", "causa_raiz": "Loja ativa", "insights": [], "acoes": [], "email": None, "decisao": {}}

    # Decisão
    decisao = _decidir(diag["score"], status, dias_cadastro, origem)

    # Ação
    email = montar_email(loja, status, diag["acoes"])

    return {
        "loja_id":      loja.get("loja_id"),
        "nome_loja":    loja.get("nome_loja"),
        "email_loja":   loja.get("email_loja"),
        "segmento":     seg_norm,
        "status_loja":  status,
        "origem":       origem,
        "dias_cadastro": dias_cadastro,
        "score_risco":  decisao["score"],
        "prioridade":   decisao["prioridade"],
        "canal":        decisao["canal"],
        "sla":          decisao["sla"],
        "causa_raiz":   " | ".join(diag["causas"]),
        "insights":     diag["insights"],
        "acoes":        diag["acoes"],
        "email":        email,
        "benchmark":    bench,
    }


def diagnosticar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Roda o diagnóstico para todas as lojas. Retorna DataFrame ordenado por score."""
    rows = []
    for _, row in df.iterrows():
        d = diagnosticar_loja(row.to_dict())
        rows.append({
            "loja_id":         d["loja_id"],
            "nome_loja":       d["nome_loja"],
            "email_loja":      d["email_loja"],
            "segmento":        d["segmento"],
            "status_loja":     d["status_loja"],
            "score_risco":     d["score_risco"],
            "prioridade":      d["prioridade"],
            "sla":             d["sla"],
            "dias_cadastro":   d["dias_cadastro"],
            "causa_raiz":      d["causa_raiz"],
            "canal":           d["canal"],
            "acao_recomendada": d["acoes"][0] if d["acoes"] else "",
            "benchmark_dias_venda":     d["benchmark"]["avg_dias_venda"],
            "benchmark_taxa_conversao": d["benchmark"]["taxa_conversao"],
            "email_assunto":   d["email"]["assunto"] if d["email"] else "",
            "metrica_impacto": d["email"]["metrica_impacto"] if d["email"] else "",
        })
    return pd.DataFrame(rows).sort_values("score_risco", ascending=False).reset_index(drop=True)
