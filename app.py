"""
LI Watch · Raio X — v3
Loja Integrada · Time de Automação · 2026

Changelog v3:
  - Score engine atualizado: lojas ATIVAS em queda recebem status QUEDA CRÍTICA
    e score proporcional à gravidade (bug da Arco Íris LED corrigido)
  - Alerta 6: detecta lojas ativas com queda de GMV 20%+ no painel principal
  - Diagnóstico individual agora inclui tendência semanal + causa raiz de queda
  - Tab Top Sellers: ação N2 inline por loja (não só na busca individual)
  - Perfil de loja na busca individual detecta QUEDA CRÍTICA antes de marcar ATIVA
  - Compatibilidade total com score_engine.py e alertas.py atualizados
"""

import streamlit as st
import pandas as pd
import requests
import io
import json
import os
from datetime import datetime, date as _date_type

# ── IMPORTS CONDICIONAIS ──────────────────────────────────────────────────────
try:
    from diagnostico_engine import diagnosticar_loja
    ENGINE_OK = True
except Exception:
    ENGINE_OK = False

try:
    from diagnostico_queda import diagnosticar_queda, diagnosticar_queda_batch
    QUEDA_OK = True
except Exception:
    QUEDA_OK = False

try:
    from score_engine import calcular_scores_df
    SCORE_ENGINE_OK = True
except Exception:
    SCORE_ENGINE_OK = False

try:
    from alertas import detectar_alertas
    ALERTAS_OK = True
except Exception:
    ALERTAS_OK = False

# ── LOG DE USO ────────────────────────────────────────────────────────────────
LOG_FILE = "uso_liwatch.json"

def registrar_uso(loja_id, nome_loja, status, score):
    try:
        log = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r") as f:
                log = json.load(f)
        log.append({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "loja_id": loja_id, "nome": nome_loja,
                    "status": status, "score": score})
        with open(LOG_FILE, "w") as f:
            json.dump(log[-500:], f)
    except Exception:
        pass

def ler_metricas_uso():
    try:
        if not os.path.exists(LOG_FILE):
            return []
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


# ── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="LI Watch · Raio X",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html,body,[class*="css"]{font-family:'Inter',sans-serif;}
.block-container{padding:1.2rem 2rem 2rem;background:#F2EDE4;}
.main{background:#F2EDE4;}

.liwatch-header{background:#0D4F4A;border-radius:14px;padding:1rem 1.5rem;
    margin-bottom:1.2rem;display:flex;align-items:center;justify-content:space-between;}
.liwatch-logo{font-size:20px;font-weight:700;color:#D4F53C;}
.liwatch-sub{font-size:12px;color:#9DBDBB;margin-top:2px;}
.liwatch-badge{background:#1A6A64;border-radius:8px;padding:4px 12px;
    font-size:11px;color:#9DBDBB;}

.torre{background:white;border-radius:14px;padding:1.2rem;height:100%;}
.torre-titulo{font-size:13px;font-weight:700;text-transform:uppercase;
    letter-spacing:.07em;margin-bottom:1rem;padding-bottom:.5rem;
    border-bottom:2px solid #F2EDE4;}
.torre-item{display:flex;justify-content:space-between;align-items:center;
    padding:6px 0;border-bottom:1px solid #F5F2EE;font-size:13px;}
.torre-item:last-child{border-bottom:none;}
.torre-label{color:#5A7A78;}
.torre-valor{font-weight:600;color:#1A2E2B;}
.tag-sim{background:#D1FAF6;color:#0D4F4A;padding:2px 8px;border-radius:20px;
    font-size:11px;font-weight:600;}
.tag-nao{background:#FEF2F2;color:#E24B4A;padding:2px 8px;border-radius:20px;
    font-size:11px;font-weight:600;}
.tag-info{background:#EEEDFE;color:#3C3489;padding:2px 8px;border-radius:20px;
    font-size:11px;font-weight:600;}

.score-box{border-radius:12px;padding:1rem;text-align:center;margin-bottom:.8rem;}
.score-num{font-size:48px;font-weight:700;line-height:1;}

.alerta-critico{background:#FEF2F2;border-left:4px solid #E24B4A;border-radius:8px;
    padding:.8rem 1rem;margin-bottom:1rem;font-size:13px;color:#991B1B;}
.alerta-atencao{background:#FFFBEB;border-left:4px solid #F59E0B;border-radius:8px;
    padding:.8rem 1rem;margin-bottom:1rem;font-size:13px;color:#92400E;}
.alerta-ok{background:#F0FDF4;border-left:4px solid #22C55E;border-radius:8px;
    padding:.8rem 1rem;margin-bottom:1rem;font-size:13px;color:#166534;}

.insight-item{font-size:13px;color:#444;padding:6px 0;border-bottom:1px solid #F5F2EE;line-height:1.5;}
.insight-item:last-child{border-bottom:none;}

.email-box{background:#F8F5F0;border:1px solid #E8E4DE;border-radius:12px;
    padding:1.2rem;font-size:13px;line-height:1.7;color:#1A2E2B;}
.email-hdr{font-size:11px;color:#888;border-bottom:1px solid #E8E4DE;
    padding-bottom:8px;margin-bottom:12px;}

div[data-testid="stTextInput"]>div>div>input{
    border-radius:10px!important;border:2px solid #C8E8E6!important;
    background:white!important;font-size:15px!important;padding:10px 14px!important;}
.stButton>button{background:#0D4F4A!important;color:#D4F53C!important;
    font-weight:600!important;border:none!important;border-radius:10px!important;}
.stDownloadButton>button{background:#1ABCB0!important;color:#0D4F4A!important;
    font-weight:600!important;border:none!important;border-radius:10px!important;}
</style>
""", unsafe_allow_html=True)


# ── METABASE ──────────────────────────────────────────────────────────────────

def _ok():
    try:
        cfg = st.secrets["metabase"]
        return bool(cfg.get("url") and ("api_key" in cfg or "token" in cfg))
    except Exception:
        return False

def _headers():
    try:
        cfg = st.secrets["metabase"]
        if "api_key" in cfg:
            return {"x-api-key": cfg["api_key"], "Content-Type": "application/json"}
        return {"X-Metabase-Session": cfg["token"], "Content-Type": "application/json"}
    except Exception:
        return {}

def _url():
    try: return st.secrets["metabase"]["url"]
    except Exception: return ""

def _db():
    try: return int(st.secrets["metabase"]["db_id"])
    except Exception: return 11

def rodar_sql(sql):
    import urllib.request, json as _json
    s = st.secrets["metabase"]
    payload = _json.dumps({
        "database": _db(), "native": {"query": sql}, "type": "native",
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{_url()}/api/dataset", data=payload,
        headers={"Content-Type": "application/json", "x-api-key": s.get("api_key", s.get("token",""))},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = _json.loads(resp.read().decode("utf-8"))
    if "error" in data:
        raise Exception(data["error"])
    cols = [c["name"] for c in data["data"]["cols"]]
    return pd.DataFrame(data["data"]["rows"], columns=cols)


# ── QUERIES CACHEADAS ─────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def buscar_dados_loja(loja_id: int):
    sql_loja = f"""
    SELECT
        loja_id,
        COALESCE(upper(nome_loja), upper(dominio_loja), CAST(loja_id AS CHAR)) AS nome_loja,
        dominio_loja, email_loja,
        upper(segmento_loja) AS segmento_loja,
        upper(situacao_loja) AS situacao_loja,
        data_cadastro_loja,
        upper(cidade_endereco_loja) AS cidade,
        upper(estado_endereco_loja) AS estado,
        aquisicao_utm_source,
        CASE WHEN aquisicao_utm_source IS NULL THEN 'ORGÂNICO' ELSE 'PAGO' END AS origem,
        data_primeira_config_pagamento,
        data_primeira_config_logistica,
        data_primeira_config_produto,
        data_ini_plano_atual,
        upper(tipo_plano_atual) AS tipo_plano,
        vlr_plano_mrr_atual,
        CASE WHEN data_ini_plano_atual IS NOT NULL THEN 'PAGO' ELSE 'GRÁTIS' END AS status_plano,
        data_primeira_visita,
        qtde_visitas_ultimos_30d,
        data_primeira_venda,
        qtd_pedido_ultimos_30d,
        vlr_gmv_ultimos_30d,
        wizard_produto,
        flag_ativo_pagali_cartao, flag_ativo_pagali_boleto, flag_ativo_pagali_pix,
        flag_ativo_mercadopago_cartao, flag_ativo_mercadopago_boleto,
        flag_ativo_pagseguro_cartao, flag_ativo_pagseguro_boleto,
        flag_ativo_paypal_cartao, flag_ativo_outros_pagamentoexterno,
        flag_ativo_enviali, flag_ativo_enviali_correios_pac,
        flag_ativo_enviali_correios_sedex, flag_ativo_enviali_jadlog,
        flag_ativo_enviali_zum_loggi, flag_ativo_correios_pac,
        flag_ativo_correios_sedex, flag_ativo_melhor_envio,
        flag_ativo_frenet, flag_ativo_motoboy, flag_ativo_retirar_pessoalmente,
        flag_config_magalu, flag_enviou_produto_magalu,
        data_primeira_venda_magalu,
        qtd_pedido_magalu_ultimos_30d, vlr_gmv_magalu_ultimos_30d,
        CASE
            WHEN data_primeira_config_pagamento IS NULL
              OR data_primeira_config_logistica IS NULL
              OR data_primeira_config_produto   IS NULL THEN 'ONBOARDING INCOMPLETO'
            WHEN data_primeira_venda IS NULL THEN 'NUNCA VENDEU'
            WHEN coalesce(vlr_gmv_ultimos_30d,0) = 0 THEN 'SEM VENDAS RECENTES'
            ELSE 'LOJA ATIVA'
        END AS status_loja
    FROM analytics_manual.mv_loja
    WHERE loja_id = {loja_id}
    LIMIT 1
    """
    sql_onb = f"SELECT * FROM analytics_manual.temp_onboarding_lojas_30d WHERE loja_id = {loja_id} LIMIT 1"
    sql_env = f"SELECT * FROM analytics_manual.mv_enviali_loja WHERE loja_id = {loja_id} LIMIT 1"
    loja_df = rodar_sql(sql_loja)
    try:    onb_df = rodar_sql(sql_onb)
    except: onb_df = pd.DataFrame()
    try:    env_df = rodar_sql(sql_env)
    except: env_df = pd.DataFrame()
    return loja_df, onb_df, env_df

@st.cache_data(ttl=600)
def buscar_base_monitoramento():
    sql = """
    SELECT
        loja_id,
        COALESCE(upper(nome_loja), upper(dominio_loja), CAST(loja_id AS CHAR)) AS nome_loja,
        upper(segmento_loja) AS segmento_loja,
        upper(situacao_loja) AS situacao_loja,
        data_cadastro_loja,
        data_primeira_venda,
        qtd_pedido_ultimos_30d,
        vlr_gmv_ultimos_30d,
        qtde_visitas_ultimos_30d,
        data_primeira_config_pagamento,
        data_primeira_config_logistica,
        data_primeira_config_produto,
        flag_ativo_enviali,
        flag_ativo_pagali_pix,
        flag_ativo_pagali_cartao,
        data_ini_plano_atual,
        CASE WHEN data_ini_plano_atual IS NOT NULL THEN 'PAGO' ELSE 'GRATIS' END AS status_plano,
        CASE
            WHEN data_primeira_config_pagamento IS NULL
              OR data_primeira_config_logistica IS NULL
              OR data_primeira_config_produto   IS NULL THEN 'ONBOARDING INCOMPLETO'
            WHEN data_primeira_venda IS NULL            THEN 'NUNCA VENDEU'
            WHEN coalesce(vlr_gmv_ultimos_30d,0) = 0   THEN 'SEM VENDAS RECENTES'
            ELSE 'LOJA ATIVA'
        END AS status_loja,
        coalesce(datediff(current_date, data_cadastro_loja), 0) AS dias_cadastro,
        coalesce(qtde_visitas_ultimos_30d, 0) AS qtde_visitas_ultimos_30d_clean,
        coalesce(vlr_gmv_ultimos_30d, 0)      AS vlr_gmv_ultimos_30d_clean,
        coalesce(qtd_pedido_ultimos_30d, 0)   AS qtd_pedido_ultimos_30d_clean
    FROM analytics_manual.mv_loja
    WHERE situacao_loja = 'ativa'
      AND data_cadastro_loja >= current_date - interval '60' day
      AND (
            data_primeira_config_pagamento IS NULL
         OR data_primeira_config_logistica IS NULL
         OR data_primeira_config_produto   IS NULL
         OR data_primeira_venda            IS NULL
         OR coalesce(vlr_gmv_ultimos_30d,0) = 0
      )
    ORDER BY
      CASE WHEN data_ini_plano_atual IS NOT NULL THEN 0 ELSE 1 END ASC,
      dias_cadastro DESC
    """
    return rodar_sql(sql)

@st.cache_data(ttl=600)
def buscar_top_lojas(limite: int = 100):
    try:
        import metabase_connector as mb
        return mb.buscar_top_lojas(limite)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def buscar_por_nome(nome: str):
    sql = f"""
    SELECT loja_id, upper(nome_loja) AS nome_loja,
           upper(segmento_loja) AS segmento_loja,
           upper(situacao_loja) AS situacao_loja,
           email_loja
    FROM analytics_manual.mv_loja
    WHERE upper(nome_loja) LIKE upper('%{nome}%')
    LIMIT 10
    """
    return rodar_sql(sql)

@st.cache_data(ttl=300)
def buscar_tendencia_semanal_cached(loja_id: int):
    try:
        import metabase_connector as mb
        return mb.buscar_tendencia_semanal(loja_id)
    except Exception:
        return {}

@st.cache_data(ttl=600)
def buscar_historico_mensal_batch_cached(ids: tuple):
    try:
        import metabase_connector as mb
        return mb.buscar_historico_mensal_batch(list(ids))
    except Exception:
        return {}


# ── DEMOS ─────────────────────────────────────────────────────────────────────

def demo_loja(loja_id):
    demos = {
        421: dict(loja_id=421, nome_loja="MODA DA ANA", dominio_loja="modadaana.lojaintegrada.com",
            email_loja="ana@modadaana.com", segmento_loja="MODA E ACESSÓRIOS",
            situacao_loja="ATIVA", status_plano="GRÁTIS", origem="ORGÂNICO",
            data_cadastro_loja="2026-04-22", data_primeira_config_produto="2026-04-22",
            data_primeira_config_pagamento=None, data_primeira_config_logistica=None,
            data_primeira_visita=None, data_primeira_venda=None,
            qtde_visitas_ultimos_30d=0, vlr_gmv_ultimos_30d=0, qtd_pedido_ultimos_30d=0,
            status_loja="ONBOARDING INCOMPLETO", cidade="SÃO PAULO", estado="SP",
            tipo_plano="GRÁTIS", vlr_plano_mrr_atual=0),
        233932: dict(loja_id=233932, nome_loja="ARCO ÍRIS LED", dominio_loja="arcoirisleds.com.br",
            email_loja="contato@arcoirisleds.com.br", segmento_loja="ELETRÔNICOS",
            situacao_loja="ATIVA", status_plano="PAGO", origem="PAGO",
            data_cadastro_loja="2023-05-10", data_primeira_config_produto="2023-05-10",
            data_primeira_config_pagamento="2023-05-10", data_primeira_config_logistica="2023-05-10",
            data_primeira_visita="2023-05-11", data_primeira_venda="2023-05-15",
            qtde_visitas_ultimos_30d=312, vlr_gmv_ultimos_30d=114093, qtd_pedido_ultimos_30d=58,
            status_loja="LOJA ATIVA", cidade="SÃO PAULO", estado="SP",
            tipo_plano="PROFISSIONAL", vlr_plano_mrr_atual=199),
    }
    return demos.get(loja_id, demos[421])

def demo_onb(loja_id):
    return dict(flag_wizard_1=1, flag_wizard_2=1, flag_wizard_3=0,
        produtos=3, visitas=48, pedidos_cap=0, pedidos_apr=0,
        gmv_cap=0, gmv_apr=0, primeira_venda_origem_cap=None,
        data_primeira_venda_apr=None)

def demo_env(loja_id):
    return dict(flag_ativacao_enviali=0, flag_ativacao_pac=0, flag_ativacao_sedex=0,
        flag_ativacao_jadlog=0, flag_ativacao_zum_loggi=0,
        etiquetas_compradas_enviali=0, etiquetas_postadas_enviali=0,
        etiquetas_canceladas_enviali=0, gmv=0, pedidos=0)


# ── HELPERS ───────────────────────────────────────────────────────────────────

def sim_nao(val):
    v = str(val) if val is not None else "0"
    ok = v not in ("0", "None", "False", "", "nan")
    if ok: return '<span class="tag-sim">✓ Sim</span>'
    return '<span class="tag-nao">✗ Não</span>'

def tag_val(val, prefix="", suffix=""):
    v = str(val) if val is not None else "—"
    if v in ("None", "nan", "0", "0.0", ""): v = "—"
    if v != "—" and prefix: v = prefix + v
    if v != "—" and suffix: v = v + suffix
    return f'<span class="tag-info">{v}</span>'

def linha(label, valor_html):
    return (f'<div class="torre-item">'
            f'<span class="torre-label">{label}</span>'
            f'<span>{valor_html}</span></div>')

def fmt_brl(v):
    try:
        return f"R${float(v or 0):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$0"

def dias_desde(data_str):
    if not data_str or str(data_str) in ("None", "nan", "NaT", ""): return None
    try:
        d = datetime.strptime(str(data_str)[:10], "%Y-%m-%d").date()
        return (_date_type.today() - d).days
    except Exception:
        return None

def gerar_sparkline(valores: list, largura=80, altura=28) -> str:
    if not valores or len(valores) < 2:
        return "<span style='color:#ccc;font-size:11px'>—</span>"
    try:
        vmin, vmax = min(valores), max(valores)
        if vmax == vmin: vmax = vmin + 1
        def _x(i): return int(i / (len(valores)-1) * largura)
        def _y(v): return int(altura - (v - vmin) / (vmax - vmin) * (altura - 4) - 2)
        pontos = " ".join(f"{_x(i)},{_y(v)}" for i, v in enumerate(valores))
        cor    = "#E24B4A" if valores[-1] < valores[0] else "#1ABCB0"
        seta   = "▼" if valores[-1] < valores[-2] else "▲"
        cor_s  = "#E24B4A" if valores[-1] < valores[-2] else "#1ABCB0"
        return (f'<svg width="{largura}" height="{altura}" xmlns="http://www.w3.org/2000/svg">'
                f'<polyline points="{pontos}" fill="none" stroke="{cor}" stroke-width="2" stroke-linejoin="round"/>'
                f'<circle cx="{_x(len(valores)-1)}" cy="{_y(valores[-1])}" r="3" fill="{cor}"/>'
                f'</svg><span style="color:{cor_s};font-size:10px;margin-left:2px">{seta}</span>')
    except Exception:
        return "<span style='color:#ccc;font-size:11px'>—</span>"

def gerar_excel(loja, diag):
    output = io.BytesIO()
    _acoes = diag.get("acoes", [])
    dados = {
        "ID":          loja.get("loja_id", "—"),
        "Nome":        loja.get("nome_loja", "—"),
        "Status":      loja.get("status_loja", "—"),
        "Score":       diag.get("score_risco", "—"),
        "Prioridade":  diag.get("prioridade", "—"),
        "Causa raiz":  diag.get("causa_raiz", "—"),
        "Acao":        _acoes[0] if _acoes else "—",
        "Data":        datetime.now().strftime("%d/%m/%Y %H:%M"),
    }
    df_excel = pd.DataFrame(list(dados.items()), columns=["Campo", "Valor"])
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_excel.to_excel(writer, sheet_name="Diagnostico", index=False)
    return output.getvalue()


# ── DIAGNÓSTICO INLINE (fallback sem engine externo) ─────────────────────────

BENCHMARK_INLINE = {
    "MODA E ACESSÓRIOS":  {"avg_dias_venda": 24.9, "taxa_conversao": 1.1},
    "ELETRÔNICOS":        {"avg_dias_venda": 25.2, "taxa_conversao": 1.1},
    "ALIMENTOS E BEBIDAS":{"avg_dias_venda": 23.3, "taxa_conversao": 1.2},
    "FITNESS E SUPLEMENTOS": {"avg_dias_venda": 12.8, "taxa_conversao": 1.8},
    "CASA E DECORAÇÃO":   {"avg_dias_venda": 31.5, "taxa_conversao": 1.2},
    "INFORMÁTICA":        {"avg_dias_venda": 16.3, "taxa_conversao": 1.7},
    "DEFAULT":            {"avg_dias_venda": 23.0, "taxa_conversao": 1.2},
}

def _diagnostico_inline(loja, tend_semanal=None):
    status = str(loja.get("status_loja", "")).upper().strip()
    seg    = str(loja.get("segmento_loja", "")).upper().strip()
    bench  = BENCHMARK_INLINE.get(seg, BENCHMARK_INLINE["DEFAULT"])
    origem = "PAGO" if loja.get("aquisicao_utm_source") else "ORGÂNICO"
    is_pago = str(loja.get("status_plano","")).upper() == "PAGO"

    def _dias(d):
        if not d or str(d) in ("None","nan",""): return 0
        try:
            return (_date_type.today() - datetime.strptime(str(d)[:10], "%Y-%m-%d").date()).days
        except: return 0

    dias = _dias(loja.get("data_cadastro_loja"))
    avg  = bench["avg_dias_venda"] or 23
    ratio = round(dias / avg, 1) if avg > 0 and dias > 0 else 0.0

    tem_prod = loja.get("data_primeira_config_produto") not in (None,"","None")
    tem_pag  = loja.get("data_primeira_config_pagamento") not in (None,"","None")
    tem_log  = loja.get("data_primeira_config_logistica") not in (None,"","None")
    visitas  = int(loja.get("qtde_visitas_ultimos_30d") or 0)
    gmv      = float(loja.get("vlr_gmv_ultimos_30d") or 0)

    score = 0; causas = []; insights = []; acoes = []

    # ── Detecta queda em loja ativa (o bug da Arco Íris) ─────────────────────
    var_gmv_sem = None
    if tend_semanal and tend_semanal.get("gmv_anterior"):
        try:
            var_gmv_sem = float(tend_semanal.get("var_gmv_pct", 0)) / 100
        except Exception:
            pass

    if status == "LOJA ATIVA" and var_gmv_sem is not None:
        queda = abs(var_gmv_sem) if var_gmv_sem < 0 else 0
        if queda >= 0.50:
            score = 85; status = "QUEDA CRÍTICA"
            causas.append(f"Queda de {queda*100:.0f}% no GMV nas últimas 2 semanas vs mesmo período 30 dias atrás")
            insights.append(f"Colapso severo — GMV caiu {queda*100:.0f}%. Investigar churn de clientes B2B, remoção de forma de pagamento e campanhas de desconto excessivo.")
            acoes.append("Raio X imediato: abrir página 3_Raio_X para diagnóstico completo de causa raiz")
        elif queda >= 0.30:
            score = 70; status = "QUEDA CRÍTICA"
            causas.append(f"Queda de {queda*100:.0f}% no GMV — declínio acelerado")
            insights.append(f"GMV caiu {queda*100:.0f}% vs período de referência. Verificar mix de pagamento, churn de recorrentes e ticket médio.")
            acoes.append("Diagnóstico de queda urgente — usar queries de análise de causa raiz")
        elif queda >= 0.20:
            score = 55; status = "QUEDA EM RISCO"
            causas.append(f"Queda de {queda*100:.0f}% no GMV — tendência preocupante")
            insights.append(f"Declínio de {queda*100:.0f}% detectado. Monitorar próximas 2 semanas.")
            acoes.append("Acompanhar evolução semanal — acionar CS se persistir")
        elif queda >= 0.10:
            score = 35; status = "QUEDA ATENÇÃO"
            causas.append(f"Queda leve de {queda*100:.0f}% no GMV")
            insights.append("Variação dentro do esperado mas com tendência negativa.")
            acoes.append("Monitorar")
        else:
            score = 5
            causas.append("Loja ativa e saudável")
            acoes.append("Monitorar normalmente")

        if is_pago and score >= 35:
            score = min(score + 10, 100)
            insights.append("Loja paga — impacto em MRR justifica priorização.")

    elif status == "ONBOARDING INCOMPLETO":
        score = 70 if dias >= 7 else 50
        if tem_prod and not tem_pag:
            causas.append("Produto cadastrado mas pagamento NÃO configurado")
            insights.append("Gargalo crítico — sem pagamento nenhum pedido pode ser finalizado.")
            acoes.append("Ativar Pagali — 5 minutos para destravar vendas")
        elif tem_prod and tem_pag and not tem_log:
            causas.append("Pagamento ativo mas frete NÃO configurado")
            insights.append("Checkout trava na etapa de entrega. Ativar Enviali resolve.")
            acoes.append("Configurar Enviali — último passo para a primeira venda")
        else:
            causas.append("Configurações básicas incompletas")
            acoes.append("Completar checklist de configuração da loja")
        if is_pago:
            score += 10
            insights.append("Loja paga — custo de aquisição em risco.")

    elif status == "NUNCA VENDEU":
        score = 40 if ratio >= 2 else 25
        causas.append(f"{'Há ' + str(dias) + ' dias sem vender — ' + str(ratio) + 'x acima da média de ' + seg if ratio >= 1 else 'Dentro da janela esperada (' + str(dias) + '/' + str(int(avg)) + ' dias)'}")
        if visitas >= 50:
            score += 15
            insights.append(f"{visitas} visitas mas zero vendas — problema de conversão (fotos, preço ou descrição).")
            acoes.append("E-mail com dicas de conversão")
        elif visitas == 0:
            score += 10
            insights.append("Zero visitas — loja invisível. Divulgação é o próximo passo.")
            acoes.append("E-mail com checklist de divulgação")
        else:
            acoes.append("E-mail com estratégias para primeira venda")

    elif status == "SEM VENDAS RECENTES":
        score = 55
        causas.append("Loja que vendia entrou em inatividade")
        if gmv == 0 and visitas == 0:
            score = 70
            insights.append("Zero pedidos e zero visitas nos últimos 30 dias — parada total.")
            acoes.append("CS verificar urgente: loja acessível e produtos ativos")
        else:
            insights.append("Visitas existem mas sem conversão. Verificar estoque e preços.")
            acoes.append("E-mail de reativação com dicas de conversão")

    else:
        score = 5; causas.append("Loja ativa e saudável")
        insights.append(f"{visitas} visitas e {fmt_brl(gmv)} GMV nos últimos 30 dias.")
        acoes.append("Monitorar normalmente")

    score = max(0, min(100, score))
    if score >= 70:   prio = "🔴 CRÍTICA"; sla = "Intervir hoje";    canal = "E-mail imediato + alerta CS"
    elif score >= 45: prio = "🟠 ALTA";    sla = "Intervir em 24h";  canal = "E-mail automático"
    elif score >= 25: prio = "🟡 MÉDIA";   sla = "Intervir em 48h";  canal = "E-mail automático"
    else:             prio = "🟢 BAIXA";   sla = "Monitorar";        canal = "Sem ação necessária"

    # Email
    if status in ("ONBOARDING INCOMPLETO", "QUEDA CRÍTICA", "QUEDA EM RISCO", "SEM VENDAS RECENTES", "NUNCA VENDEU"):
        assunto = {"ONBOARDING INCOMPLETO": "Sua loja está quase pronta — falta pouco para a primeira venda",
                   "NUNCA VENDEU": "Sua loja está configurada — veja como atrair os primeiros compradores",
                   "SEM VENDAS RECENTES": "Sua loja ficou um tempo sem vendas — veja como reativar",
                   "QUEDA CRÍTICA": "Identificamos uma queda no seu faturamento — vamos resolver juntos?",
                   "QUEDA EM RISCO": "Sua loja está com tendência de queda — veja o que fazer"}.get(status, "Acompanhamento da sua loja")
        itens = []
        if not tem_prod: itens.append("→ Cadastrar pelo menos 1 produto")
        if not tem_pag:  itens.append("→ Configurar pagamento (Pagali)")
        if not tem_log:  itens.append("→ Configurar frete (Enviali)")
        corpo = f"Olá, {loja.get('nome_loja','Lojista')}!\n\n{acoes[0] if acoes else '→ Entre em contato com nosso time.'}\n\n— Time Loja Integrada"
        email_diag = {"assunto": assunto, "corpo": corpo, "metrica_impacto": "Acompanhar em 7 dias"}
    else:
        email_diag = None

    return {
        "score_risco": score, "prioridade": prio, "sla": sla, "canal": canal,
        "causa_raiz": " | ".join(causas) if causas else status,
        "insights": insights, "acoes": acoes if acoes else ["Monitorar normalmente"],
        "benchmark": bench, "dias_cadastro": dias,
        "status_calculado": status,
        "var_gmv_semanal": var_gmv_sem,
        "email": email_diag,
    }


# ════════════════════════════════════════════════════════════════════════════════
# HEADER PRINCIPAL
# ════════════════════════════════════════════════════════════════════════════════

modo_real = _ok()
st.markdown(f"""
<div class="liwatch-header">
    <div>
        <div class="liwatch-logo">👁️ LI Watch · Raio X</div>
        <div class="liwatch-sub">Diagnóstico de lojistas · Loja Integrada</div>
    </div>
    <div class="liwatch-badge">{'● Dados reais' if modo_real else '● Demo — dados simulados'}</div>
</div>
""", unsafe_allow_html=True)

col_b, col_btn, col_help = st.columns([4.5, 1, 0.7])
with col_b:
    termo = st.text_input("", placeholder="🔍  ID da loja ou nome — Ex: 233932 ou Arco Íris LED",
        label_visibility="collapsed")
with col_btn:
    st.button("Buscar", use_container_width=True)
with col_help:
    with st.popover("?"):
        st.markdown("**Como usar o LI Watch · Raio X**")
        st.markdown("**Buscar:** ID numérico ou nome parcial da loja.")
        st.markdown("**Score:** 70+ crítico (hoje), 45 alto (24h), 25 médio (48h), abaixo baixo.")
        st.markdown("**NOVO v3:** Lojas ativas em queda de 20%+ agora aparecem com score real — não mais como saudáveis.")
        st.markdown("**Raio X completo:** Use a página 3_Raio_X para diagnóstico com linha do tempo, mix de pagamento e clientes churned.")


# ════════════════════════════════════════════════════════════════════════════════
# TELA INICIAL — painel quando sem busca
# ════════════════════════════════════════════════════════════════════════════════

if not termo.strip():
    if not modo_real:
        st.markdown(
            "<div style='text-align:center;padding:3rem 0;color:#5A7A78'>"
            "<div style='font-size:18px;font-weight:600;color:#1A2E2B'>LI Watch · Raio X</div>"
            "<div style='font-size:13px;margin-top:8px;color:#9DBDBB'>"
            "Conecte ao Metabase para monitorar a base · Digite um ID para diagnosticar"
            "</div></div>", unsafe_allow_html=True)
    else:
        import metabase_connector as mb
        import datetime as _dt
        import numpy as np

        # ── Carrega dados base ──────────────────────────────────────────────
        df_alert = pd.DataFrame()
        df_top   = pd.DataFrame()
        tendencias_map = {}

        try:
            with st.spinner("Carregando base de monitoramento..."):
                df_alert = buscar_base_monitoramento()
        except Exception as e:
            st.warning(f"Erro ao carregar base: {e}")

        try:
            with st.spinner("Analisando top sellers..."):
                df_top = buscar_top_lojas(100)
            if not df_top.empty and "var_projetado_pct" in df_top.columns:
                df_top["var_projetado_pct"] = pd.to_numeric(df_top["var_projetado_pct"], errors="coerce")
                tendencias_map = {
                    int(r["conta_id"]): float(r["var_projetado_pct"]) / 100
                    for _, r in df_top.iterrows()
                    if pd.notna(r.get("var_projetado_pct"))
                }
        except Exception as e:
            st.warning(f"Erro ao carregar top sellers: {e}")

        # ── Painel de alertas v2 (com alerta de queda em ativas) ───────────
        try:
            if ALERTAS_OK:
                alertas = detectar_alertas(df_alert if not df_alert.empty else pd.DataFrame(), tendencias_map)
            else:
                alertas = []

            if alertas:
                n_criticos = sum(1 for a in alertas if a["severidade"] == "CRÍTICO")
                n_atencao  = sum(1 for a in alertas if a["severidade"] == "ATENÇÃO")
                st.markdown(f"""
                <div style='background:#0D4F4A;border-radius:12px;padding:1rem 1.2rem;margin-bottom:1rem'>
                    <div style='display:flex;justify-content:space-between;align-items:center'>
                        <div>
                            <div style='font-size:13px;font-weight:700;color:#D4F53C;margin-bottom:2px'>
                                👁️ LI Watch — {len(alertas)} alerta(s) detectado(s) agora
                            </div>
                            <div style='font-size:12px;color:#9DCFCC'>
                                {n_criticos} crítico(s) · {n_atencao} atenção · Atualizado agora
                            </div>
                        </div>
                        <div style='font-size:24px'>{"🔴" if n_criticos > 0 else "🟡"}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                for alerta in alertas:
                    col_alert, col_acao = st.columns([4, 1])
                    with col_alert:
                        st.markdown(f"""
                        <div style='background:{alerta["cor_bg"]};border:1px solid {alerta["cor_borda"]};
                                    border-left:4px solid {alerta["cor_borda"]};border-radius:8px;
                                    padding:.6rem 1rem;margin-bottom:5px'>
                            <div style='font-size:13px;font-weight:600;color:{alerta["cor_texto"]}'>
                                {alerta["emoji"]} {alerta["titulo"]}
                            </div>
                            <div style='font-size:12px;color:#555;margin-top:2px'>{alerta["descricao"]}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    with col_acao:
                        st.markdown(f"""
                        <div style='background:white;border:1px solid {alerta["cor_borda"]};border-radius:8px;
                                    padding:.6rem .8rem;margin-bottom:5px;font-size:11px;
                                    color:{alerta["cor_texto"]};font-weight:600;text-align:center'>
                            → {alerta["acao"]}
                        </div>
                        """, unsafe_allow_html=True)
                st.divider()
            else:
                st.markdown("""
                <div style='background:#F0FDF4;border:1px solid #86EFAC;border-radius:10px;
                            padding:.7rem 1.2rem;margin-bottom:1rem;font-size:13px;color:#166534'>
                    ✅ <strong>Sem alertas críticos agora</strong> — todas as lojas monitoradas estão dentro do esperado.
                </div>
                """, unsafe_allow_html=True)
        except Exception as e:
            st.warning(f"Alertas indisponíveis: {e}")

        # ── Abas principais ─────────────────────────────────────────────────
        tab_onb, tab_churn = st.tabs(["🚀 Onboarding — Lojas novas", "🔥 Top Sellers em risco"])

        # ══════════════════════════════════════════════════════════════════════
        # TAB 1 — ONBOARDING
        # ══════════════════════════════════════════════════════════════════════
        with tab_onb:
            st.caption("Lojas criadas nos últimos 60 dias · ordenadas por urgência · foco em travamentos críticos")
            try:
                if df_alert.empty:
                    df_onb = buscar_base_monitoramento()
                else:
                    df_onb = df_alert.copy()

                if not df_onb.empty:
                    # Score via score_engine ou fallback vetorizado
                    if SCORE_ENGINE_OK:
                        df_onb = calcular_scores_df(df_onb)
                    else:
                        df_onb["dias_cadastro"] = pd.to_numeric(df_onb.get("dias_cadastro", 0), errors="coerce").fillna(0).astype(int)
                        df_onb["status_loja"]   = df_onb["status_loja"].fillna("").astype(str)
                        df_onb["status_plano"]  = df_onb["status_plano"].fillna("").astype(str)
                        _s = df_onb["status_loja"]
                        _d = df_onb["dias_cadastro"]
                        _p = df_onb["status_plano"].str.upper()
                        _sc = np.where(_s=="ONBOARDING INCOMPLETO", np.where(_d>=7,70,50),
                               np.where(_s=="NUNCA VENDEU", np.where(_d>=20,45,25),
                               np.where(_s=="SEM VENDAS RECENTES", 55, 5)))
                        df_onb["score"] = np.where(_p=="PAGO", np.minimum(_sc+10,100), _sc).astype(int)

                        def _gargalo(row):
                            if row["status_loja"] == "LOJA ATIVA": return "✅ Ativa"
                            if str(row.get("data_primeira_config_produto","")) in ("","None","nan","NaT"): return "🔴 Sem produto"
                            if str(row.get("data_primeira_config_pagamento","")) in ("","None","nan","NaT"): return "🔴 Sem pagamento"
                            if str(row.get("data_primeira_config_logistica","")) in ("","None","nan","NaT"): return "🟡 Sem frete"
                            if str(row.get("data_primeira_venda","")) in ("","None","nan","NaT"): return "🟠 Nunca vendeu"
                            return "✅ Configurada"
                        def _janela(row):
                            d = int(row.get("dias_cadastro") or 0)
                            if d >= 15: return "⚠️ Janela vencida"
                            if d >= 7:  return "🕐 Janela crítica"
                            return "🟢 Janela aberta"
                        df_onb["gargalo"] = df_onb.apply(_gargalo, axis=1)
                        df_onb["janela"]  = df_onb.apply(_janela, axis=1)

                    df_onb = df_onb.sort_values("score", ascending=False)

                    n_total        = len(df_onb[df_onb["status_loja"] != "LOJA ATIVA"])
                    n_critico      = len(df_onb[df_onb["score"] >= 70])
                    n_pago_travado = len(df_onb[(df_onb["status_plano"].str.upper()=="PAGO") & (df_onb["status_loja"]!="LOJA ATIVA")])
                    n_janela       = len(df_onb[(df_onb["dias_cadastro"] >= 7) & (df_onb["status_loja"]!="LOJA ATIVA")])

                    c1,c2,c3,c4 = st.columns(4)
                    c1.metric("Lojas em onboarding", n_total)
                    c2.metric("🔴 Score crítico (70+)", n_critico)
                    c3.metric("💸 Pagos travados", n_pago_travado)
                    c4.metric("⏰ Passaram janela (7d+)", n_janela)
                    st.divider()

                    # Mês de entrada
                    df_onb["mes_entrada"] = pd.to_datetime(df_onb["data_cadastro_loja"], errors="coerce").dt.strftime("%Y-%m").fillna("—")
                    meses = sorted([m for m in df_onb["mes_entrada"].unique() if m != "—"], reverse=True)
                    _nomes_mes = {"01":"Janeiro","02":"Fevereiro","03":"Março","04":"Abril",
                                  "05":"Maio","06":"Junho","07":"Julho","08":"Agosto",
                                  "09":"Setembro","10":"Outubro","11":"Novembro","12":"Dezembro"}
                    def _fmt_mes(m):
                        try: a,n = m.split("-"); return f"{_nomes_mes.get(n,n)}/{a}"
                        except: return m
                    meses_labels = {m: _fmt_mes(m) for m in meses}
                    _lbl_val = {v: k for k,v in meses_labels.items()}
                    opcoes_mes = ["Todos"] + [meses_labels[m] for m in meses]

                    cf1,cf2,cf3,cf4 = st.columns(4)
                    with cf1: f_mes     = st.selectbox("Mês de entrada", opcoes_mes, key="f_mes_onb")
                    with cf2: f_gargalo = st.selectbox("Gargalo", ["Todos","🔴 Sem produto","🔴 Sem pagamento","🟡 Sem frete","🟠 Nunca vendeu"], key="f_gargalo")
                    with cf3: f_plano   = st.selectbox("Plano", ["Todos","PAGO","GRATIS"], key="f_plano_onb")
                    with cf4: f_janela  = st.selectbox("Janela", ["Todos","🟢 Janela aberta","🕐 Janela crítica","⚠️ Janela vencida"], key="f_janela")

                    df_v = df_onb[df_onb["status_loja"] != "LOJA ATIVA"].copy()
                    if f_mes     != "Todos": df_v = df_v[df_v["mes_entrada"] == _lbl_val.get(f_mes, f_mes)]
                    if f_gargalo != "Todos": df_v = df_v[df_v["gargalo"] == f_gargalo]
                    if f_plano   != "Todos": df_v = df_v[df_v["status_plano"].str.upper() == f_plano]
                    if f_janela  != "Todos": df_v = df_v[df_v["janela"] == f_janela]

                    # Ação inline por loja (novo v3)
                    def _acao_inline(row):
                        g = row.get("gargalo","")
                        if "produto"    in g.lower(): return "📦 Ligar: cadastrar produto"
                        if "pagamento"  in g.lower(): return "💳 Ligar: ativar Pagali"
                        if "frete"      in g.lower(): return "📬 E-mail: configurar Enviali"
                        if "nunca"      in g.lower(): return "📢 E-mail: guia de divulgação"
                        return "👁️ Monitorar"
                    df_v["acao_cs"] = df_v.apply(_acao_inline, axis=1)

                    st.caption(f"{len(df_v)} loja(s) · ação inline por loja")
                    _cols = [c for c in ["loja_id","nome_loja","segmento_loja","status_plano","score","dias_cadastro","gargalo","janela","acao_cs"] if c in df_v.columns]
                    st.dataframe(
                        df_v[_cols].rename(columns={
                            "loja_id":"ID","nome_loja":"Loja","segmento_loja":"Segmento",
                            "status_plano":"Plano","score":"Score","dias_cadastro":"Dias",
                            "gargalo":"Gargalo","janela":"Janela","acao_cs":"Ação CS",
                        }),
                        use_container_width=True, hide_index=True,
                        column_config={"Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d")},
                    )
                    st.download_button("Exportar CSV",
                        data=df_v[_cols].to_csv(index=False).encode("utf-8"),
                        file_name=f"onboarding_{_dt.date.today().strftime('%Y%m%d')}.csv",
                        mime="text/csv")

                    # Cohort
                    st.divider()
                    st.markdown("#### 📊 Evolução por cohort — % que já vendeu por mês de entrada")
                    df_c = df_onb[df_onb["status_loja"] != "LOJA ATIVA"].copy()
                    if not df_c.empty and "mes_entrada" in df_c.columns:
                        cohort = df_c.groupby("mes_entrada").agg(
                            total=("loja_id","count"),
                            venderam=("data_primeira_venda", lambda x: x.notna().sum()),
                            pagos=("status_plano", lambda x: (x.str.upper()=="PAGO").sum()),
                            criticos=("score", lambda x: (pd.to_numeric(x,errors="coerce")>=70).sum()),
                        ).reset_index().sort_values("mes_entrada", ascending=False)
                        cohort["% vendeu"]   = (cohort["venderam"] / cohort["total"] * 100).round(1)
                        cohort["% pagos"]    = (cohort["pagos"]    / cohort["total"] * 100).round(1)
                        cohort["% críticos"] = (cohort["criticos"] / cohort["total"] * 100).round(1)
                        cohort["Mês"]        = cohort["mes_entrada"].apply(_fmt_mes)

                        cols_c = st.columns(min(len(cohort), 3))
                        for i, (_, rc) in enumerate(cohort.iterrows()):
                            with cols_c[i % 3]:
                                pv  = float(rc["% vendeu"])
                                cor = "#1ABCB0" if pv >= 30 else "#F59E0B" if pv >= 10 else "#E24B4A"
                                st.markdown(
                                    f"<div style='background:white;border:1px solid #e0ddd6;border-radius:10px;"
                                    f"padding:.8rem 1rem;margin-bottom:8px'>"
                                    f"<div style='font-size:13px;font-weight:700;color:#1A2E2B'>{rc['Mês']}</div>"
                                    f"<div style='font-size:11px;color:#888;margin:.3rem 0'>{int(rc['total'])} lojas</div>"
                                    f"<div style='display:flex;gap:8px;margin-top:.5rem'>"
                                    f"<div style='flex:1;background:#F0FDF4;border-radius:6px;padding:.4rem;text-align:center'>"
                                    f"<div style='font-size:18px;font-weight:800;color:{cor}'>{pv:.0f}%</div>"
                                    f"<div style='font-size:10px;color:#888'>já vendeu</div></div>"
                                    f"<div style='flex:1;background:#FEF2F2;border-radius:6px;padding:.4rem;text-align:center'>"
                                    f"<div style='font-size:18px;font-weight:800;color:#E24B4A'>{rc['% críticos']:.0f}%</div>"
                                    f"<div style='font-size:10px;color:#888'>críticos</div></div>"
                                    f"<div style='flex:1;background:#EEEDFE;border-radius:6px;padding:.4rem;text-align:center'>"
                                    f"<div style='font-size:18px;font-weight:800;color:#6366F1'>{rc['% pagos']:.0f}%</div>"
                                    f"<div style='font-size:10px;color:#888'>pagos</div></div>"
                                    f"</div></div>", unsafe_allow_html=True)

            except Exception as e:
                st.warning(f"Erro ao carregar onboarding: {e}")

        # ══════════════════════════════════════════════════════════════════════
        # TAB 2 — TOP SELLERS EM RISCO (v3: ação N2 inline + tendência semanal)
        # ══════════════════════════════════════════════════════════════════════
        with tab_churn:
            st.caption("Top 100 lojas por GMV — comparando projeção do mês atual vs média 2 meses anteriores")
            try:
                if df_top.empty:
                    st.info("Dados de top sellers não disponíveis.")
                    st.stop()

                df_top["var_projetado_pct"] = pd.to_numeric(df_top["var_projetado_pct"], errors="coerce")
                df_risco = df_top[df_top["var_projetado_pct"] <= -20].copy()
                df_risco["gmv_em_risco"] = (
                    pd.to_numeric(df_risco["vlr_gmv_media_2m"], errors="coerce") -
                    pd.to_numeric(df_risco["vlr_gmv_projetado"], errors="coerce")
                ).clip(lower=0).round(2)

                n_risco   = len(df_risco)
                dia_atual = _dt.date.today().day
                n_critico = len(df_top[df_top["var_projetado_pct"] <= -50])
                n_atencao = len(df_top[(df_top["var_projetado_pct"] > -50) & (df_top["var_projetado_pct"] <= -20)])
                n_ok      = len(df_top[df_top["var_projetado_pct"] > -20])
                gmv_total_risco = max(0,
                    pd.to_numeric(df_risco["vlr_gmv_media_2m"], errors="coerce").fillna(0).sum() -
                    pd.to_numeric(df_risco["vlr_gmv_projetado"], errors="coerce").fillna(0).sum()
                )

                st.markdown(f"""
                <div style='background:linear-gradient(135deg,#0D4F4A,#1A7A72);border-radius:12px;
                            padding:1rem 1.5rem;margin-bottom:1rem;display:flex;
                            justify-content:space-between;align-items:center'>
                    <div>
                        <div style='font-size:11px;color:#9DCFCC;text-transform:uppercase;letter-spacing:.1em'>
                            GMV em risco este mês
                        </div>
                        <div style='font-size:28px;font-weight:800;color:#D4F53C'>
                            {fmt_brl(gmv_total_risco)}
                        </div>
                        <div style='font-size:12px;color:#9DCFCC;margin-top:2px'>
                            em {n_risco} das top 100 lojas monitoradas
                        </div>
                    </div>
                    <div style='text-align:right'>
                        <div style='font-size:11px;color:#9DCFCC;margin-bottom:4px'>Distribuição de risco</div>
                        <div style='font-size:14px;color:white'>
                            🔴 {n_critico} crítico &nbsp;·&nbsp; 🟡 {n_atencao} risco &nbsp;·&nbsp; 🟢 {n_ok} OK
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Top sellers monitoradas", len(df_top))
                c2.metric("🔴 Crítico (>50% queda)", n_critico)
                c3.metric("🟡 Risco (20-50% queda)", n_atencao)
                c4.metric("🟢 OK", n_ok)
                st.divider()

                if n_risco > 0:
                    st.markdown(f"**{n_risco} loja(s) com queda de 20%+ — diagnóstico inline abaixo**")

                    ids_risco = df_risco["conta_id"].astype(int).tolist()
                    try:
                        historico_map = buscar_historico_mensal_batch_cached(tuple(ids_risco))
                    except Exception:
                        historico_map = {}

                    # v3: diagnóstico de queda batch inline
                    diag_queda_map = {}
                    if QUEDA_OK:
                        try:
                            with st.spinner("Executando diagnóstico de queda nas lojas em risco..."):
                                diag_queda_map = diagnosticar_queda_batch(df_risco.rename(columns={"conta_id":"loja_id"}), mb)
                        except Exception:
                            pass

                    rows_ui = []
                    sparks  = []
                    for _, r in df_risco.sort_values("var_projetado_pct").iterrows():
                        lid  = int(r["conta_id"])
                        hist = historico_map.get(lid, [])
                        spark = gerar_sparkline(hist)
                        var_v = float(r["var_projetado_pct"])

                        # Causa raiz inline
                        causa_inline = "—"
                        acao_inline  = "Diagnóstico manual"
                        if lid in diag_queda_map:
                            dq = diag_queda_map[lid]
                            causa_inline = dq.get("causa_raiz","—")[:55]
                            acoes_dq = dq.get("causas",[])
                            if acoes_dq:
                                acao_inline = acoes_dq[0].get("tipo","Investigar")

                        rows_ui.append({
                            "ID":                         lid,
                            "Loja":                       r["nome_loja"],
                            "Segmento":                   r.get("segmento","—"),
                            f"Média 2m (d1-{dia_atual})": fmt_brl(r["vlr_gmv_media_2m"]),
                            f"Atual (d1-{dia_atual})":    fmt_brl(r["vlr_gmv_mes_atual"]),
                            "Projetado":                  fmt_brl(r["vlr_gmv_projetado"]),
                            "Variação":                   f"{var_v:.0f}%",
                            "GMV em risco":               fmt_brl(r["gmv_em_risco"]),
                            "Causa raiz":                 causa_inline,
                            "Ação N2":                    acao_inline,
                        })
                        sparks.append(spark)

                    df_ui = pd.DataFrame(rows_ui)
                    st.dataframe(df_ui, use_container_width=True, hide_index=True)

                    # Sparklines
                    if any(historico_map.values()):
                        st.markdown("**Tendência GMV — últimos 6 meses:**")
                        n_cols = 4
                        cols_sp = st.columns(n_cols)
                        for i, (row, spark) in enumerate(zip(rows_ui, sparks)):
                            with cols_sp[i % n_cols]:
                                var_val = float(df_risco.sort_values("var_projetado_pct").iloc[i]["var_projetado_pct"])
                                cor_v   = "#E24B4A" if var_val <= -50 else "#F59E0B"
                                st.markdown(
                                    f"<div style='background:white;border:1px solid #e0ddd6;border-radius:8px;"
                                    f"padding:.6rem .8rem;margin-bottom:8px;text-align:center'>"
                                    f"<div style='font-size:11px;font-weight:600;color:#1A2E2B;margin-bottom:4px'>{str(row['Loja'])[:22]}</div>"
                                    f"{spark}"
                                    f"<div style='font-size:12px;color:{cor_v};font-weight:700;margin-top:4px'>{row['Variação']}</div>"
                                    f"<div style='font-size:10px;color:#888'>{row['GMV em risco']} em risco</div>"
                                    f"</div>", unsafe_allow_html=True)

                    st.download_button("Exportar CSV",
                        data=df_ui.to_csv(index=False).encode("utf-8"),
                        file_name=f"top_sellers_risco_{_dt.date.today().strftime('%Y%m%d')}.csv",
                        mime="text/csv")
                else:
                    st.markdown(
                        "<div style='background:#F0FDF4;border:1px solid #86EFAC;border-radius:8px;"
                        "padding:.8rem 1rem;font-size:13px;color:#166534'>"
                        "✅ Nenhum top seller com queda de 20%+ hoje.</div>", unsafe_allow_html=True)

            except Exception as e:
                st.warning(f"Erro ao carregar top sellers: {e}")

    st.stop()


# ════════════════════════════════════════════════════════════════════════════════
# BUSCA INDIVIDUAL
# ════════════════════════════════════════════════════════════════════════════════

termo_clean = termo.strip()
loja_id = None
loja = onb = env = None

if modo_real:
    try:
        if termo_clean.isdigit():
            loja_id = int(termo_clean)
        else:
            with st.spinner("Buscando loja..."):
                df_busca = buscar_por_nome(termo_clean)
            if df_busca.empty:
                st.markdown(f"""<div style='background:#FFFBEB;border:1px solid #FDE68A;
                    border-radius:10px;padding:1.2rem;text-align:center;color:#92400E'>
                    <strong>Nenhuma loja encontrada para "{termo_clean}"</strong>
                </div>""", unsafe_allow_html=True)
                st.stop()
            elif len(df_busca) == 1:
                loja_id = int(df_busca.iloc[0]["loja_id"])
            else:
                opcoes = {f"{r['loja_id']} — {r['nome_loja']} ({r['segmento_loja']})": int(r["loja_id"])
                          for _, r in df_busca.iterrows()}
                sel = st.selectbox(f"{len(df_busca)} lojas encontradas:", list(opcoes.keys()))
                loja_id = opcoes[sel]

        if loja_id:
            with st.spinner("Carregando dados..."):
                loja_df, onb_df, env_df = buscar_dados_loja(loja_id)
            if loja_df.empty:
                st.error(f"Loja {loja_id} não encontrada.")
                st.stop()
            loja = loja_df.iloc[0].to_dict()
            onb  = onb_df.iloc[0].to_dict() if not onb_df.empty else dict(
                flag_wizard_1=1, flag_wizard_2=1, flag_wizard_3=None,
                produtos=None, visitas=loja.get("qtde_visitas_ultimos_30d"),
                pedidos_cap=loja.get("qtd_pedido_ultimos_30d"),
                pedidos_apr=loja.get("qtd_pedido_ultimos_30d"),
                gmv_cap=loja.get("vlr_gmv_ultimos_30d"), gmv_apr=loja.get("vlr_gmv_ultimos_30d"),
                primeira_venda_origem_cap=None,
                data_primeira_venda_apr=loja.get("data_primeira_venda"),
            )
            env = env_df.iloc[0].to_dict() if not env_df.empty else dict(
                flag_ativacao_enviali=0, flag_ativacao_pac=0, flag_ativacao_sedex=0,
                flag_ativacao_jadlog=0, flag_ativacao_zum_loggi=0,
                etiquetas_compradas_enviali=None, etiquetas_postadas_enviali=None,
                etiquetas_canceladas_enviali=None,
                etiquetas_compradas_pac=None, etiquetas_compradas_sedex=None,
                etiquetas_compradas_jadlog=None,
                pedidos_cotados_enviali=None, gmv=None, pedidos=None,
            )

    except Exception as e:
        st.warning(f"Erro na conexão: {e}. Usando dados simulados.")
        modo_real = False

if not modo_real or loja is None:
    loja_id = int(termo_clean) if termo_clean.isdigit() else 421
    loja = demo_loja(loja_id)
    onb  = demo_onb(loja_id)
    env  = demo_env(loja_id)


# ── BUSCA TENDÊNCIA SEMANAL (v3: usa para corrigir score de ativas) ───────────
tend_semanal = {}
if modo_real and loja_id and str(loja.get("status_loja","")) == "LOJA ATIVA":
    try:
        with st.spinner("Verificando tendência de GMV..."):
            tend_semanal = buscar_tendencia_semanal_cached(loja_id)
    except Exception:
        pass


# ── DIAGNÓSTICO ───────────────────────────────────────────────────────────────
if ENGINE_OK:
    try:
        diag = diagnosticar_loja(loja)
        if "score_risco" not in diag:
            diag = _diagnostico_inline(loja, tend_semanal)
        # v3: re-calcula com tendência se loja ativa
        elif str(loja.get("status_loja","")) == "LOJA ATIVA" and tend_semanal:
            diag_tend = _diagnostico_inline(loja, tend_semanal)
            if diag_tend["score_risco"] > diag["score_risco"]:
                diag = diag_tend
    except Exception:
        diag = _diagnostico_inline(loja, tend_semanal)
else:
    diag = _diagnostico_inline(loja, tend_semanal)

score      = diag["score_risco"]
prioridade = diag["prioridade"]
status     = diag.get("status_calculado", str(loja.get("status_loja","")).upper())
registrar_uso(loja.get("loja_id"), loja.get("nome_loja"), status, score)


# ── PERFIL DA LOJA (v3: inclui QUEDA CRÍTICA) ─────────────────────────────────
def _dias_loja(d):
    if not d or str(d) in ("None","nan",""): return 999
    try: return (_date_type.today() - datetime.strptime(str(d)[:10],"%Y-%m-%d").date()).days
    except: return 999

dias_desde_cadastro = _dias_loja(loja.get("data_cadastro_loja"))
gmv_30d_check       = float(loja.get("vlr_gmv_ultimos_30d") or 0)
pedidos_30d_check   = int(loja.get("qtd_pedido_ultimos_30d") or 0)
var_gmv_sem         = diag.get("var_gmv_semanal")

# v3: detecta queda crítica antes de classificar como saudável
if status == "QUEDA CRÍTICA" or (status == "LOJA ATIVA" and var_gmv_sem is not None and var_gmv_sem <= -0.30):
    perfil_loja  = "QUEDA_CRITICA"
    perfil_label = "⚠️ Queda crítica detectada"
    perfil_desc  = f"Loja ativa mas com queda de {abs(var_gmv_sem or 0)*100:.0f}% no GMV — risco de churn de clientes B2B"
    perfil_cor   = "#E24B4A"
    perfil_bg    = "#FEF2F2"
elif status == "QUEDA EM RISCO" or (status == "LOJA ATIVA" and var_gmv_sem is not None and var_gmv_sem <= -0.20):
    perfil_loja  = "QUEDA_RISCO"
    perfil_label = "📉 Tendência de queda"
    perfil_desc  = f"Queda de {abs(var_gmv_sem or 0)*100:.0f}% no GMV semanal — monitorar de perto"
    perfil_cor   = "#F59E0B"
    perfil_bg    = "#FFFBEB"
elif dias_desde_cadastro < 90:
    perfil_loja  = "NOVO"
    perfil_label = "Novo lojista"
    perfil_desc  = f"Cadastrado há {dias_desde_cadastro} dias — foco em ativação"
    perfil_cor   = "#1ABCB0"
    perfil_bg    = "#D1FAF6"
elif gmv_30d_check > 0 or pedidos_30d_check > 0:
    perfil_loja  = "CASA_ATIVO"
    perfil_label = "Cliente da casa — ativo"
    perfil_desc  = f"{pedidos_30d_check} pedidos e {fmt_brl(gmv_30d_check)} GMV nos últimos 30 dias"
    perfil_cor   = "#22C55E"
    perfil_bg    = "#F0FDF4"
elif status == "SEM VENDAS RECENTES":
    perfil_loja  = "CASA_RISCO"
    perfil_label = "Cliente da casa — em risco"
    perfil_desc  = f"Loja estabelecida ({dias_desde_cadastro} dias) com queda de faturamento"
    perfil_cor   = "#F59E0B"
    perfil_bg    = "#FFFBEB"
else:
    perfil_loja  = "CASA_INATIVO"
    perfil_label = "Cliente da casa — inativo"
    perfil_desc  = f"Loja com {dias_desde_cadastro} dias sem vendas recentes"
    perfil_cor   = "#E24B4A"
    perfil_bg    = "#FEF2F2"

# Cores do score
if score >= 70:   cor="#E24B4A"; bg="#FEF2F2"; bd="#FCA5A5"
elif score >= 45: cor="#F59E0B"; bg="#FFFBEB"; bd="#FDE68A"
elif score >= 25: cor="#1ABCB0"; bg="#D1FAF6"; bd="#99F6E4"
else:             cor="#22C55E"; bg="#F0FDF4"; bd="#86EFAC"


# ── ALERTA NO TOPO ────────────────────────────────────────────────────────────
if score >= 70:
    st.markdown(f'<div class="alerta-critico">🔴 <strong>CRÍTICO — {prioridade}</strong> &nbsp;·&nbsp; {diag["sla"]} &nbsp;·&nbsp; {diag["canal"]}</div>', unsafe_allow_html=True)
elif score >= 45:
    st.markdown(f'<div class="alerta-atencao">🟠 <strong>ATENÇÃO — {prioridade}</strong> &nbsp;·&nbsp; {diag["sla"]}</div>', unsafe_allow_html=True)
elif status == "LOJA ATIVA" and perfil_loja == "CASA_ATIVO":
    st.markdown('<div class="alerta-ok">✅ <strong>LOJA ATIVA</strong> — Sem intervenção necessária</div>', unsafe_allow_html=True)


# ── CABEÇALHO DA LOJA ─────────────────────────────────────────────────────────
col_nome, col_score = st.columns([4,1])

with col_nome:
    mrr     = loja.get("vlr_plano_mrr_atual") or 0
    _mrr_str = f"— R${mrr}/mês" if float(mrr or 0) > 0 else ""
    st.markdown(
        "<div style='background:white;border-radius:14px;padding:1.2rem;margin-bottom:1rem'>"
        "<div style='display:flex;align-items:center;gap:10px;margin-bottom:4px'>"
        f"<span style='font-size:20px;font-weight:700;color:#1A2E2B'>{loja.get('nome_loja','—')}</span>"
        f"<span style='background:{perfil_bg};color:{perfil_cor};font-size:11px;"
        f"font-weight:700;padding:3px 10px;border-radius:20px;white-space:nowrap'>{perfil_label}</span>"
        "</div>"
        f"<div style='font-size:12px;color:{perfil_cor};margin-bottom:6px'>{perfil_desc}</div>"
        "<div style='font-size:13px;color:#5A7A78'>"
        f"ID: <strong>{loja.get('loja_id','—')}</strong> &nbsp;·&nbsp; {loja.get('segmento_loja','—')}"
        f" &nbsp;·&nbsp; {loja.get('cidade','—')}/{loja.get('estado','—')}"
        f" &nbsp;·&nbsp; Plano: <strong>{loja.get('status_plano','—')} {_mrr_str}</strong>"
        f" &nbsp;·&nbsp; Origem: <strong>{loja.get('origem','—')}</strong>"
        "</div>"
        f"<div style='font-size:13px;color:#5A7A78;margin-top:2px'>"
        f"{loja.get('email_loja','—')} &nbsp;·&nbsp; {loja.get('dominio_loja','—')}"
        "</div></div>",
        unsafe_allow_html=True)

with col_score:
    st.markdown(f"""
    <div style='background:{bg};border:2px solid {bd};border-radius:14px;
                padding:1rem;text-align:center;margin-bottom:1rem'>
        <div style='font-size:11px;color:#5A7A78'>Score de risco</div>
        <div class="score-num" style='color:{cor}'>{score}</div>
        <div style='font-size:11px;color:#5A7A78'>de 100</div>
        <div style='font-size:13px;font-weight:700;color:{cor};margin-top:4px'>{prioridade}</div>
    </div>""", unsafe_allow_html=True)


# ── BANNER DE QUEDA CRÍTICA (v3) ──────────────────────────────────────────────
if perfil_loja in ("QUEDA_CRITICA", "QUEDA_RISCO"):
    var_pct_str = f"{abs(var_gmv_sem or 0)*100:.0f}%"
    st.markdown(
        f"<div style='background:#0D4F4A;border-radius:12px;padding:1rem 1.2rem;margin-bottom:.8rem'>"
        f"<div style='font-size:11px;color:#1ABCB0;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px'>"
        f"⚠️ Alerta Raio X v3 — Queda detectada em loja ativa</div>"
        f"<div style='font-size:13px;color:#D1FAF6;line-height:1.6'>"
        f"Esta loja tem <strong style='color:#D4F53C'>GMV caindo {var_pct_str}</strong> nas últimas 2 semanas "
        f"vs mesmo período 30 dias atrás — mas aparecia como saudável no app anterior. "
        f"Para diagnóstico completo (causa raiz, clientes churned, mix de pagamento), "
        f"abra a <strong style='color:#D4F53C'>página 🔬 Raio X</strong> com o ID "
        f"<strong style='color:#D4F53C'>{loja.get('loja_id','—')}</strong>."
        f"</div></div>",
        unsafe_allow_html=True)


# ── TENDÊNCIA SEMANAL INLINE (v3) ─────────────────────────────────────────────
if tend_semanal and tend_semanal.get("gmv_anterior"):
    try:
        _gmv_at  = float(tend_semanal.get("gmv_atual", 0))
        _gmv_ant = float(tend_semanal.get("gmv_anterior", 0))
        _var_g   = float(tend_semanal.get("var_gmv_pct", 0))
        _var_p   = float(tend_semanal.get("var_pedidos_pct", 0))
        _var_t   = float(tend_semanal.get("var_ticket_pct", 0))
        _risco   = float(tend_semanal.get("gmv_em_risco", 0))

        st.markdown("**📉 Tendência semanal — últimas 2 semanas vs mesmo período 30 dias atrás**")
        _tc1,_tc2,_tc3,_tc4 = st.columns(4)
        def _met_delta(col, label, val_str, delta_pct):
            _c = "#E24B4A" if delta_pct < 0 else "#1ABCB0"
            _s = "+" if delta_pct > 0 else ""
            col.markdown(
                f"<div style='background:white;border-radius:10px;padding:.7rem .8rem'>"
                f"<div style='font-size:11px;color:#888;text-transform:uppercase'>{label}</div>"
                f"<div style='font-size:20px;font-weight:700;color:#1A2E2B'>{val_str}</div>"
                f"<div style='font-size:12px;font-weight:700;color:{_c}'>{_s}{delta_pct:.1f}%</div>"
                f"</div>", unsafe_allow_html=True)
        _met_delta(_tc1, "GMV (2sem)", fmt_brl(_gmv_at), _var_g)
        _met_delta(_tc2, "Pedidos", str(int(tend_semanal.get("pedidos_atual",0))), _var_p)
        _met_delta(_tc3, "Ticket médio", fmt_brl(tend_semanal.get("ticket_atual",0)), _var_t)
        _cor_r = "#E24B4A" if _risco > 0 else "#1ABCB0"
        _tc4.markdown(
            f"<div style='background:white;border-radius:10px;padding:.7rem .8rem'>"
            f"<div style='font-size:11px;color:#888;text-transform:uppercase'>GMV em risco</div>"
            f"<div style='font-size:20px;font-weight:700;color:{_cor_r}'>{fmt_brl(_risco)}</div>"
            f"<div style='font-size:12px;color:#888'>vs referência</div>"
            f"</div>", unsafe_allow_html=True)
    except Exception:
        pass


# ── DUAS TORRES ───────────────────────────────────────────────────────────────
col_t1, col_t2 = st.columns(2)

with col_t1:
    tem_prod = loja.get("data_primeira_config_produto") not in (None,"","None")
    tem_pag  = loja.get("data_primeira_config_pagamento") not in (None,"","None")
    tem_log  = loja.get("data_primeira_config_logistica") not in (None,"","None")
    tem_vis  = loja.get("data_primeira_visita") not in (None,"","None")
    tem_venda= loja.get("data_primeira_venda") not in (None,"","None")

    loja_conf = tem_prod and tem_pag and tem_log
    w1 = int(onb.get("flag_wizard_1") or (1 if loja_conf else 0))
    w2 = int(onb.get("flag_wizard_2") or (1 if loja_conf else 0))
    w3_raw = onb.get("flag_wizard_3")
    w3 = int(w3_raw) if w3_raw is not None and str(w3_raw) not in ("None","nan","") else (1 if loja_conf else 0)

    _prod_raw = loja.get("wizard_produto") or onb.get("produtos")
    try:
        produtos = int(float(_prod_raw)) if _prod_raw not in (None,"None","nan","") else None
        if produtos == 0: produtos = None
    except Exception:
        produtos = None

    env_ativo    = int(loja.get("flag_ativo_enviali") or env.get("flag_ativacao_enviali") or 0)
    pac_ativo    = int(loja.get("flag_ativo_enviali_correios_pac") or loja.get("flag_ativo_correios_pac") or env.get("flag_ativacao_pac") or 0)
    sdx_ativo    = int(loja.get("flag_ativo_enviali_correios_sedex") or loja.get("flag_ativo_correios_sedex") or env.get("flag_ativacao_sedex") or 0)
    jdl_ativo    = int(loja.get("flag_ativo_enviali_jadlog") or env.get("flag_ativacao_jadlog") or 0)
    zum_ativo    = int(loja.get("flag_ativo_enviali_zum_loggi") or env.get("flag_ativacao_zum_loggi") or 0)
    melhor_envio = int(loja.get("flag_ativo_melhor_envio") or 0)
    frenet       = int(loja.get("flag_ativo_frenet") or 0)
    motoboy      = int(loja.get("flag_ativo_motoboy") or 0)
    retirada     = int(loja.get("flag_ativo_retirar_pessoalmente") or 0)

    pag_pagali_cartao = int(loja.get("flag_ativo_pagali_cartao") or 0)
    pag_pagali_pix    = int(loja.get("flag_ativo_pagali_pix") or 0)
    pag_pagali_boleto = int(loja.get("flag_ativo_pagali_boleto") or 0)
    pag_mp_cartao     = int(loja.get("flag_ativo_mercadopago_cartao") or 0)
    pag_mp_boleto     = int(loja.get("flag_ativo_mercadopago_boleto") or 0)
    pag_pags_cartao   = int(loja.get("flag_ativo_pagseguro_cartao") or 0)
    pag_externo       = int(loja.get("flag_ativo_outros_pagamentoexterno") or 0)

    magalu_config = int(loja.get("flag_config_magalu") or 0)
    magalu_venda  = int(loja.get("flag_enviou_produto_magalu") or 0)
    gmv_magalu    = float(loja.get("vlr_gmv_magalu_ultimos_30d") or 0)
    ped_magalu    = int(loja.get("qtd_pedido_magalu_ultimos_30d") or 0)

    gmv_30d     = float(loja.get("vlr_gmv_ultimos_30d") or 0)
    pedidos_30d = int(loja.get("qtd_pedido_ultimos_30d") or 0)
    visitas_30d = int(loja.get("qtde_visitas_ultimos_30d") or 0)

    pag_html = ""
    if pag_pagali_cartao or pag_pagali_pix or pag_pagali_boleto:
        modos = [m for m, f in [("Cartão",pag_pagali_cartao),("Pix",pag_pagali_pix),("Boleto",pag_pagali_boleto)] if f]
        pag_html += linha("Pagali", tag_val(", ".join(modos)))
    if pag_mp_cartao or pag_mp_boleto: pag_html += linha("Mercado Pago", sim_nao(1))
    if pag_pags_cartao:                pag_html += linha("PagSeguro", sim_nao(1))
    if pag_externo:                    pag_html += linha("Pagamento externo", sim_nao(1))
    if not pag_html:                   pag_html  = linha("Nenhum pagamento ativo", sim_nao(0))

    frete_html = ""
    if env_ativo:
        frs = [m for m, f in [("PAC",pac_ativo),("SEDEX",sdx_ativo),("Jadlog",jdl_ativo),("Zum",zum_ativo)] if f]
        frete_html += linha("Enviali", tag_val(", ".join(frs) if frs else "Ativo"))
    if melhor_envio: frete_html += linha("Melhor Envio", sim_nao(1))
    if frenet:       frete_html += linha("Frenet", sim_nao(1))
    if pac_ativo and not env_ativo: frete_html += linha("Correios PAC", sim_nao(1))
    if sdx_ativo and not env_ativo: frete_html += linha("Correios SEDEX", sim_nao(1))
    if motoboy:      frete_html += linha("Motoboy", sim_nao(1))
    if retirada:     frete_html += linha("Retirada pessoal", sim_nao(1))
    if not frete_html: frete_html = linha("Nenhum frete ativo", sim_nao(0))

    html_t1 = (
        '<div class="torre">'
        '<div class="torre-titulo" style="color:#0D4F4A;border-color:#D1FAF6">Configuração da loja</div>'
        '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:8px 0 4px">Onboarding</div>'
        + linha("Wizard passo 1", sim_nao(w1))
        + linha("Wizard passo 2", sim_nao(w2))
        + linha("Wizard passo 3", sim_nao(w3))
        + linha("Produto cadastrado", sim_nao(tem_prod))
        + linha("Pagamento configurado", sim_nao(tem_pag))
        + linha("Frete configurado", sim_nao(tem_log))
        + '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Formas de pagamento</div>'
        + pag_html
        + '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Fretes configurados</div>'
        + frete_html
        + '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Comportamento</div>'
        + linha("Produtos cadastrados", tag_val(produtos) if produtos else "<span style='color:#9DBDBB;font-size:12px'>Não disponível</span>")
        + linha("Visitas (30d)", tag_val(visitas_30d))
        + linha("1ª visita", sim_nao(tem_vis))
        + linha("1ª venda", sim_nao(tem_venda))
        + (
            '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Marketplace</div>'
            + linha("Magalu configurado", sim_nao(magalu_config))
            + linha("Produto enviado Magalu", sim_nao(magalu_venda))
            + linha("GMV Magalu (30d)", tag_val(fmt_brl(gmv_magalu)) if gmv_magalu > 0 else tag_val("—"))
            + linha("Pedidos Magalu (30d)", tag_val(ped_magalu) if ped_magalu > 0 else tag_val("—"))
            if magalu_config else ""
        )
        + "</div>"
    )
    st.markdown(html_t1, unsafe_allow_html=True)

with col_t2:
    etiq_comp = int(env.get("etiquetas_compradas_enviali") or 0)
    etiq_post = int(env.get("etiquetas_postadas_enviali") or 0)
    etiq_canc = int(env.get("etiquetas_canceladas_enviali") or 0)
    etiq_pac  = int(env.get("etiquetas_compradas_pac") or 0)
    etiq_sdx  = int(env.get("etiquetas_compradas_sedex") or 0)
    etiq_jdl  = int(env.get("etiquetas_compradas_jadlog") or 0)
    ped_cot   = int(env.get("pedidos_cotados_enviali") or 0)

    pedidos_onb = int(onb.get("pedidos_apr") or 0)
    gmv_onb     = float(onb.get("gmv_apr") or 0)
    orig_venda  = str(onb.get("primeira_venda_origem_apr") or "—")

    bench     = diag.get("benchmark", {})
    avg_dias  = bench.get("avg_dias_venda", 23)
    taxa      = bench.get("taxa_conversao", 1.2)
    dias_loja = diag.get("dias_cadastro", 0) or 0
    ratio     = round(dias_loja/avg_dias, 1) if avg_dias > 0 else 0
    cor_ratio = "#E24B4A" if ratio > 2 else "#F59E0B" if ratio > 1 else "#22C55E"

    html_t2 = (
        '<div class="torre">'
        '<div class="torre-titulo" style="color:#534AB7;border-color:#EEEDFE">📊 Métricas e performance</div>'
        '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:8px 0 4px">Vendas</div>'
        + linha("Pedidos aprovados (30d)", tag_val(pedidos_30d))
        + linha("GMV (30d)", tag_val(fmt_brl(gmv_30d) if gmv_30d > 0 else "—"))
        + linha("Pedidos total (onboarding)", tag_val(pedidos_onb))
        + linha("GMV total (onboarding)", tag_val(fmt_brl(gmv_onb) if gmv_onb > 0 else "—"))
        + linha("Origem 1ª venda", tag_val(orig_venda))
        + '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Etiquetas Enviali</div>'
        + (
            '<div style="font-size:12px;color:#9DBDBB;padding:6px 0;font-style:italic">Loja não utiliza Enviali</div>'
            if not env_ativo and etiq_comp == 0 and ped_cot == 0
            else (
                linha("Compradas (total)", tag_val(etiq_comp))
                + linha("Postadas", tag_val(etiq_post))
                + linha("Canceladas", tag_val(etiq_canc))
                + linha("PAC compradas", tag_val(etiq_pac))
                + linha("SEDEX compradas", tag_val(etiq_sdx))
                + linha("Jadlog compradas", tag_val(etiq_jdl))
                + linha("Pedidos cotados", tag_val(ped_cot))
            )
        )
        + f'<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Benchmark — {loja.get("segmento_loja","—")}</div>'
        + linha("Avg dias para 1ª venda", tag_val(f"{avg_dias} dias"))
        + linha("Taxa de conversão", tag_val(f"{taxa}%"))
        + linha(f"Esta loja ({dias_loja} dias)", f'<span style="color:{cor_ratio};font-weight:600">{ratio}x a média</span>')
        + "</div>"
    )
    st.markdown(html_t2, unsafe_allow_html=True)


# ── OPORTUNIDADES DE PRODUTO NATIVO ──────────────────────────────────────────
_ops = []
if not env_ativo and tem_log:
    _ops.append(("Enviali não ativado",
        "Loja com frete configurado mas sem Enviali. Ativar reduz custo e amplia opções de entrega.",
        "#FFFBEB","#FDE68A","#92400E"))
if str(loja.get("status_plano","")).upper() == "GRATIS" and not tem_pag:
    _ops.append(("Pagali não configurado",
        "Plano grátis sem pagamento. Ativar o Pagali é o passo crítico para a primeira venda.",
        "#F0F9FF","#BAE6FD","#0369A1"))
if str(loja.get("status_plano","")).upper() == "GRATIS" and gmv_30d > 1000:
    _ops.append((f"Upgrade — {fmt_brl(gmv_30d)} GMV no plano grátis",
        "Loja gerando receita no plano grátis. Upgrade desbloquearia mais produtos e maior visibilidade.",
        "#F0FDF4","#86EFAC","#166534"))

if _ops:
    st.markdown("<div style='font-size:14px;font-weight:600;color:#1A2E2B;margin:.8rem 0 .4rem'>Oportunidades de produto nativo</div>", unsafe_allow_html=True)
    _cols_op = st.columns(len(_ops))
    for _i, (_t, _d, _bg, _bd, _tc) in enumerate(_ops):
        with _cols_op[_i]:
            st.markdown(
                f"<div style='background:{_bg};border:1px solid {_bd};border-left:4px solid {_bd};border-radius:10px;padding:1rem'>"
                f"<div style='font-size:13px;font-weight:700;color:{_tc};margin-bottom:6px'>{_t}</div>"
                f"<div style='font-size:12px;color:#5A7A78;line-height:1.6'>{_d}</div></div>",
                unsafe_allow_html=True)


# ── DIAGNÓSTICO + INSIGHTS + EMAIL ───────────────────────────────────────────
st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)
col_d1, col_d2 = st.columns(2)

with col_d1:
    causa    = diag.get("causa_raiz","—")
    _acoes   = diag.get("acoes", [])
    acao     = _acoes[0] if _acoes else "—"
    insights = diag.get("insights",[])
    insights_html = "".join([f'<div class="insight-item">→ {i}</div>' for i in insights]) if insights else '<div class="insight-item" style="color:#9DBDBB">Sem insights adicionais</div>'

    st.markdown(f"""
    <div class="torre">
        <div class="torre-titulo" style='color:#E24B4A;border-color:#FEF2F2'>🎯 Diagnóstico</div>
        <div style='font-size:13px;font-weight:600;color:#1A2E2B;margin-bottom:8px'>{causa}</div>
        <div style='background:#D1FAF6;border-radius:8px;padding:.8rem;
                    font-size:13px;color:#0D4F4A;margin-bottom:12px'>
            <strong>Ação:</strong> {acao}
        </div>
        <div style='font-size:11px;font-weight:600;color:#9DBDBB;
                    text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px'>Insights</div>
        {insights_html}
    </div>""", unsafe_allow_html=True)

with col_d2:
    if diag.get("email") and status not in ("LOJA ATIVA",) or perfil_loja in ("QUEDA_CRITICA","QUEDA_RISCO"):
        email = diag.get("email") or {}
        corpo_html = str(email.get("corpo","")).replace("\n","<br>")
        st.markdown(f"""
        <div class="torre">
            <div class="torre-titulo" style='color:#1ABCB0;border-color:#D1FAF6'>✉️ E-mail pronto para disparar</div>
            <div class="email-box">
                <div class="email-hdr">
                    <strong>Para:</strong> {loja.get("email_loja","—")} &nbsp;·&nbsp;
                    <strong>Assunto:</strong> {email.get("assunto","—")}
                </div>
                {corpo_html}
            </div>
            <div style='background:#F0FDF4;border-radius:8px;padding:.6rem .8rem;
                        margin-top:8px;font-size:12px;color:#166534'>
                📏 <strong>Métrica:</strong> {email.get("metrica_impacto","—")} &nbsp;·&nbsp;
                ⏱ <strong>SLA:</strong> {diag.get("sla","—")}
            </div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="torre">
            <div class="torre-titulo" style='color:#22C55E;border-color:#F0FDF4'>✅ Loja saudável</div>
            <div style='text-align:center;padding:2rem 0;color:#5A7A78'>
                <div style='font-size:32px;margin-bottom:8px'>🟢</div>
                <div style='font-size:14px;font-weight:600;color:#1A2E2B'>Nenhuma intervenção necessária</div>
                <div style='font-size:13px;margin-top:4px'>Monitorar normalmente</div>
            </div>
        </div>""", unsafe_allow_html=True)


# ── LINK PARA RAIO X COMPLETO ─────────────────────────────────────────────────
if perfil_loja in ("QUEDA_CRITICA","QUEDA_RISCO","CASA_RISCO","CASA_INATIVO"):
    st.markdown(
        f"<div style='background:#EEEDFE;border:1px solid #C4B5FD;border-radius:10px;"
        f"padding:.8rem 1.2rem;margin-top:.5rem;font-size:13px;color:#3C3489'>"
        f"🔬 <strong>Diagnóstico avançado disponível</strong> — Abra a página "
        f"<strong>Raio X</strong> no menu lateral com o ID "
        f"<strong>{loja.get('loja_id','—')}</strong> para ver: linha do tempo, "
        f"mix de pagamento, clientes churned e causa raiz detalhada."
        f"</div>",
        unsafe_allow_html=True)


# ── ROADMAP ATUALIZADO ────────────────────────────────────────────────────────
st.divider()
with st.expander("Roadmap — LI Watch · Raio X", expanded=False):
    col_r1, col_r2, col_r3 = st.columns(3)
    with col_r1:
        st.markdown("""<div style='background:#F0FDF4;border:1px solid #86EFAC;border-radius:12px;padding:1.2rem'>
<div style='font-size:11px;font-weight:700;color:#166534;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px'>FASE 1+2 — CONCLUÍDA</div>
<div style='font-size:14px;font-weight:700;color:#1A2E2B;margin-bottom:8px'>Diagnóstico + Alertas v3</div>
<div style='font-size:12px;color:#5A7A78;line-height:1.7'>
✓ Score corrigido para lojas ativas em queda<br>✓ Alerta 6: queda crítica em ativas<br>✓ Ação inline por loja no onboarding<br>✓ Causa raiz inline nos top sellers<br>✓ Tendência semanal na busca individual<br>✓ Página 3 Raio X completa
</div></div>""", unsafe_allow_html=True)
    with col_r2:
        st.markdown("""<div style='background:#FFFBEB;border:1px solid #FDE68A;border-radius:12px;padding:1.2rem'>
<div style='font-size:11px;font-weight:700;color:#92400E;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px'>FASE 3 — EM CONSTRUÇÃO</div>
<div style='font-size:14px;font-weight:700;color:#1A2E2B;margin-bottom:8px'>Benchmark + Gestão</div>
<div style='font-size:12px;color:#5A7A78;line-height:1.7'>
⏳ Página 4 Benchmark — peers por segmento<br>⏳ Página 5 Gestão — KPIs para liderança<br>⏳ Filtro por CS responsável<br>⏳ Integração HubSpot<br>⏳ Disparo automático de e-mails<br>⏳ Log de intervenções e impacto
</div></div>""", unsafe_allow_html=True)
    with col_r3:
        st.markdown("""<div style='background:#EEEDFE;border:1px solid #C4B5FD;border-radius:12px;padding:1.2rem'>
<div style='font-size:11px;font-weight:700;color:#3C3489;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px'>FASE 4 — VISÃO</div>
<div style='font-size:14px;font-weight:700;color:#1A2E2B;margin-bottom:8px'>IA Generativa</div>
<div style='font-size:12px;color:#5A7A78;line-height:1.7'>
💡 LLM analisa contexto da loja<br>💡 Diagnóstico em linguagem natural<br>💡 E-mail gerado por IA por lojista<br>💡 Detecção preditiva de churn B2B<br>💡 Chat com dados do lojista<br>💡 North Star: 5 pedidos em 15 dias
</div></div>""", unsafe_allow_html=True)
    st.markdown("""<div style='background:#0D4F4A;border-radius:10px;padding:1rem 1.2rem;margin-top:1rem;font-size:13px;color:#9DCFCC;line-height:1.6'>
<strong style='color:#D4F53C'>North Star 2026:</strong> % de novos lojistas com 5 pedidos em até 15 dias.
O Raio X é a ferramenta que viabiliza essa meta — do diagnóstico individual à automação em escala, sem dependência técnica do CS.
</div>""", unsafe_allow_html=True)


# ── DOWNLOAD ──────────────────────────────────────────────────────────────────
st.divider()
col_dl, col_ts = st.columns(2)
with col_dl:
    excel = gerar_excel(loja, diag)
    st.download_button("⬇️ Baixar diagnóstico em Excel", data=excel,
        file_name=f"liwatch_{loja.get('loja_id','loja')}_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True)
with col_ts:
    _n_diag = len(ler_metricas_uso())
    _ts     = datetime.now().strftime("%d/%m/%Y às %H:%M")
    _nm     = str(loja.get("nome_loja","—"))
    st.markdown(
        "<div style='background:#F2EDE4;border-radius:10px;padding:.8rem;font-size:12px;color:#5A7A78;text-align:center'>"
        + _ts + " &nbsp;·&nbsp; <strong style='color:#1A2E2B'>" + _nm + "</strong>"
        + " &nbsp;·&nbsp; Score " + str(score) + "/100"
        + "<br><span style='font-size:11px;color:#9DBDBB'>" + str(_n_diag)
        + " diagnóstico(s) realizados com o LI Watch</span></div>",
        unsafe_allow_html=True)
