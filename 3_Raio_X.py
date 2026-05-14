"""
LI Watch · Raio X — Diagnóstico Completo da Loja
Página 3 do app multi-page.

O que esta página entrega que o app.py não entregava:
  1. Linha do tempo visual da loja (marcos de cadastro até hoje)
  2. Score de saúde em 4 dimensões: Configuração, Tráfego, Conversão, Retenção
  3. Tendência semanal de GMV com comparação vs período anterior
  4. Mix de pagamento histórico + alerta de remoção de forma crítica
  5. Clientes B2B churned com receita em risco
  6. Ação recomendada personalizada por audiência (N2 / CS / Liderança)
  7. E-mail pronto para disparo

Requer metabase_connector.py e diagnostico_engine.py no mesmo diretório.
"""

import streamlit as st
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# ── IMPORTS ───────────────────────────────────────────────────────────────────
try:
    import metabase_connector as mb
    CONECTOR_OK = True
except Exception:
    CONECTOR_OK = False

try:
    from diagnostico_engine import diagnosticar_loja
    ENGINE_OK = True
except Exception:
    ENGINE_OK = False

try:
    from diagnostico_queda import diagnosticar_queda
    QUEDA_OK = True
except Exception:
    QUEDA_OK = False


# ── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Raio X · LI Watch",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html,body,[class*="css"]{font-family:'Inter',sans-serif;}
.block-container{padding:1.2rem 2rem 2rem;background:#F2EDE4;}
.main{background:#F2EDE4;}
.stButton>button{background:#0D4F4A!important;color:#D4F53C!important;
    font-weight:600!important;border:none!important;border-radius:10px!important;}
.stDownloadButton>button{background:#1ABCB0!important;color:#0D4F4A!important;
    font-weight:600!important;border:none!important;border-radius:10px!important;}
div[data-testid="stTextInput"]>div>div>input{
    border-radius:10px!important;border:2px solid #C8E8E6!important;
    background:white!important;font-size:15px!important;padding:10px 14px!important;}
</style>
""", unsafe_allow_html=True)


# ── HELPERS ───────────────────────────────────────────────────────────────────

def safe_float(v):
    try:
        import math
        f = float(v or 0)
        return 0.0 if math.isnan(f) else f
    except:
        return 0.0

def safe_int(v):
    try:
        return int(float(v or 0))
    except:
        return 0

def fmt_brl(v):
    return f"R${safe_float(v):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_pct(v, sinal=True):
    v = safe_float(v)
    s = "+" if v > 0 and sinal else ""
    return f"{s}{v:.1f}%"

def dias_desde(data_str):
    if not data_str or str(data_str) in ("None", "nan", "NaT", ""):
        return None
    try:
        d = datetime.strptime(str(data_str)[:10], "%Y-%m-%d").date()
        return (date.today() - d).days
    except:
        return None

def cor_variacao(v):
    if v is None: return "#888"
    return "#E24B4A" if v < 0 else "#1ABCB0"

def badge(texto, cor_bg, cor_texto):
    return f"<span style='background:{cor_bg};color:{cor_texto};padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600'>{texto}</span>"


# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='background:#0D4F4A;border-radius:14px;padding:1rem 1.5rem;margin-bottom:1.2rem;
            display:flex;align-items:center;justify-content:space-between'>
    <div>
        <div style='font-size:20px;font-weight:700;color:#D4F53C'>🔬 Raio X</div>
        <div style='font-size:12px;color:#9DBDBB;margin-top:2px'>
            Diagnóstico completo da loja · LI Watch
        </div>
    </div>
    <a href='/' style='font-size:12px;color:#9DBDBB;text-decoration:none'>← Voltar ao painel</a>
</div>
""", unsafe_allow_html=True)


# ── BUSCA ─────────────────────────────────────────────────────────────────────
col_input, col_btn = st.columns([5, 1])
with col_input:
    termo = st.text_input(
        "",
        placeholder="🔍  ID da loja ou nome — Ex: 233932 ou Arco Íris LED",
        label_visibility="collapsed",
        key="rx_busca",
    )
with col_btn:
    st.button("Analisar", use_container_width=True)

if not termo.strip():
    st.markdown("""
    <div style='text-align:center;padding:4rem 0;color:#5A7A78'>
        <div style='font-size:16px;font-weight:600;color:#1A2E2B'>🔬 Raio X</div>
        <div style='font-size:13px;margin-top:8px;color:#9DBDBB'>
            Digite o ID ou nome da loja para iniciar o diagnóstico completo
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── RESOLUÇÃO DA LOJA ─────────────────────────────────────────────────────────
loja_id = None
loja    = None

if not CONECTOR_OK:
    st.error("Metabase não conectado. Verifique as credenciais em .streamlit/secrets.toml")
    st.stop()

termo_clean = termo.strip()
try:
    if termo_clean.isdigit():
        loja_id = int(termo_clean)
    else:
        with st.spinner("Buscando loja..."):
            df_busca = mb._rodar_sql(f"""
                SELECT loja_id, upper(nome_loja) AS nome_loja,
                       upper(segmento_loja) AS segmento_loja,
                       upper(situacao_loja) AS situacao_loja,
                       email_loja
                FROM analytics_manual.mv_loja
                WHERE upper(nome_loja) LIKE upper('%{termo_clean}%')
                LIMIT 10
            """)
        if df_busca.empty:
            st.warning(f"Nenhuma loja encontrada para \"{termo_clean}\"")
            st.stop()
        elif len(df_busca) == 1:
            loja_id = int(df_busca.iloc[0]["loja_id"])
        else:
            opcoes = {
                f"{r['loja_id']} — {r['nome_loja']} ({r['segmento_loja']})": int(r["loja_id"])
                for _, r in df_busca.iterrows()
            }
            sel = st.selectbox(f"{len(df_busca)} lojas encontradas:", list(opcoes.keys()))
            loja_id = opcoes[sel]

    if not loja_id:
        st.stop()

    with st.spinner("Carregando dados da loja..."):
        loja_df = mb._rodar_sql(f"""
            SELECT
                loja_id,
                COALESCE(upper(nome_loja), upper(dominio_loja), CAST(loja_id AS CHAR)) AS nome_loja,
                dominio_loja, email_loja,
                upper(segmento_loja)    AS segmento_loja,
                upper(situacao_loja)    AS situacao_loja,
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
        """)

    if loja_df.empty:
        st.error(f"Loja {loja_id} não encontrada.")
        st.stop()

    loja = loja_df.iloc[0].to_dict()

except Exception as e:
    st.error(f"Erro ao carregar loja: {e}")
    st.stop()


# ── BUSCA DADOS COMPLEMENTARES ────────────────────────────────────────────────
hoje       = date.today()
data_ini_6m = (hoje - relativedelta(months=6)).strftime("%Y-%m-%d")
data_fim    = hoje.strftime("%Y-%m-%d")
data_ini_3m = (hoje - relativedelta(months=3)).strftime("%Y-%m-%d")

with st.spinner("Carregando histórico..."):
    try:
        df_tendencia = mb.buscar_tendencia(loja_id, data_ini_6m, data_fim)
    except:
        df_tendencia = pd.DataFrame()

    try:
        tend_semanal = mb.buscar_tendencia_semanal(loja_id)
    except:
        tend_semanal = {}

    try:
        df_pagamento = mb.buscar_mix_pagamento(loja_id, data_ini_6m, data_fim)
    except:
        df_pagamento = pd.DataFrame()

    try:
        df_novos_rec = mb.buscar_novos_recorrentes(loja_id, data_ini_6m, data_fim)
    except:
        df_novos_rec = pd.DataFrame()

    # Clientes churned: ativos em out-nov, sumidos desde fev
    try:
        m_ref_ini = (hoje - relativedelta(months=5)).strftime("%Y-%m-01")
        m_ref_fim = (hoje - relativedelta(months=4)).strftime("%Y-%m-28")
        m_corte   = (hoje - relativedelta(months=2)).strftime("%Y-%m-01")
        df_churned = mb.buscar_clientes_churned(loja_id, m_ref_ini, m_ref_fim, m_corte)
    except:
        df_churned = pd.DataFrame()


# ── DIAGNÓSTICO ───────────────────────────────────────────────────────────────
diag = {}
if ENGINE_OK:
    diag = diagnosticar_loja(loja)

diag_queda = {}
if QUEDA_OK and tend_semanal:
    diag_queda = diagnosticar_queda(loja_id, str(loja.get("nome_loja", "")), mb)


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 1 — CABEÇALHO DA LOJA
# ════════════════════════════════════════════════════════════════════════════

nome_loja   = str(loja.get("nome_loja", f"Loja {loja_id}"))
segmento    = str(loja.get("segmento_loja", "—"))
status_loja = str(loja.get("status_loja", "—"))
status_plano = str(loja.get("status_plano", "GRÁTIS"))
cidade      = str(loja.get("cidade", "—"))
estado      = str(loja.get("estado", "—"))
email_loja  = str(loja.get("email_loja", "—"))
origem      = str(loja.get("origem", "—"))
dias_cad    = dias_desde(loja.get("data_cadastro_loja")) or 0

# Cor do status
_cores_status = {
    "LOJA ATIVA":            ("#D1FAF6", "#0D4F4A"),
    "NUNCA VENDEU":          ("#FFFBEB", "#92400E"),
    "ONBOARDING INCOMPLETO": ("#FEF2F2", "#991B1B"),
    "SEM VENDAS RECENTES":   ("#FEF2F2", "#991B1B"),
}
_cor_bg, _cor_txt = _cores_status.get(status_loja, ("#F2EDE4", "#1A2E2B"))

# Score e prioridade do diagnóstico
score_risco  = diag.get("score_risco", 0)
prioridade   = diag.get("prioridade", "—")
causa_raiz   = diag.get("causa_raiz", "—")

# Variação de GMV semanal
var_gmv_sem  = None
if tend_semanal and tend_semanal.get("gmv_anterior"):
    try:
        var_gmv_sem = float(tend_semanal.get("var_gmv_pct", 0)) / 100
    except:
        pass

# Detecta queda crítica em loja "ativa"
if status_loja == "LOJA ATIVA" and var_gmv_sem is not None:
    if var_gmv_sem <= -0.30:
        status_loja = "QUEDA CRÍTICA"
        _cor_bg, _cor_txt = "#FEF2F2", "#991B1B"
        if score_risco < 70:
            score_risco = 75
    elif var_gmv_sem <= -0.20:
        status_loja = "QUEDA EM RISCO"
        _cor_bg, _cor_txt = "#FFFBEB", "#92400E"
        if score_risco < 55:
            score_risco = 55

st.markdown(f"""
<div style='background:white;border-radius:14px;padding:1.2rem 1.5rem;margin-bottom:1rem'>
    <div style='display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:.8rem'>
        <div>
            <div style='font-size:22px;font-weight:700;color:#1A2E2B'>{nome_loja}</div>
            <div style='font-size:12px;color:#888;margin-top:4px'>
                ID {loja_id} · {segmento} · {cidade}/{estado} · {email_loja}
            </div>
            <div style='margin-top:8px;display:flex;gap:6px;flex-wrap:wrap'>
                <span style='background:{_cor_bg};color:{_cor_txt};padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600'>{status_loja}</span>
                <span style='background:#EEEDFE;color:#3C3489;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600'>{status_plano}</span>
                <span style='background:#F2EDE4;color:#5A7A78;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600'>{origem} · {dias_cad}d desde cadastro</span>
            </div>
        </div>
        <div style='text-align:right'>
            <div style='font-size:40px;font-weight:800;color:{"#E24B4A" if score_risco >= 70 else "#F59E0B" if score_risco >= 40 else "#1ABCB0"}'>{score_risco}</div>
            <div style='font-size:11px;color:#888'>score de risco</div>
            <div style='font-size:12px;font-weight:600;margin-top:2px'>{prioridade}</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 2 — LINHA DO TEMPO
# ════════════════════════════════════════════════════════════════════════════

st.markdown("### 📅 Linha do tempo da loja")

marcos = [
    ("Cadastro",        loja.get("data_cadastro_loja"),              "#0D4F4A", "✅"),
    ("1ª config. pag.", loja.get("data_primeira_config_pagamento"),   "#1ABCB0", "💳"),
    ("1ª config. frete",loja.get("data_primeira_config_logistica"),   "#1ABCB0", "📦"),
    ("1ª config. prod.",loja.get("data_primeira_config_produto"),     "#1ABCB0", "🛍️"),
    ("1ª visita",       loja.get("data_primeira_visita"),             "#6366F1", "👁️"),
    ("1ª venda",        loja.get("data_primeira_venda"),              "#D4F53C", "🎉"),
]

cols_tl = st.columns(len(marcos))
for col, (label, data, cor, emoji) in zip(cols_tl, marcos):
    with col:
        if data and str(data) not in ("None", "nan", "NaT", ""):
            dias = dias_desde(data) or 0
            dt_str = str(data)[:10]
            st.markdown(
                f"<div style='background:{cor};border-radius:10px;padding:.7rem;text-align:center'>"
                f"<div style='font-size:18px'>{emoji}</div>"
                f"<div style='font-size:11px;font-weight:700;color:{'#1A2E2B' if cor == '#D4F53C' else 'white'};margin-top:4px'>{label}</div>"
                f"<div style='font-size:10px;color:{'#1A2E2B' if cor == '#D4F53C' else '#9DBDBB'};margin-top:2px'>{dt_str}</div>"
                f"<div style='font-size:10px;color:{'#1A2E2B' if cor == '#D4F53C' else '#9DBDBB'}'>{dias}d atrás</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='background:#F5F2EE;border-radius:10px;padding:.7rem;text-align:center;border:1px dashed #C8C0B4'>"
                f"<div style='font-size:18px;opacity:.3'>{emoji}</div>"
                f"<div style='font-size:11px;color:#AAA;margin-top:4px'>{label}</div>"
                f"<div style='font-size:10px;color:#CCC;margin-top:2px'>não realizado</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 3 — SCORE 4 DIMENSÕES
# ════════════════════════════════════════════════════════════════════════════

st.markdown("### 📊 Score de saúde — 4 dimensões")

# Calcula cada dimensão
tem_prod = str(loja.get("data_primeira_config_produto", "")) not in ("", "None", "nan", "NaT")
tem_pag  = str(loja.get("data_primeira_config_pagamento", "")) not in ("", "None", "nan", "NaT")
tem_log  = str(loja.get("data_primeira_config_logistica", "")) not in ("", "None", "nan", "NaT")
score_config = (int(tem_prod) + int(tem_pag) + int(tem_log)) / 3 * 100

visitas = safe_int(loja.get("qtde_visitas_ultimos_30d"))
score_trafego = min(100, visitas / 10)  # 1000 visitas = 100%

gmv_30d   = safe_float(loja.get("vlr_gmv_ultimos_30d"))
pedidos_30d = safe_int(loja.get("qtd_pedido_ultimos_30d"))
tem_venda = str(loja.get("data_primeira_venda", "")) not in ("", "None", "nan", "NaT")
score_conversao = 100 if (tem_venda and pedidos_30d > 0) else (50 if tem_venda else 0)

# Retenção: baseada na tendência semanal
if var_gmv_sem is not None:
    score_retencao = max(0, min(100, 60 + var_gmv_sem * 100))
elif tem_venda and pedidos_30d > 0:
    score_retencao = 60
elif tem_venda:
    score_retencao = 20
else:
    score_retencao = 0

dimensoes = [
    ("Configuração",   score_config,    "Prod + Pag + Frete",   "#0D4F4A"),
    ("Tráfego",        score_trafego,   f"{visitas} visitas/30d","#6366F1"),
    ("Conversão",      score_conversao, f"{pedidos_30d} pedidos/30d", "#F59E0B"),
    ("Retenção",       score_retencao,  "Tendência semanal",    "#E24B4A" if score_retencao < 40 else "#1ABCB0"),
]

cols_dim = st.columns(4)
for col, (nome, score, sub, cor) in zip(cols_dim, dimensoes):
    with col:
        cor_score = cor if score >= 50 else "#E24B4A"
        st.markdown(
            f"<div style='background:white;border-radius:12px;padding:1rem;text-align:center'>"
            f"<div style='font-size:13px;font-weight:700;color:#1A2E2B;margin-bottom:.5rem'>{nome}</div>"
            f"<div style='font-size:36px;font-weight:800;color:{cor_score}'>{score:.0f}</div>"
            f"<div style='background:#F2EDE4;border-radius:6px;height:6px;margin:.5rem 0'>"
            f"<div style='background:{cor_score};width:{min(score,100):.0f}%;height:6px;border-radius:6px'></div></div>"
            f"<div style='font-size:11px;color:#888'>{sub}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 4 — TENDÊNCIA SEMANAL
# ════════════════════════════════════════════════════════════════════════════

st.markdown("### 📉 Tendência de GMV — últimas 2 semanas vs mesmo período 30 dias atrás")

if tend_semanal and tend_semanal.get("gmv_anterior"):
    gmv_atual  = safe_float(tend_semanal.get("gmv_atual"))
    gmv_ant    = safe_float(tend_semanal.get("gmv_anterior"))
    ped_atual  = safe_int(tend_semanal.get("pedidos_atual"))
    ped_ant    = safe_int(tend_semanal.get("pedidos_anterior"))
    tick_atual = safe_float(tend_semanal.get("ticket_atual"))
    tick_ant   = safe_float(tend_semanal.get("ticket_anterior"))
    var_gmv    = safe_float(tend_semanal.get("var_gmv_pct"))
    var_ped    = safe_float(tend_semanal.get("var_pedidos_pct"))
    var_tick   = safe_float(tend_semanal.get("var_ticket_pct"))
    gmv_risco  = safe_float(tend_semanal.get("gmv_em_risco"))
    atual_de   = str(tend_semanal.get("atual_de", ""))[:10]
    atual_ate  = str(tend_semanal.get("atual_ate", ""))[:10]
    ref_de     = str(tend_semanal.get("ref_de", ""))[:10]
    ref_ate    = str(tend_semanal.get("ref_ate", ""))[:10]

    st.caption(f"Período atual: {atual_de} → {atual_ate} | Referência: {ref_de} → {ref_ate}")

    col1, col2, col3, col4 = st.columns(4)
    metricas = [
        ("GMV atual",      fmt_brl(gmv_atual),  var_gmv,  fmt_brl(gmv_ant)),
        ("Pedidos",        str(ped_atual),       var_ped,  str(ped_ant)),
        ("Ticket médio",   fmt_brl(tick_atual),  var_tick, fmt_brl(tick_ant)),
        ("GMV em risco",   fmt_brl(gmv_risco),   None,     "vs referência"),
    ]
    for col, (label, valor, var, ref) in zip([col1,col2,col3,col4], metricas):
        with col:
            if var is not None:
                cor_v = "#E24B4A" if var < 0 else "#1ABCB0"
                sinal = "+" if var > 0 else ""
                var_str = f"<span style='color:{cor_v};font-weight:700'>{sinal}{var:.1f}%</span>"
            else:
                cor_v = "#E24B4A" if gmv_risco > 0 else "#1ABCB0"
                var_str = f"<span style='color:{cor_v};font-weight:700'>{'↓' if gmv_risco > 0 else '↑'}</span>"

            st.markdown(
                f"<div style='background:white;border-radius:10px;padding:.8rem 1rem'>"
                f"<div style='font-size:11px;color:#888;text-transform:uppercase'>{label}</div>"
                f"<div style='font-size:22px;font-weight:700;color:#1A2E2B'>{valor}</div>"
                f"<div style='font-size:12px;margin-top:2px'>{var_str} vs {ref}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # Causa raiz da queda (do diagnostico_queda)
    if diag_queda and diag_queda.get("causa_raiz"):
        sev = diag_queda.get("severidade", "INVESTIGAR")
        cor_sev = {"CRÍTICO": "#FEF2F2", "RISCO": "#FFFBEB", "ATENÇÃO": "#FFFBEB"}.get(sev, "#F0F9FF")
        bor_sev = {"CRÍTICO": "#E24B4A", "RISCO": "#F59E0B", "ATENÇÃO": "#F59E0B"}.get(sev, "#60A5FA")
        txt_sev = {"CRÍTICO": "#991B1B", "RISCO": "#92400E", "ATENÇÃO": "#92400E"}.get(sev, "#1E40AF")
        st.markdown(
            f"<div style='background:{cor_sev};border-left:4px solid {bor_sev};border-radius:8px;"
            f"padding:.8rem 1rem;margin-top:.8rem'>"
            f"<div style='font-size:12px;font-weight:700;color:{txt_sev}'>Causa raiz detectada</div>"
            f"<div style='font-size:13px;color:#333;margin-top:4px'>{diag_queda.get('causa_raiz','—')}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        if diag_queda.get("causas") and len(diag_queda["causas"]) > 1:
            with st.expander(f"Ver todas as {len(diag_queda['causas'])} causas identificadas"):
                for c in diag_queda["causas"]:
                    st.markdown(f"**{c['emoji']} {c['tipo']}** (peso {c['peso']}) — {c['descricao']}")
else:
    st.info("Histórico semanal insuficiente para calcular tendência. A loja pode ser muito nova ou não ter vendas no período.")

st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 5 — HISTÓRICO MENSAL
# ════════════════════════════════════════════════════════════════════════════

if not df_tendencia.empty:
    st.markdown("### 📆 Evolução mensal — últimos 6 meses")

    df_t = df_tendencia.copy()
    for col_num in ["total_pedidos","receita_total","ticket_medio","desconto_medio","frete_medio"]:
        if col_num in df_t.columns:
            df_t[col_num] = pd.to_numeric(df_t[col_num], errors="coerce").fillna(0)

    # Formata para exibição
    df_show = pd.DataFrame()
    df_show["Mês"]             = df_t["mes"]
    df_show["Pedidos"]         = df_t["total_pedidos"].astype(int)
    df_show["Receita"]         = df_t["receita_total"].apply(fmt_brl)
    df_show["Ticket Médio"]    = df_t["ticket_medio"].apply(fmt_brl)
    df_show["Desconto Médio"]  = df_t["desconto_medio"].apply(fmt_brl) if "desconto_medio" in df_t else "—"

    # Variação mês a mês
    if len(df_t) >= 2:
        var_mensal = df_t["receita_total"].pct_change().fillna(0) * 100
        df_show["Var. Receita"] = var_mensal.apply(
            lambda v: f"+{v:.1f}%" if v > 0 else f"{v:.1f}%"
        )

    st.dataframe(df_show, use_container_width=True, hide_index=True)
    st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 6 — MIX DE PAGAMENTO
# ════════════════════════════════════════════════════════════════════════════

if not df_pagamento.empty and "mes" in df_pagamento.columns:
    st.markdown("### 💳 Mix de pagamento — últimos 6 meses")

    df_pag = df_pagamento.copy()
    df_pag["total_pedidos"] = pd.to_numeric(df_pag["total_pedidos"], errors="coerce").fillna(0)
    df_pag["ticket_medio"]  = pd.to_numeric(df_pag["ticket_medio"],  errors="coerce").fillna(0)

    # Detecta formas removidas (alerta)
    meses_sorted = sorted(df_pag["mes"].unique())
    if len(meses_sorted) >= 2:
        m_recente = meses_sorted[-1]
        m_anterior = meses_sorted[-2]
        formas_recente  = set(df_pag[df_pag["mes"] == m_recente]["forma_pagamento"].tolist())
        formas_anterior = set(df_pag[df_pag["mes"] == m_anterior]["forma_pagamento"].tolist())
        removidas = formas_anterior - formas_recente
        criticas  = [f for f in removidas if any(k in str(f).upper() for k in ["PIX","CART","CRED","DEBIT"])]

        if criticas:
            st.markdown(
                f"<div style='background:#FEF2F2;border-left:4px solid #E24B4A;border-radius:8px;"
                f"padding:.7rem 1rem;margin-bottom:.8rem'>"
                f"<div style='font-size:13px;font-weight:700;color:#991B1B'>⚠️ Forma de pagamento crítica removida</div>"
                f"<div style='font-size:12px;color:#666;margin-top:3px'>"
                f"{', '.join(criticas)} não aparece em {m_recente} mas estava em {m_anterior}.</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
        elif removidas:
            st.markdown(
                f"<div style='background:#FFFBEB;border-left:4px solid #F59E0B;border-radius:8px;"
                f"padding:.7rem 1rem;margin-bottom:.8rem'>"
                f"<div style='font-size:12px;color:#92400E'>Forma de pagamento removida em {m_recente}: {', '.join(removidas)}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # Tabela pivot
    try:
        piv = df_pag.pivot_table(
            index="forma_pagamento",
            columns="mes",
            values="total_pedidos",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()
        piv.columns.name = None
        st.dataframe(piv, use_container_width=True, hide_index=True)
    except:
        st.dataframe(df_pag[["mes","forma_pagamento","total_pedidos","ticket_medio"]], use_container_width=True, hide_index=True)

    st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 7 — NOVOS VS RECORRENTES
# ════════════════════════════════════════════════════════════════════════════

if not df_novos_rec.empty and "mes" in df_novos_rec.columns:
    st.markdown("### 👥 Novos vs. recorrentes")

    df_nr = df_novos_rec.copy()
    df_nr["total_pedidos"] = pd.to_numeric(df_nr["total_pedidos"], errors="coerce").fillna(0)
    df_nr["ticket_medio"]  = pd.to_numeric(df_nr["ticket_medio"],  errors="coerce").fillna(0)

    # Detecta queda de recorrentes (sinal de churn B2B)
    rec = df_nr[df_nr["tipo_cliente"] == "Recorrente"].sort_values("mes")
    if len(rec) >= 2:
        receita_rec = pd.to_numeric(rec.get("receita_total", rec["total_pedidos"]), errors="coerce")
        if not receita_rec.empty:
            queda_rec = (receita_rec.iloc[-1] - receita_rec.iloc[0]) / max(receita_rec.iloc[0], 1) * 100
            if queda_rec <= -40:
                st.markdown(
                    f"<div style='background:#FEF2F2;border-left:4px solid #E24B4A;border-radius:8px;"
                    f"padding:.7rem 1rem;margin-bottom:.8rem'>"
                    f"<div style='font-size:13px;font-weight:700;color:#991B1B'>🔴 Queda severa de clientes recorrentes</div>"
                    f"<div style='font-size:12px;color:#666;margin-top:3px'>"
                    f"Volume de recorrentes caiu {abs(queda_rec):.0f}% no período — sinal de churn B2B.</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    st.dataframe(
        df_nr[["mes","tipo_cliente","total_pedidos","ticket_medio"]].rename(columns={
            "mes":"Mês","tipo_cliente":"Tipo","total_pedidos":"Pedidos","ticket_medio":"Ticket Médio"
        }),
        use_container_width=True, hide_index=True,
    )
    st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 8 — CLIENTES CHURNED
# ════════════════════════════════════════════════════════════════════════════

if not df_churned.empty:
    receita_risco = safe_float(df_churned["receita_total_historico"].sum()) if "receita_total_historico" in df_churned.columns else 0

    st.markdown(f"### ⚠️ Clientes que sumiram — {len(df_churned)} identificados")
    st.markdown(
        f"<div style='background:#FEF2F2;border-left:4px solid #E24B4A;border-radius:8px;"
        f"padding:.8rem 1rem;margin-bottom:.8rem;display:flex;justify-content:space-between;align-items:center'>"
        f"<div>"
        f"<div style='font-size:13px;font-weight:700;color:#991B1B'>"
        f"💰 {fmt_brl(receita_risco)} em receita histórica dos churned</div>"
        f"<div style='font-size:12px;color:#666;margin-top:3px'>"
        f"Clientes ativos no período de referência que pararam de comprar. Perfil predominantemente B2B.</div>"
        f"</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    cols_ch = [c for c in ["cliente_nome","cliente_email","total_pedidos_historico",
                            "receita_total_historico","ticket_medio","ultimo_pedido"]
               if c in df_churned.columns]
    if cols_ch:
        df_ch_show = df_churned[cols_ch].copy()
        if "receita_total_historico" in df_ch_show:
            df_ch_show["receita_total_historico"] = df_ch_show["receita_total_historico"].apply(fmt_brl)
        if "ticket_medio" in df_ch_show:
            df_ch_show["ticket_medio"] = df_ch_show["ticket_medio"].apply(fmt_brl)
        st.dataframe(df_ch_show.head(15), use_container_width=True, hide_index=True)

    st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 9 — AÇÃO RECOMENDADA (por audiência)
# ════════════════════════════════════════════════════════════════════════════

st.markdown("### 🎯 Ação recomendada")

tab_n2, tab_cs, tab_lider = st.tabs(["🔧 N2 · Automação", "📞 CS · Sucesso do Cliente", "📊 Liderança"])

acoes = diag.get("acoes", [])
insights = diag.get("insights", [])
canal = diag.get("canal", "—")
sla   = diag.get("sla", "—")

with tab_n2:
    st.markdown(f"**Canal:** {canal} · **SLA:** {sla}")
    if insights:
        for ins in insights:
            st.markdown(f"- {ins}")
    else:
        st.info("Sem insights de diagnóstico técnico disponíveis.")

    if diag_queda and diag_queda.get("sinais"):
        sinais = diag_queda["sinais"]
        st.markdown("**Sinais do diagnóstico de queda:**")
        cols_sig = st.columns(3)
        with cols_sig[0]:
            st.metric("GMV atual (2sem)", fmt_brl(sinais.get("gmv_atual")), f"{sinais.get('var_gmv_pct',0):+.1f}%")
        with cols_sig[1]:
            st.metric("Var. pedidos", f"{sinais.get('var_ped_pct',0):+.1f}%")
        with cols_sig[2]:
            st.metric("Var. ticket", f"{sinais.get('var_tick_pct',0):+.1f}%")

with tab_cs:
    # Traduz para linguagem operacional
    status_cs = {
        "ONBOARDING INCOMPLETO": "Esta loja ainda não terminou a configuração. Entrar em contato para ajudar a completar os passos que faltam.",
        "NUNCA VENDEU":          "Loja configurada mas sem nenhuma venda. Oferecer suporte para divulgação e primeiros pedidos.",
        "SEM VENDAS RECENTES":   "Loja que vendeu no passado mas parou. Verificar o que mudou e oferecer reativação.",
        "QUEDA CRÍTICA":         "Loja ativa mas com queda acelerada de GMV. Contato imediato — risco de churn de clientes B2B.",
        "QUEDA EM RISCO":        "GMV caindo há semanas. Monitorar de perto e oferecer diagnóstico proativo.",
        "LOJA ATIVA":            "Loja saudável. Monitoramento de rotina.",
    }
    st.markdown(f"**Situação resumida:** {status_cs.get(status_loja, 'Verificar diagnóstico.')}")
    if acoes:
        st.markdown("**O que fazer agora:**")
        for acao in acoes[:3]:
            st.markdown(f"→ {acao}")
    if not df_churned.empty:
        st.markdown(f"→ Contatar os **{min(5, len(df_churned))} principais clientes churned** listados acima — não usar e-mail marketing genérico, usar WhatsApp ou telefone.")

with tab_lider:
    col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
    with col_kpi1:
        st.metric("GMV últimos 30d", fmt_brl(gmv_30d))
    with col_kpi2:
        st.metric("Pedidos 30d", str(pedidos_30d))
    with col_kpi3:
        var_disp = f"{var_gmv:.1f}%" if (var_gmv := safe_float(tend_semanal.get("var_gmv_pct"))) else "—"
        st.metric("Var. GMV semanal", var_disp)

    st.markdown(f"**Causa raiz:** {causa_raiz}")
    if receita_risco := (safe_float(df_churned["receita_total_historico"].sum()) if not df_churned.empty and "receita_total_historico" in df_churned.columns else 0):
        st.metric("Receita em risco (churned)", fmt_brl(receita_risco))

st.divider()


# ════════════════════════════════════════════════════════════════════════════
# SEÇÃO 10 — E-MAIL PRONTO
# ════════════════════════════════════════════════════════════════════════════

if diag.get("email"):
    email = diag["email"]
    st.markdown("### ✉️ E-mail pronto para envio")
    st.markdown(
        f"<div style='background:#F8F5F0;border:1px solid #E8E4DE;border-radius:12px;padding:1.2rem;font-size:13px;line-height:1.7;color:#1A2E2B'>"
        f"<div style='font-size:11px;color:#888;border-bottom:1px solid #E8E4DE;padding-bottom:8px;margin-bottom:12px'>"
        f"<strong>Para:</strong> {email_loja} &nbsp;|&nbsp; <strong>Assunto:</strong> {email.get('assunto','')}</div>"
        f"<pre style='font-family:inherit;white-space:pre-wrap;margin:0'>{email.get('corpo','')}</pre>"
        f"</div>",
        unsafe_allow_html=True,
    )
    st.caption(f"Métrica de impacto esperada: {email.get('metrica_impacto','')}")


# ── EXPORTAR ──────────────────────────────────────────────────────────────────
import io, json
export_data = {
    "loja_id":    loja_id,
    "nome":       nome_loja,
    "status":     status_loja,
    "score":      score_risco,
    "causa_raiz": causa_raiz,
    "acoes":      acoes,
    "var_gmv_semanal_pct": tend_semanal.get("var_gmv_pct") if tend_semanal else None,
    "churned_count": len(df_churned),
    "receita_risco": safe_float(df_churned["receita_total_historico"].sum()) if not df_churned.empty and "receita_total_historico" in df_churned.columns else 0,
    "data_geracao": date.today().strftime("%Y-%m-%d"),
}
st.download_button(
    "📥 Exportar diagnóstico JSON",
    data=json.dumps(export_data, ensure_ascii=False, indent=2),
    file_name=f"raio_x_{loja_id}_{date.today().strftime('%Y%m%d')}.json",
    mime="application/json",
)
