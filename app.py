import streamlit as st
import pandas as pd
import requests
import io
from datetime import datetime

# ── IMPORTS CONDICIONAIS ──────────────────────────────────────────────────────
try:
    from diagnostico_engine import diagnosticar_loja
    ENGINE_OK = True
except:
    ENGINE_OK = False

# ── CONFIG ────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="LI Watch · Loja Integrada",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="collapsed"
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

.insight-item{font-size:13px;color:#444;padding:6px 0;border-bottom:1px solid #F5F2EE;
    line-height:1.5;}
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
        tem_auth = "api_key" in cfg or "token" in cfg
        return bool(cfg.get("url") and tem_auth)
    except:
        return False

def _headers():
    try:
        cfg = st.secrets["metabase"]
        if "api_key" in cfg:
            return {"x-api-key": cfg["api_key"], "Content-Type": "application/json"}
        return {"X-Metabase-Session": cfg["token"], "Content-Type": "application/json"}
    except:
        return {}

def _url():
    try: return st.secrets["metabase"]["url"]
    except: return ""

def _db():
    try: return int(st.secrets["metabase"]["db_id"])
    except: return 11

def rodar_sql(sql):
    import urllib.request, json as _json
    s = st.secrets["metabase"]
    payload = _json.dumps({
        "database": _db(),
        "native":   {"query": sql},
        "type":     "native",
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{_url()}/api/dataset",
        data=payload,
        headers={"Content-Type": "application/json", "x-api-key": s["api_key"]},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = _json.loads(resp.read().decode("utf-8"))
    if "error" in data:
        raise Exception(data["error"])
    cols = [c["name"] for c in data["data"]["cols"]]
    return pd.DataFrame(data["data"]["rows"], columns=cols)

@st.cache_data(ttl=300)
def buscar_dados_loja(loja_id: int):
    """Busca todos os dados da loja nas 3 tabelas."""

    # mv_loja — dados gerais
    sql_loja = f"""
    SELECT
        loja_id,
        upper(nome_loja)            AS nome_loja,
        dominio_loja,
        email_loja,
        upper(segmento_loja)        AS segmento_loja,
        upper(situacao_loja)        AS situacao_loja,
        data_cadastro_loja,
        upper(cidade_endereco_loja) AS cidade,
        upper(estado_endereco_loja) AS estado,
        aquisicao_utm_source,
        CASE WHEN aquisicao_utm_source IS NULL THEN 'ORGÂNICO' ELSE 'PAGO' END AS origem,
        data_primeira_config_pagamento,
        data_primeira_config_logistica,
        data_primeira_config_produto,
        data_ini_plano_atual,
        upper(tipo_plano_atual)     AS tipo_plano,
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
    """

    # temp_onboarding_lojas_30d — wizard e comportamento
    sql_onb = f"""
    SELECT *
    FROM analytics_manual.temp_onboarding_lojas_30d
    WHERE loja_id = {loja_id}
    LIMIT 1
    """

    # mv_enviali_loja — fretes e etiquetas
    sql_env = f"""
    SELECT *
    FROM analytics_manual.mv_enviali_loja
    WHERE loja_id = {loja_id}
    LIMIT 1
    """

    loja_df = rodar_sql(sql_loja)
    try:
        onb_df = rodar_sql(sql_onb)
    except:
        onb_df = pd.DataFrame()
    try:
        env_df = rodar_sql(sql_env)
    except:
        env_df = pd.DataFrame()

    return loja_df, onb_df, env_df

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

# ── DEMOS ─────────────────────────────────────────────────────────────────────

def demo_loja(loja_id):
    demos = {
        421: dict(loja_id=421,nome_loja="MODA DA ANA",dominio_loja="modadaana.lojaintegrada.com",
            email_loja="ana@modadaana.com",segmento_loja="MODA E ACESSÓRIOS",
            situacao_loja="ATIVA",status_plano="GRÁTIS",origem="ORGÂNICO",
            data_cadastro_loja="2026-04-22",data_primeira_config_produto="2026-04-22",
            data_primeira_config_pagamento=None,data_primeira_config_logistica=None,
            data_primeira_visita=None,data_primeira_venda=None,
            qtde_visitas_ultimos_30d=0,vlr_gmv_ultimos_30d=0,qtd_pedido_ultimos_30d=0,
            status_loja="ONBOARDING INCOMPLETO",cidade="SÃO PAULO",estado="SP",
            tipo_plano="GRÁTIS",vlr_plano_mrr_atual=0),
        834: dict(loja_id=834,nome_loja="TECH STORE BR",dominio_loja="techstorebr.com",
            email_loja="contato@techstorebr.com",segmento_loja="ELETRÔNICOS",
            situacao_loja="ATIVA",status_plano="PAGO",origem="PAGO",
            data_cadastro_loja="2026-04-17",data_primeira_config_produto="2026-04-17",
            data_primeira_config_pagamento="2026-04-17",data_primeira_config_logistica="2026-04-17",
            data_primeira_visita="2026-04-18",data_primeira_venda=None,
            qtde_visitas_ultimos_30d=48,vlr_gmv_ultimos_30d=0,qtd_pedido_ultimos_30d=0,
            status_loja="NUNCA VENDEU",cidade="CURITIBA",estado="PR",
            tipo_plano="PROFISSIONAL",vlr_plano_mrr_atual=79),
    }
    return demos.get(loja_id, demos[421])

def demo_onb(loja_id):
    return dict(flag_wizard_1=1,flag_wizard_2=1,flag_wizard_3=0,
        produtos=3,visitas=48,pedidos_cap=0,pedidos_apr=0,
        gmv_cap=0,gmv_apr=0,primeira_venda_origem_cap=None,
        data_primeira_venda_apr=None)

def demo_env(loja_id):
    return dict(flag_ativacao_enviali=0,flag_ativacao_pac=0,flag_ativacao_sedex=0,
        flag_ativacao_jadlog=0,flag_ativacao_zum_loggi=0,
        etiquetas_compradas_enviali=0,etiquetas_postadas_enviali=0,
        etiquetas_canceladas_enviali=0,gmv=0,pedidos=0)

# ── HELPERS ───────────────────────────────────────────────────────────────────

def sim_nao(val):
    v = str(val) if val is not None else "0"
    ok = v not in ("0","None","False","","nan")
    if ok: return '<span class="tag-sim">✓ Sim</span>'
    return '<span class="tag-nao">✗ Não</span>'

def tag_val(val, prefix="", suffix=""):
    v = str(val) if val is not None else "—"
    if v in ("None","nan","0","0.0",""): v = "—"
    if v != "—" and prefix: v = prefix + v
    if v != "—" and suffix: v = v + suffix
    return f'<span class="tag-info">{v}</span>'

def linha(label, valor_html):
    return f"""<div class="torre-item">
        <span class="torre-label">{label}</span>
        <span>{valor_html}</span>
    </div>"""

def gerar_excel(loja, diag):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame([{
            "ID": loja.get("loja_id"), "Nome": loja.get("nome_loja"),
            "Status": loja.get("status_loja"), "Score": diag.get("score_risco"),
            "Prioridade": diag.get("prioridade"), "Causa raiz": diag.get("causa_raiz"),
            "Ação": diag.get("acoes",[""])[0], "Data": datetime.now().strftime("%d/%m/%Y %H:%M"),
        }]).T.reset_index().rename(columns={"index":"Campo",0:"Valor"}).to_excel(
            writer, sheet_name="Diagnóstico", index=False)
    return output.getvalue()

# ── HEADER ────────────────────────────────────────────────────────────────────

modo_real = _ok()
st.markdown(f"""
<div class="liwatch-header">
    <div>
        <div class="liwatch-logo">👁️ LI Watch</div>
        <div class="liwatch-sub">Diagnóstico de lojistas · Loja Integrada</div>
    </div>
    <div class="liwatch-badge">{'● Dados reais' if modo_real else '● Demo — dados simulados'}</div>
</div>
""", unsafe_allow_html=True)

# ── BUSCA ─────────────────────────────────────────────────────────────────────

col_b, col_btn = st.columns([5,1])
with col_b:
    termo = st.text_input("", placeholder="🔍  Digite o ID da loja ou nome — Ex: 123456 ou Virtual Make",
        label_visibility="collapsed")
with col_btn:
    st.button("Buscar", use_container_width=True)

if not termo.strip():
    st.markdown("""
    <div style='text-align:center;padding:3rem 0;color:#5A7A78'>
        <div style='font-size:48px;margin-bottom:1rem'>👁️</div>
        <div style='font-size:18px;font-weight:600;color:#1A2E2B;margin-bottom:8px'>LI Watch</div>
        <div style='font-size:14px'>Digite o ID ou nome de uma loja para ver o diagnóstico completo</div>
        <div style='font-size:13px;margin-top:8px;color:#9DBDBB'>
            Configurações · Fretes · Pagamentos · Métricas · Score de risco · E-mail pronto
        </div>
    </div>""", unsafe_allow_html=True)
    st.stop()

# ── RESOLVE LOJA ──────────────────────────────────────────────────────────────

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
                opcoes = {f"{r['loja_id']} — {r['nome_loja']} ({r['segmento_loja']})": int(r['loja_id'])
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
            onb  = onb_df.iloc[0].to_dict() if not onb_df.empty else demo_onb(loja_id)
            env  = env_df.iloc[0].to_dict() if not env_df.empty else demo_env(loja_id)

    except Exception as e:
        st.warning(f"Erro na conexão: {e}. Usando dados simulados.")
        modo_real = False

if not modo_real or loja is None:
    loja_id = int(termo_clean) if termo_clean.isdigit() else 421
    loja = demo_loja(loja_id)
    onb  = demo_onb(loja_id)
    env  = demo_env(loja_id)

# ── DIAGNÓSTICO ───────────────────────────────────────────────────────────────

if ENGINE_OK:
    diag = diagnosticar_loja(loja)
else:
    scores = {"ONBOARDING INCOMPLETO":70,"NUNCA VENDEU":45,"SEM VENDAS RECENTES":55,"LOJA ATIVA":5}
    score  = scores.get(str(loja.get("status_loja","")).upper(), 30)
    diag   = {"score_risco":score,"prioridade":"🔴 CRÍTICA" if score>=70 else "🟠 ALTA",
              "sla":"Intervir hoje","canal":"E-mail + CS","causa_raiz":loja.get("status_loja",""),
              "insights":[],"acoes":["Verificar configurações"],"benchmark":{},
              "email":{"assunto":"Atenção necessária","corpo":"Olá!\n\nSua loja precisa de atenção.\n\n— Time LI","metrica_impacto":"Ação em 24h"}}

score      = diag["score_risco"]
prioridade = diag["prioridade"]
status     = str(loja.get("status_loja","")).upper()

# Cores
if score >= 70:   cor="#E24B4A"; bg="#FEF2F2"; bd="#FCA5A5"
elif score >= 45: cor="#F59E0B"; bg="#FFFBEB"; bd="#FDE68A"
elif score >= 25: cor="#1ABCB0"; bg="#D1FAF6"; bd="#99F6E4"
else:             cor="#22C55E"; bg="#F0FDF4"; bd="#86EFAC"

# ── ALERTA NO TOPO ────────────────────────────────────────────────────────────

if score >= 70:
    st.markdown(f'<div class="alerta-critico">🔴 <strong>CRÍTICO — {prioridade}</strong> &nbsp;·&nbsp; {diag["sla"]} &nbsp;·&nbsp; {diag["canal"]}</div>', unsafe_allow_html=True)
elif score >= 45:
    st.markdown(f'<div class="alerta-atencao">🟠 <strong>ATENÇÃO — {prioridade}</strong> &nbsp;·&nbsp; {diag["sla"]}</div>', unsafe_allow_html=True)
elif status == "LOJA ATIVA":
    st.markdown('<div class="alerta-ok">✅ <strong>LOJA ATIVA</strong> — Sem intervenção necessária</div>', unsafe_allow_html=True)

# ── CABEÇALHO DA LOJA ─────────────────────────────────────────────────────────

col_nome, col_score = st.columns([4,1])

with col_nome:
    mrr = loja.get("vlr_plano_mrr_atual") or 0
    st.markdown(f"""
    <div style='background:white;border-radius:14px;padding:1.2rem;margin-bottom:1rem'>
        <div style='font-size:20px;font-weight:700;color:#1A2E2B'>{loja.get("nome_loja","—")}</div>
        <div style='font-size:13px;color:#5A7A78;margin-top:4px'>
            ID: <strong>{loja.get("loja_id","—")}</strong> &nbsp;·&nbsp;
            {loja.get("segmento_loja","—")} &nbsp;·&nbsp;
            {loja.get("cidade","—")}/{loja.get("estado","—")} &nbsp;·&nbsp;
            Plano: <strong>{loja.get("status_plano","—")} {("— R$"+str(mrr)+"/mês") if float(mrr or 0)>0 else ""}</strong> &nbsp;·&nbsp;
            Origem: <strong>{loja.get("origem","—")}</strong>
        </div>
        <div style='font-size:13px;color:#5A7A78;margin-top:2px'>
            📧 {loja.get("email_loja","—")} &nbsp;·&nbsp; 🌐 {loja.get("dominio_loja","—")}
        </div>
    </div>""", unsafe_allow_html=True)

with col_score:
    st.markdown(f"""
    <div style='background:{bg};border:2px solid {bd};border-radius:14px;
                padding:1rem;text-align:center;margin-bottom:1rem'>
        <div style='font-size:11px;color:#5A7A78'>Score de risco</div>
        <div class="score-num" style='color:{cor}'>{score}</div>
        <div style='font-size:11px;color:#5A7A78'>de 100</div>
        <div style='font-size:13px;font-weight:700;color:{cor};margin-top:4px'>{prioridade}</div>
    </div>""", unsafe_allow_html=True)

# ── DUAS TORRES PRINCIPAIS ────────────────────────────────────────────────────

col_t1, col_t2 = st.columns(2)

# ── TORRE 1: O QUE CONFIGUROU ─────────────────────────────────────────────────
with col_t1:
    tem_prod = loja.get("data_primeira_config_produto") not in (None,"","None")
    tem_pag  = loja.get("data_primeira_config_pagamento") not in (None,"","None")
    tem_log  = loja.get("data_primeira_config_logistica") not in (None,"","None")
    tem_vis  = loja.get("data_primeira_visita") not in (None,"","None")
    tem_venda= loja.get("data_primeira_venda") not in (None,"","None")

    # Wizard steps do onboarding
    w1 = int(onb.get("flag_wizard_1") or 0)
    w2 = int(onb.get("flag_wizard_2") or 0)
    w3 = int(onb.get("flag_wizard_3") or 0)

    # Enviali
    env_ativo  = int(env.get("flag_ativacao_enviali") or 0)
    pac_ativo  = int(env.get("flag_ativacao_pac") or 0)
    sdx_ativo  = int(env.get("flag_ativacao_sedex") or 0)
    jdl_ativo  = int(env.get("flag_ativacao_jadlog") or 0)
    zum_ativo  = int(env.get("flag_ativacao_zum_loggi") or 0)

    gmv_30d    = float(loja.get("vlr_gmv_ultimos_30d") or 0)
    pedidos_30d= int(loja.get("qtd_pedido_ultimos_30d") or 0)
    visitas_30d= int(loja.get("qtde_visitas_ultimos_30d") or 0)
    produtos   = int(onb.get("produtos") or 0)

    html_t1 = (
        '<div class="torre">'
        '<div class="torre-titulo" style="color:#0D4F4A;border-color:#D1FAF6">🏪 Configuração da loja</div>'
        '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:8px 0 4px">Onboarding</div>'
        + linha("Wizard passo 1", sim_nao(w1))
        + linha("Wizard passo 2", sim_nao(w2))
        + linha("Wizard passo 3", sim_nao(w3))
        + linha("Produto cadastrado", sim_nao(tem_prod))
        + linha("Pagamento configurado", sim_nao(tem_pag))
        + linha("Frete configurado", sim_nao(tem_log))
        + '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Fretes Enviali</div>'
        + linha("Enviali ativo", sim_nao(env_ativo))
        + linha("PAC", sim_nao(pac_ativo))
        + linha("SEDEX", sim_nao(sdx_ativo))
        + linha("Jadlog", sim_nao(jdl_ativo))
        + linha("Zum/Loggi", sim_nao(zum_ativo))
        + '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Comportamento</div>'
        + linha("Produtos cadastrados", tag_val(produtos))
        + linha("Visitas (30d)", tag_val(visitas_30d))
        + linha("1ª visita", sim_nao(tem_vis))
        + linha("1ª venda", sim_nao(tem_venda))
        + '</div>'
    )
    st.markdown(html_t1, unsafe_allow_html=True)

# ── TORRE 2: MÉTRICAS E PERFORMANCE ───────────────────────────────────────────
with col_t2:
    etiq_comp  = int(env.get("etiquetas_compradas_enviali") or 0)
    etiq_post  = int(env.get("etiquetas_postadas_enviali") or 0)
    etiq_canc  = int(env.get("etiquetas_canceladas_enviali") or 0)
    etiq_pac   = int(env.get("etiquetas_compradas_pac") or 0)
    etiq_sdx   = int(env.get("etiquetas_compradas_sedex") or 0)
    etiq_jdl   = int(env.get("etiquetas_compradas_jadlog") or 0)
    gmv_env    = float(env.get("gmv") or 0)
    ped_env    = int(env.get("pedidos") or 0)
    ped_cot    = int(env.get("pedidos_cotados_enviali") or 0)

    pedidos_onb = int(onb.get("pedidos_apr") or 0)
    gmv_onb     = float(onb.get("gmv_apr") or 0)
    orig_venda  = str(onb.get("primeira_venda_origem_apr") or "—")

    bench = diag.get("benchmark", {})
    avg_dias = bench.get("avg_dias_venda", 23)
    taxa     = bench.get("taxa_conversao", 1.2)
    dias_loja= diag.get("dias_cadastro", 0) or 0
    ratio    = round(dias_loja/avg_dias,1) if avg_dias > 0 else 0
    cor_ratio= "#E24B4A" if ratio>2 else "#F59E0B" if ratio>1 else "#22C55E"

    gmv_30d_str = f"R${gmv_30d:,.2f}" if gmv_30d > 0 else "—"
    gmv_onb_str = f"R${gmv_onb:,.2f}" if gmv_onb > 0 else "—"
    seg_bench   = loja.get("segmento_loja","—")
    ratio_html  = f'<span style="color:{cor_ratio};font-weight:600">{ratio}x a média</span>'

    html_t2 = (
        '<div class="torre">'
        '<div class="torre-titulo" style="color:#534AB7;border-color:#EEEDFE">📊 Métricas e performance</div>'
        '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:8px 0 4px">Vendas</div>'
        + linha("Pedidos aprovados (30d)", tag_val(pedidos_30d))
        + linha("GMV (30d)", tag_val(gmv_30d_str))
        + linha("Pedidos total (onboarding)", tag_val(pedidos_onb))
        + linha("GMV total (onboarding)", tag_val(gmv_onb_str))
        + linha("Origem 1ª venda", tag_val(orig_venda))
        + '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Etiquetas Enviali</div>'
        + linha("Compradas (total)", tag_val(etiq_comp))
        + linha("Postadas", tag_val(etiq_post))
        + linha("Canceladas", tag_val(etiq_canc))
        + linha("PAC compradas", tag_val(etiq_pac))
        + linha("SEDEX compradas", tag_val(etiq_sdx))
        + linha("Jadlog compradas", tag_val(etiq_jdl))
        + linha("Pedidos cotados", tag_val(ped_cot))
        + f'<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Benchmark — {seg_bench}</div>'
        + linha("Avg dias para 1ª venda", tag_val(f"{avg_dias} dias"))
        + linha("Taxa de conversão", tag_val(f"{taxa}%"))
        + linha(f"Esta loja ({dias_loja} dias)", ratio_html)
        + '</div>'
    )
    st.markdown(html_t2, unsafe_allow_html=True)

# ── DIAGNÓSTICO + INSIGHTS ────────────────────────────────────────────────────

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

col_d1, col_d2 = st.columns(2)

with col_d1:
    causa = diag.get("causa_raiz","—")
    acao  = diag.get("acoes",["—"])[0]
    insights = diag.get("insights",[])
    insights_html = "".join([f'<div class="insight-item">→ {i}</div>' for i in insights]) if insights else '<div class="insight-item" style="color:#9DBDBB">Sem insights adicionais</div>'

    st.markdown(f"""
    <div class="torre">
        <div class="torre-titulo" style='color:#E24B4A;border-color:#FEF2F2'>
            🎯 Diagnóstico
        </div>
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
    if status != "LOJA ATIVA" and diag.get("email"):
        email = diag["email"]
        corpo_html = email["corpo"].replace("\n","<br>")
        st.markdown(f"""
        <div class="torre">
            <div class="torre-titulo" style='color:#1ABCB0;border-color:#D1FAF6'>
                ✉️ E-mail pronto para disparar
            </div>
            <div class="email-box">
                <div class="email-hdr">
                    <strong>Para:</strong> {loja.get("email_loja","—")} &nbsp;·&nbsp;
                    <strong>Assunto:</strong> {email["assunto"]}
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
    st.markdown(f"""
    <div style='background:#F2EDE4;border-radius:10px;padding:.8rem;
                font-size:12px;color:#5A7A78;text-align:center'>
        {datetime.now().strftime('%d/%m/%Y às %H:%M')} &nbsp;·&nbsp;
        <strong style='color:#1A2E2B'>{loja.get("nome_loja","—")}</strong>
        &nbsp;·&nbsp; Score {score}/100
    </div>""", unsafe_allow_html=True)
