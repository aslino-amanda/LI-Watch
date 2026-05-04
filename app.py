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

import json, os
from datetime import datetime as _dtnow

LOG_FILE = "uso_liwatch.json"

def registrar_uso(loja_id, nome_loja, status, score):
    try:
        log = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r") as f:
                log = json.load(f)
        log.append({"ts": _dtnow.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "loja_id": loja_id, "nome": nome_loja,
                    "status": status, "score": score})
        with open(LOG_FILE, "w") as f:
            json.dump(log[-500:], f)
    except:
        pass

def ler_metricas_uso():
    try:
        if not os.path.exists(LOG_FILE):
            return []
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    except:
        return []

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

    # mv_loja — dados gerais + pagamentos + fretes + marketplace
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
        wizard_produto,
        -- Pagamentos
        flag_ativo_pagali_cartao,
        flag_ativo_pagali_boleto,
        flag_ativo_pagali_pix,
        flag_ativo_mercadopago_cartao,
        flag_ativo_mercadopago_boleto,
        flag_ativo_pagseguro_cartao,
        flag_ativo_pagseguro_boleto,
        flag_ativo_paypal_cartao,
        flag_ativo_outros_pagamentoexterno,
        -- Fretes
        flag_ativo_enviali,
        flag_ativo_enviali_correios_pac,
        flag_ativo_enviali_correios_sedex,
        flag_ativo_enviali_jadlog,
        flag_ativo_enviali_zum_loggi,
        flag_ativo_correios_pac,
        flag_ativo_correios_sedex,
        flag_ativo_melhor_envio,
        flag_ativo_frenet,
        flag_ativo_motoboy,
        flag_ativo_retirar_pessoalmente,
        -- Marketplace
        flag_config_magalu,
        flag_enviou_produto_magalu,
        data_primeira_venda_magalu,
        qtd_pedido_magalu_ultimos_30d,
        vlr_gmv_magalu_ultimos_30d,
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

@st.cache_data(ttl=600)
def buscar_base_monitoramento():
    """Busca lojas que precisam de ação — fila de prioridades da base."""
    sql = """
    SELECT
        loja_id,
        upper(nome_loja)            AS nome_loja,
        upper(segmento_loja)        AS segmento_loja,
        upper(situacao_loja)        AS situacao_loja,
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
        coalesce(qtde_visitas_ultimos_30d, 0)                     AS qtde_visitas_ultimos_30d_clean,
        coalesce(vlr_gmv_ultimos_30d, 0)                          AS vlr_gmv_ultimos_30d_clean,
        coalesce(qtd_pedido_ultimos_30d, 0)                       AS qtd_pedido_ultimos_30d_clean
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
    ORDER BY dias_cadastro DESC
    LIMIT 200
    """
    return rodar_sql(sql)

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

col_b, col_btn, col_help = st.columns([4.5, 1, 0.7])
with col_b:
    termo = st.text_input("", placeholder="🔍  Digite o ID da loja ou nome — Ex: 123456 ou Virtual Make",
        label_visibility="collapsed")
with col_btn:
    st.button("Buscar", use_container_width=True)
with col_help:
    with st.popover("?"):
        st.markdown("**Como usar o LI Watch**")
        st.markdown("**Buscar:** ID numerico ou nome parcial da loja.")
        st.markdown("**Score de risco:** 70+ critico (hoje), 45 alto (24h), 25 medio (48h), abaixo baixo.")
        st.markdown("**Torre 1:** Configuracoes da loja — wizard, produto, pagamento, frete, Enviali.")
        st.markdown("**Torre 2:** Metricas — pedidos, GMV, etiquetas Enviali, benchmark do segmento.")
        st.markdown("**E-mail pronto:** Aparece quando ha intervencao. Copie e envie para o lojista.")
        st.markdown("**Download Excel:** Exporta o diagnostico completo.")

if not termo.strip():
    if not modo_real:
        st.markdown(
            "<div style='text-align:center;padding:3rem 0;color:#5A7A78'>"
            "<div style='font-size:18px;font-weight:600;color:#1A2E2B'>LI Watch</div>"
            "<div style='font-size:13px;margin-top:8px;color:#9DBDBB'>Conecte ao Metabase para monitorar a base · Digite um ID para diagnosticar</div>"
            "</div>", unsafe_allow_html=True)
    else:
        st.markdown("### Lojas que precisam de acao")
        st.caption("Lojas ativas dos ultimos 60 dias com algum bloqueio · ordenadas por score de risco")
        try:
            with st.spinner("Carregando base..."):
                df_base = buscar_base_monitoramento()
            if not df_base.empty:
                total = len(df_base)
                onb_n = len(df_base[df_base["status_loja"]=="ONBOARDING INCOMPLETO"])
                nv_n  = len(df_base[df_base["status_loja"]=="NUNCA VENDEU"])
                svr_n = len(df_base[df_base["status_loja"]=="SEM VENDAS RECENTES"])
                m1,m2,m3,m4 = st.columns(4)
                m1.metric("Total para acao", total)
                m2.metric("Onboarding incompleto", onb_n)
                m3.metric("Nunca vendeu", nv_n)
                m4.metric("Sem vendas recentes", svr_n)
                st.divider()
                cf1,cf2,cf3 = st.columns(3)
                with cf1:
                    f_st = st.selectbox("Status", ["Todos","ONBOARDING INCOMPLETO","NUNCA VENDEU","SEM VENDAS RECENTES"])
                with cf2:
                    f_pl = st.selectbox("Plano", ["Todos","PAGO","GRATIS"])
                with cf3:
                    _segs = ["Todos"] + sorted(df_base["segmento_loja"].dropna().unique().tolist())
                    f_sg = st.selectbox("Segmento", _segs)
                df_f = df_base.copy()
                if f_st != "Todos": df_f = df_f[df_f["status_loja"]==f_st]
                if f_pl != "Todos": df_f = df_f[df_f["status_plano"]==f_pl]
                if f_sg != "Todos": df_f = df_f[df_f["segmento_loja"]==f_sg]
                def _sc(r):
                    s=r.get("status_loja",""); d=int(r.get("dias_cadastro") or 0)
                    v=int(r.get("qtde_visitas_ultimos_30d") or 0); sc=0
                    if s=="ONBOARDING INCOMPLETO": sc=70 if d>=7 else 50
                    elif s=="NUNCA VENDEU": sc=45 if d>=20 else 25
                    elif s=="SEM VENDAS RECENTES": sc=55
                    if v>=50 and s=="NUNCA VENDEU": sc+=15
                    if str(r.get("status_plano","")).upper()=="PAGO": sc+=10
                    return min(100,sc)
                df_f["score"] = df_f.apply(_sc, axis=1)
                df_f["prio"]  = df_f["score"].apply(lambda x: "Critico" if x>=70 else "Alto" if x>=45 else "Medio" if x>=25 else "Baixo")
                df_f = df_f.sort_values("score", ascending=False)
                _cs = ["loja_id","nome_loja","segmento_loja","status_loja","score","prio","dias_cadastro","qtde_visitas_ultimos_30d_clean","vlr_gmv_ultimos_30d_clean","status_plano"]
                _ce = [c for c in _cs if c in df_f.columns]
                st.dataframe(df_f[_ce].rename(columns={
                    "loja_id":"ID","nome_loja":"Loja","segmento_loja":"Segmento",
                    "status_loja":"Status","score":"Score","prio":"Prioridade",
                    "dias_cadastro":"Dias","qtde_visitas_ultimos_30d_clean":"Visitas 30d",
                    "vlr_gmv_ultimos_30d_clean":"GMV 30d","status_plano":"Plano"
                }), use_container_width=True, hide_index=True)
                st.caption(f"{len(df_f)} lojas · Pesquise o ID acima para ver o diagnostico completo")
                st.download_button("Exportar CSV",
                    data=df_f[_ce].to_csv(index=False).encode("utf-8"),
                    file_name=f"liwatch_base_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv")
        except Exception as e:
            st.warning(f"Erro ao carregar base: {e}")
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
            # Se não tem onboarding recente, monta a partir da mv_loja
            if not onb_df.empty:
                onb = onb_df.iloc[0].to_dict()
            else:
                onb = dict(
                    flag_wizard_1=1,
                    flag_wizard_2=1,
                    flag_wizard_3=None,
                    produtos=None,
                    visitas=loja.get("qtde_visitas_ultimos_30d"),
                    pedidos_cap=loja.get("qtd_pedido_ultimos_30d"),
                    pedidos_apr=loja.get("qtd_pedido_ultimos_30d"),
                    gmv_cap=loja.get("vlr_gmv_ultimos_30d"),
                    gmv_apr=loja.get("vlr_gmv_ultimos_30d"),
                    primeira_venda_origem_cap=None,
                    data_primeira_venda_apr=loja.get("data_primeira_venda"),
                )
            # Se não tem Enviali, monta estrutura vazia
            if not env_df.empty:
                env = env_df.iloc[0].to_dict()
            else:
                env = dict(
                    flag_ativacao_enviali=0,flag_ativacao_pac=0,
                    flag_ativacao_sedex=0,flag_ativacao_jadlog=0,
                    flag_ativacao_zum_loggi=0,
                    etiquetas_compradas_enviali=None,etiquetas_postadas_enviali=None,
                    etiquetas_canceladas_enviali=None,
                    etiquetas_compradas_pac=None,etiquetas_compradas_sedex=None,
                    etiquetas_compradas_jadlog=None,
                    pedidos_cotados_enviali=None,gmv=None,pedidos=None,
                )

    except Exception as e:
        st.warning(f"Erro na conexão: {e}. Usando dados simulados.")
        modo_real = False

if not modo_real or loja is None:
    loja_id = int(termo_clean) if termo_clean.isdigit() else 421
    loja = demo_loja(loja_id)
    onb  = demo_onb(loja_id)
    env  = demo_env(loja_id)

# ── DIAGNÓSTICO ───────────────────────────────────────────────────────────────

# ── Motor de diagnóstico inline (não depende de arquivo externo) ──────────────

BENCHMARK_INLINE = {
    "MODA E ACESSÓRIOS":                           {"avg_dias_venda": 24.9, "taxa_conversao": 1.1},
    "COSMÉTICOS, PERFUMARIA E CUIDADOS PESSOAIS":  {"avg_dias_venda": 22.4, "taxa_conversao": 1.3},
    "ALIMENTOS E BEBIDAS":                         {"avg_dias_venda": 23.3, "taxa_conversao": 1.2},
    "ELETRÔNICOS":                                 {"avg_dias_venda": 25.2, "taxa_conversao": 1.1},
    "FITNESS E SUPLEMENTOS":                       {"avg_dias_venda": 12.8, "taxa_conversao": 1.8},
    "ESPORTE E LAZER":                             {"avg_dias_venda": 26.8, "taxa_conversao": 1.7},
    "CASA E DECORAÇÃO":                            {"avg_dias_venda": 31.5, "taxa_conversao": 1.2},
    "INFORMÁTICA":                                 {"avg_dias_venda": 16.3, "taxa_conversao": 1.7},
    "ARTESANATO":                                  {"avg_dias_venda": 22.3, "taxa_conversao": 1.1},
    "SAÚDE":                                       {"avg_dias_venda": 50.5, "taxa_conversao": 0.9},
    "DEFAULT":                                     {"avg_dias_venda": 23.0, "taxa_conversao": 1.2},
}

EMAILS_INLINE = {
    "ONBOARDING INCOMPLETO": {
        "assunto": "Sua loja está quase pronta — falta pouco para a primeira venda",
        "corpo": "Olá, {nome}!\n\nIdentificamos que sua loja ainda precisa de algumas configurações para estar pronta para vender.\n\n{itens}\n\n— Time Loja Integrada",
        "metrica": "Config completa em 24h após contato"
    },
    "NUNCA VENDEU": {
        "assunto": "Sua loja está configurada — veja como atrair os primeiros compradores",
        "corpo": "Olá, {nome}!\n\nSua loja está configurada e pronta. Agora é hora de atrair visitantes e converter em vendas.\n\n→ Compartilhe o link da sua loja no WhatsApp\n→ Poste fotos dos produtos no Instagram\n→ Peça indicações para amigos e familiares\n\n— Time Loja Integrada",
        "metrica": "1ª venda em até 15 dias após contato"
    },
    "SEM VENDAS RECENTES": {
        "assunto": "Sua loja ficou um tempo sem vendas — veja como reativar",
        "corpo": "Olá, {nome}!\n\nNotamos que sua loja está há alguns dias sem registrar vendas.\n\n→ Verifique se os produtos estão com estoque\n→ Atualize fotos e descrições\n→ Considere uma promoção ou frete grátis\n\n— Time Loja Integrada",
        "metrica": "Retorno de vendas em até 30 dias após contato"
    },
}

def _diagnostico_inline(loja):
    status = str(loja.get("status_loja","")).upper().strip()
    seg    = str(loja.get("segmento_loja","")).upper().strip()
    bench  = BENCHMARK_INLINE.get(seg, BENCHMARK_INLINE["DEFAULT"])
    origem = "PAGO" if loja.get("aquisicao_utm_source") else "ORGÂNICO"

    from datetime import date, datetime as _dt
    def _dias(d):
        if not d or str(d) in ("None","nan",""): return 0
        try:
            dt = _dt.strptime(str(d)[:10], "%Y-%m-%d").date()
            return (date.today() - dt).days
        except: return 0

    dias = _dias(loja.get("data_cadastro_loja"))
    avg  = bench["avg_dias_venda"] or 23
    ratio = round(dias / avg, 1) if avg > 0 and dias > 0 else 0.0

    tem_prod = loja.get("data_primeira_config_produto") not in (None,"","None")
    tem_pag  = loja.get("data_primeira_config_pagamento") not in (None,"","None")
    tem_log  = loja.get("data_primeira_config_logistica") not in (None,"","None")
    visitas  = int(loja.get("qtde_visitas_ultimos_30d") or 0)
    gmv      = float(loja.get("vlr_gmv_ultimos_30d") or 0)

    # Score
    score = 0
    causas = []
    insights = []
    acoes = []

    if status == "ONBOARDING INCOMPLETO":
        score = 70 if dias >= 7 else 50
        if tem_prod and not tem_pag:
            causas.append("Configurou produto mas NÃO ativou pagamento")
            insights.append("Gargalo crítico — sem pagamento nenhum pedido pode ser finalizado.")
            acoes.append("Ativar Pagali — 5 minutos para destravar vendas")
        elif tem_prod and tem_pag and not tem_log:
            causas.append("Pagamento ativo mas frete NÃO configurado")
            insights.append("Checkout trava na etapa de entrega. Ativar Enviali resolve.")
            acoes.append("Configurar Enviali — último passo para a primeira venda")
        else:
            causas.append("Configurações básicas incompletas")
            acoes.append("Completar checklist de configuração da loja")
        if origem == "PAGO":
            score += 10
            insights.append("Loja veio de canal pago — custo de aquisição em risco.")

    elif status == "NUNCA VENDEU":
        score = 40 if ratio >= 2 else 25
        if ratio >= 2:
            causas.append(f"Há {dias} dias sem vender — {ratio}x acima da média de {seg} ({avg} dias)")
            insights.append(f"Segmento {seg} leva em média {avg} dias para 1ª venda. Esta loja está {ratio}x além.")
        else:
            causas.append(f"Dentro da janela esperada ({dias}/{avg} dias)")
        if visitas >= 50:
            score += 15
            insights.append(f"{visitas} visitas mas zero vendas — problema de conversão (fotos, preço ou descrição).")
            acoes.append("E-mail com dicas de conversão — fotos, preço e descrição")
        elif visitas == 0:
            score += 10
            insights.append("Zero visitas — loja configurada mas invisível. Divulgação é o próximo passo.")
            acoes.append("E-mail com checklist de divulgação — WhatsApp e Instagram")
        else:
            acoes.append("E-mail com estratégias para primeira venda")

    elif status == "SEM VENDAS RECENTES":
        score = 55
        causas.append("Loja que vendia entrou em inatividade")
        if gmv == 0 and visitas == 0:
            score = 70
            insights.append("Zero pedidos e zero visitas nos últimos 30 dias — parada total.")
            acoes.append("CS verificar urgente: loja acessível e produtos ativos")
        elif gmv == 0:
            insights.append("Visitas existem mas sem conversão — verificar estoque e preços.")
            acoes.append("E-mail de reativação com dicas de conversão")
        else:
            acoes.append("Monitorar evolução nos próximos 7 dias")

    else:  # LOJA ATIVA
        score = 5
        causas.append("Loja ativa e saudável")
        insights.append(f"{visitas} visitas e R${gmv:,.2f} GMV nos últimos 30 dias.")
        acoes.append("Monitorar normalmente")

    score = max(0, min(100, score))

    if score >= 70:   prio = "🔴 CRÍTICA"; sla = "Intervir hoje"; canal = "E-mail imediato + alerta CS"
    elif score >= 45: prio = "🟠 ALTA";    sla = "Intervir em 24h"; canal = "E-mail automático"
    elif score >= 25: prio = "🟡 MÉDIA";   sla = "Intervir em 48h"; canal = "E-mail automático"
    else:             prio = "🟢 BAIXA";   sla = "Monitorar";       canal = "Sem ação necessária"

    # Monta email
    email_tpl = EMAILS_INLINE.get(status, {"assunto":"Acompanhamento da sua loja","corpo":"Olá!\n\n— Time LI","metrica":"Acompanhamento"})
    itens = []
    if not tem_prod: itens.append("→ Cadastrar pelo menos 1 produto")
    if not tem_pag:  itens.append("→ Configurar pagamento (Pagali)")
    if not tem_log:  itens.append("→ Configurar frete (Enviali)")
    corpo = email_tpl["corpo"].format(nome=loja.get("nome_loja","Lojista"), itens="\n".join(itens) if itens else "→ Revisar configurações gerais")

    return {
        "score_risco":  score,
        "prioridade":   prio,
        "sla":          sla,
        "canal":        canal,
        "causa_raiz":   " | ".join(causas) if causas else status,
        "insights":     insights,
        "acoes":        acoes if acoes else ["Monitorar normalmente"],
        "benchmark":    bench,
        "dias_cadastro": dias,
        "email":        {"assunto": email_tpl["assunto"], "corpo": corpo, "metrica_impacto": email_tpl["metrica"]},
    }

# Roda diagnóstico — engine externo ou inline
if ENGINE_OK:
    try:
        diag = diagnosticar_loja(loja)
        if "score_risco" not in diag:
            diag = _diagnostico_inline(loja)
    except:
        diag = _diagnostico_inline(loja)
else:
    diag = _diagnostico_inline(loja)

score      = diag["score_risco"]
prioridade = diag["prioridade"]
status     = str(loja.get("status_loja","")).upper()
registrar_uso(loja.get("loja_id"), loja.get("nome_loja"), status, score)

# ── DETECÇÃO AUTOMÁTICA DE PERFIL ─────────────────────────────────────────────
from datetime import date as _date, datetime as _dtcheck
def _dias_loja(d):
    if not d or str(d) in ("None","nan",""): return 999
    try: return (_date.today() - _dtcheck.strptime(str(d)[:10],"%Y-%m-%d").date()).days
    except: return 999

dias_desde_cadastro = _dias_loja(loja.get("data_cadastro_loja"))
gmv_30d_check = float(loja.get("vlr_gmv_ultimos_30d") or 0)
pedidos_30d_check = int(loja.get("qtd_pedido_ultimos_30d") or 0)

# Perfil: NOVO (<90 dias) ou CASA (>=90 dias)
if dias_desde_cadastro < 90:
    perfil_loja = "NOVO"
    perfil_label = "Novo lojista"
    perfil_desc = f"Cadastrado ha {dias_desde_cadastro} dias — foco em ativacao"
    perfil_cor = "#1ABCB0"
    perfil_bg = "#D1FAF6"
elif gmv_30d_check > 0 or pedidos_30d_check > 0:
    perfil_loja = "CASA_ATIVO"
    perfil_label = "Cliente da casa — ativo"
    perfil_desc = f"{pedidos_30d_check} pedidos e R${gmv_30d_check:,.0f} GMV nos ultimos 30 dias"
    perfil_cor = "#22C55E"
    perfil_bg = "#F0FDF4"
elif status == "SEM VENDAS RECENTES":
    perfil_loja = "CASA_RISCO"
    perfil_label = "Cliente da casa — em risco"
    perfil_desc = f"Loja estabelecida ({dias_desde_cadastro} dias) com queda de faturamento"
    perfil_cor = "#F59E0B"
    perfil_bg = "#FFFBEB"
else:
    perfil_loja = "CASA_INATIVO"
    perfil_label = "Cliente da casa — inativo"
    perfil_desc = f"Loja com {dias_desde_cadastro} dias sem vendas recentes"
    perfil_cor = "#E24B4A"
    perfil_bg = "#FEF2F2"

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
    _mrr_str = f"— R${mrr}/mes" if float(mrr or 0) > 0 else ""
    _nome    = str(loja.get("nome_loja","—"))
    _loja_id = str(loja.get("loja_id","—"))
    _seg     = str(loja.get("segmento_loja","—"))
    _cid     = str(loja.get("cidade","—"))
    _est     = str(loja.get("estado","—"))
    _plano   = str(loja.get("status_plano","—"))
    _orig    = str(loja.get("origem","—"))
    _email   = str(loja.get("email_loja","—"))
    _dom     = str(loja.get("dominio_loja","—"))
    st.markdown(
        "<div style='background:white;border-radius:14px;padding:1.2rem;margin-bottom:1rem'>"
        "<div style='display:flex;align-items:center;gap:10px;margin-bottom:4px'>"
        "<span style='font-size:20px;font-weight:700;color:#1A2E2B'>" + _nome + "</span>"
        "<span style='background:" + perfil_bg + ";color:" + perfil_cor + ";font-size:11px;"
        "font-weight:700;padding:3px 10px;border-radius:20px;white-space:nowrap'>"
        + perfil_label + "</span>"
        "</div>"
        "<div style='font-size:12px;color:" + perfil_cor + ";margin-bottom:6px'>" + perfil_desc + "</div>"
        "<div style='font-size:13px;color:#5A7A78'>"
        "ID: <strong>" + _loja_id + "</strong> &nbsp;·&nbsp; " + _seg +
        " &nbsp;·&nbsp; " + _cid + "/" + _est +
        " &nbsp;·&nbsp; Plano: <strong>" + _plano + " " + _mrr_str + "</strong>"
        " &nbsp;·&nbsp; Origem: <strong>" + _orig + "</strong>"
        "</div>"
        "<div style='font-size:13px;color:#5A7A78;margin-top:2px'>"
        + _email + " &nbsp;·&nbsp; " + _dom +
        "</div></div>",
        unsafe_allow_html=True
    )

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

    # Wizard — se configurada considera completo
    loja_configurada = tem_prod and tem_pag and tem_log
    w1 = int(onb.get("flag_wizard_1") or (1 if loja_configurada else 0))
    w2 = int(onb.get("flag_wizard_2") or (1 if loja_configurada else 0))
    w3_raw = onb.get("flag_wizard_3")
    w3 = int(w3_raw) if w3_raw is not None and str(w3_raw) not in ("None","nan","") else (1 if loja_configurada else 0)

    # Produtos — tenta wizard_produto da mv_loja, depois onboarding
    _prod_raw = loja.get("wizard_produto")
    if _prod_raw is None or str(_prod_raw) in ("None","nan",""):
        _prod_raw = onb.get("produtos")
    try:
        produtos = int(float(_prod_raw)) if _prod_raw is not None and str(_prod_raw) not in ("None","nan","") else None
        if produtos == 0:
            produtos = None  # 0 significa sem dado, não zero produtos
    except:
        produtos = None

    # Fretes — usa mv_loja diretamente (mais completo)
    def _flag(campo): return int(loja.get(campo) or env.get(campo.replace("flag_ativo_","flag_ativacao_").replace("enviali_correios_","").replace("enviali_","")) or 0)
    env_ativo  = int(loja.get("flag_ativo_enviali") or env.get("flag_ativacao_enviali") or 0)
    pac_ativo  = int(loja.get("flag_ativo_enviali_correios_pac") or loja.get("flag_ativo_correios_pac") or env.get("flag_ativacao_pac") or 0)
    sdx_ativo  = int(loja.get("flag_ativo_enviali_correios_sedex") or loja.get("flag_ativo_correios_sedex") or env.get("flag_ativacao_sedex") or 0)
    jdl_ativo  = int(loja.get("flag_ativo_enviali_jadlog") or env.get("flag_ativacao_jadlog") or 0)
    zum_ativo  = int(loja.get("flag_ativo_enviali_zum_loggi") or env.get("flag_ativacao_zum_loggi") or 0)
    melhor_envio = int(loja.get("flag_ativo_melhor_envio") or 0)
    frenet     = int(loja.get("flag_ativo_frenet") or 0)
    motoboy    = int(loja.get("flag_ativo_motoboy") or 0)
    retirada   = int(loja.get("flag_ativo_retirar_pessoalmente") or 0)

    # Pagamentos da mv_loja
    pag_pagali_cartao = int(loja.get("flag_ativo_pagali_cartao") or 0)
    pag_pagali_pix    = int(loja.get("flag_ativo_pagali_pix") or 0)
    pag_pagali_boleto = int(loja.get("flag_ativo_pagali_boleto") or 0)
    pag_mp_cartao     = int(loja.get("flag_ativo_mercadopago_cartao") or 0)
    pag_mp_boleto     = int(loja.get("flag_ativo_mercadopago_boleto") or 0)
    pag_pags_cartao   = int(loja.get("flag_ativo_pagseguro_cartao") or 0)
    pag_externo       = int(loja.get("flag_ativo_outros_pagamentoexterno") or 0)
    algum_pagamento   = any([pag_pagali_cartao, pag_pagali_pix, pag_pagali_boleto,
                             pag_mp_cartao, pag_mp_boleto, pag_pags_cartao, pag_externo])

    # Marketplace
    magalu_config = int(loja.get("flag_config_magalu") or 0)
    magalu_venda  = int(loja.get("flag_enviou_produto_magalu") or 0)
    gmv_magalu    = float(loja.get("vlr_gmv_magalu_ultimos_30d") or 0)
    ped_magalu    = int(loja.get("qtd_pedido_magalu_ultimos_30d") or 0)

    gmv_30d    = float(loja.get("vlr_gmv_ultimos_30d") or 0)
    pedidos_30d= int(loja.get("qtd_pedido_ultimos_30d") or 0)
    visitas_30d= int(loja.get("qtde_visitas_ultimos_30d") or 0)

    # Monta seção de pagamentos
    pag_html = ""
    if pag_pagali_cartao or pag_pagali_pix or pag_pagali_boleto:
        modos = []
        if pag_pagali_cartao: modos.append("Cartão")
        if pag_pagali_pix:    modos.append("Pix")
        if pag_pagali_boleto: modos.append("Boleto")
        pag_html += linha("Pagali", tag_val(", ".join(modos)))
    if pag_mp_cartao or pag_mp_boleto:
        pag_html += linha("Mercado Pago", sim_nao(1))
    if pag_pags_cartao:
        pag_html += linha("PagSeguro", sim_nao(1))
    if pag_externo:
        pag_html += linha("Pagamento externo", sim_nao(1))
    if not pag_html:
        pag_html = linha("Nenhum pagamento ativo", sim_nao(0))

    # Monta seção de fretes
    frete_html = ""
    if env_ativo:
        frs = []
        if pac_ativo: frs.append("PAC")
        if sdx_ativo: frs.append("SEDEX")
        if jdl_ativo: frs.append("Jadlog")
        if zum_ativo: frs.append("Zum")
        frete_html += linha("Enviali", tag_val(", ".join(frs) if frs else "Ativo"))
    if melhor_envio: frete_html += linha("Melhor Envio", sim_nao(1))
    if frenet:       frete_html += linha("Frenet", sim_nao(1))
    if pac_ativo and not env_ativo: frete_html += linha("Correios PAC", sim_nao(1))
    if sdx_ativo and not env_ativo: frete_html += linha("Correios SEDEX", sim_nao(1))
    if motoboy:      frete_html += linha("Motoboy", sim_nao(1))
    if retirada:     frete_html += linha("Retirada pessoal", sim_nao(1))
    if not frete_html:
        frete_html = linha("Nenhum frete ativo", sim_nao(0))

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
        + linha("Produtos cadastrados", tag_val(produtos) if produtos is not None else "<span style=\'color:#9DBDBB;font-size:12px\'>Nao disponivel</span>")
        + linha("Visitas (30d)", tag_val(visitas_30d))
        + linha("1a visita", sim_nao(tem_vis))
        + linha("1a venda", sim_nao(tem_venda))
        + (
            '<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Marketplace</div>'
            + linha("Magalu configurado", sim_nao(magalu_config))
            + linha("Produto enviado Magalu", sim_nao(magalu_venda))
            + linha("GMV Magalu (30d)", tag_val(f"R${gmv_magalu:,.2f}") if gmv_magalu > 0 else tag_val("—"))
            + linha("Pedidos Magalu (30d)", tag_val(ped_magalu) if ped_magalu > 0 else tag_val("—"))
            if magalu_config else ""
        )
        + "</div>"
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
        + f'<div style="font-size:11px;font-weight:600;color:#9DBDBB;text-transform:uppercase;letter-spacing:.06em;margin:12px 0 4px">Benchmark — {seg_bench}</div>'
        + linha("Avg dias para 1ª venda", tag_val(f"{avg_dias} dias"))
        + linha("Taxa de conversão", tag_val(f"{taxa}%"))
        + linha(f"Esta loja ({dias_loja} dias)", ratio_html)
        + '</div>'
    )
    st.markdown(html_t2, unsafe_allow_html=True)

# ── DIAGNÓSTICO ESPECÍFICO POR PERFIL ────────────────────────────────────────

if perfil_loja in ("CASA_RISCO", "CASA_INATIVO"):
    st.markdown(
        "<div style='background:#0D4F4A;border-radius:12px;padding:1rem 1.2rem;margin-bottom:.8rem'>"
        "<div style='font-size:11px;color:#1ABCB0;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px'>Cliente da casa — diagnostico de queda</div>"
        "<div style='font-size:13px;color:#D1FAF6;line-height:1.6'>"
        "Esta loja tem historico de vendas mas esta em queda ou inativa. "
        "Para um diagnostico completo de causa raiz (ticket, recorrencia, cupons, mix de pagamento), "
        "use as queries de diagnostico de queda com o conta_id: <strong style='color:#D4F53C'>"
        + str(loja.get("loja_id","—")) + "</strong>"
        "</div></div>",
        unsafe_allow_html=True
    )

# ── OPORTUNIDADES DE PRODUTO NATIVO ──────────────────────────────────────────

_ops = []
if not env_ativo and tem_log:
    _ops.append(("Enviali nao ativado",
        "Loja com frete configurado mas sem Enviali. Ativar reduz custo e aumenta opcoes de entrega.",
        "#FFFBEB","#FDE68A","#92400E"))
_gmv_c = float(loja.get("vlr_gmv_ultimos_30d") or 0)
_plano_c = str(loja.get("status_plano","")).upper()
if _plano_c == "GRATIS" and not tem_pag:
    _ops.append(("Pagali nao configurado",
        "Plano gratis sem pagamento. Ativar o Pagali e o passo critico para a primeira venda.",
        "#F0F9FF","#BAE6FD","#0369A1"))
if _plano_c == "GRATIS" and _gmv_c > 1000:
    _ops.append((f"Upgrade — R${_gmv_c:,.0f} GMV no plano gratis",
        "Loja gerando receita no plano gratis. Upgrade desbloquearia mais produtos e visitas.",
        "#F0FDF4","#86EFAC","#166534"))

if _ops:
    st.markdown("<div style='font-size:14px;font-weight:600;color:#1A2E2B;margin:.8rem 0 .4rem'>Oportunidades de produto nativo</div>", unsafe_allow_html=True)
    _cols_op = st.columns(len(_ops))
    for _i, (_t, _d, _bg, _bd, _tc) in enumerate(_ops):
        with _cols_op[_i]:
            st.markdown(
                "<div style='background:" + _bg + ";border:1px solid " + _bd + ";border-left:4px solid " + _bd + ";border-radius:10px;padding:1rem'>"
                "<div style='font-size:13px;font-weight:700;color:" + _tc + ";margin-bottom:6px'>" + _t + "</div>"
                "<div style='font-size:12px;color:#5A7A78;line-height:1.6'>" + _d + "</div></div>",
                unsafe_allow_html=True)

# ── DIAGNÓSTICO + INSIGHTS ────────────────────────────────────────────────────

st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

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


# ── ROADMAP ───────────────────────────────────────────────────────────────────

st.divider()
with st.expander("Roadmap — próximos passos do LI Watch", expanded=False):
    col_r1, col_r2, col_r3 = st.columns(3)
    with col_r1:
        st.markdown("""<div style='background:#F0FDF4;border:1px solid #86EFAC;border-radius:12px;padding:1.2rem'>
<div style='font-size:11px;font-weight:700;color:#166534;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px'>FASE 1 — CONCLUÍDA</div>
<div style='font-size:14px;font-weight:700;color:#1A2E2B;margin-bottom:8px'>Diagnóstico Individual</div>
<div style='font-size:12px;color:#5A7A78;line-height:1.7'>
✓ Busca por ID ou nome<br>✓ Torres configuração + métricas<br>✓ Score de risco com dados reais<br>✓ Benchmark por segmento<br>✓ Conexão Metabase ao vivo<br>✓ Download em Excel
</div></div>""", unsafe_allow_html=True)
    with col_r2:
        st.markdown("""<div style='background:#FFFBEB;border:1px solid #FDE68A;border-radius:12px;padding:1.2rem'>
<div style='font-size:11px;font-weight:700;color:#92400E;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px'>FASE 2 — EM CONSTRUÇÃO</div>
<div style='font-size:14px;font-weight:700;color:#1A2E2B;margin-bottom:8px'>Automação de Intervenção</div>
<div style='font-size:12px;color:#5A7A78;line-height:1.7'>
⏳ Pipeline automático diário (8h)<br>⏳ Integração HubSpot<br>⏳ Disparo de e-mail por gargalo<br>⏳ Log de intervenções<br>⏳ Filtro anti-spam (3 dias)<br>⏳ Métricas de impacto pós-envio
</div></div>""", unsafe_allow_html=True)
    with col_r3:
        st.markdown("""<div style='background:#EEEDFE;border:1px solid #C4B5FD;border-radius:12px;padding:1.2rem'>
<div style='font-size:11px;font-weight:700;color:#3C3489;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px'>FASE 3 — VISÃO</div>
<div style='font-size:14px;font-weight:700;color:#1A2E2B;margin-bottom:8px'>IA Generativa</div>
<div style='font-size:12px;color:#5A7A78;line-height:1.7'>
💡 LLM analisa contexto da loja<br>💡 Diagnóstico em linguagem natural<br>💡 E-mail gerado por IA por lojista<br>💡 Detecção de padrões de churn<br>💡 Recomendações proativas<br>💡 Chat com dados do lojista
</div></div>""", unsafe_allow_html=True)
    st.markdown("""<div style='background:#0D4F4A;border-radius:10px;padding:1rem 1.2rem;margin-top:1rem;font-size:13px;color:#9DCFCC;line-height:1.6'>
<strong style='color:#D4F53C'>North Star 2026:</strong> % de novos lojistas com 5 pedidos em até 15 dias.
O LI Watch é a ferramenta que viabiliza essa meta — do diagnóstico individual à automação em escala, sem dependência técnica do CS.
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
    _ts = datetime.now().strftime("%d/%m/%Y as %H:%M")
    _nm = str(loja.get("nome_loja","—"))
    st.markdown(
        "<div style='background:#F2EDE4;border-radius:10px;padding:.8rem;font-size:12px;color:#5A7A78;text-align:center'>"
        + _ts + " &nbsp;·&nbsp; <strong style='color:#1A2E2B'>" + _nm + "</strong>"
        + " &nbsp;·&nbsp; Score " + str(score) + "/100"
        + "<br><span style='font-size:11px;color:#9DBDBB'>" + str(_n_diag) + " diagnostico(s) realizados com o LI Watch</span>"
        + "</div>",
        unsafe_allow_html=True)
