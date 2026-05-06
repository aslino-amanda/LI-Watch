import streamlit as st
import requests
import pandas as pd
from datetime import datetime, date
from diagnostico_engine import diagnosticar_dataframe, diagnosticar_loja
from alertas import exibir_alertas
from diagnostico_queda import (
    diagnosticar_queda_batch,
    formatar_causa_curta,
    formatar_variacao_gmv,
)
import metabase_connector as mb

# ── CONFIGURAÇÃO ──────────────────────────────────────────────────────────────

METABASE_URL    = st.secrets["metabase"]["url"]
METABASE_APIKEY = st.secrets["metabase"]["api_key"]
METABASE_DB_ID  = int(st.secrets["metabase"]["db_id"])
HUBSPOT_TOKEN   = st.secrets.get("hubspot", {}).get("token", "")

HEADERS_MB = {"x-api-key": METABASE_APIKEY, "Content-Type": "application/json"}
HEADERS_HS = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

# ── EMAILS POR STATUS ─────────────────────────────────────────────────────────

EMAILS = {
    "ONBOARDING INCOMPLETO": {
        "assunto": "Sua loja está quase pronta — falta pouco para começar a vender!",
        "corpo": """Olá, {nome_loja}!

Identificamos que sua loja ainda precisa de algumas configurações para estar pronta para vender.

O que está faltando:
{itens_faltando}

Cada item leva poucos minutos para configurar. Assim que terminar, sua loja estará pronta para receber os primeiros pedidos!

Acesse sua loja e finalize a configuração agora.

— Time Loja Integrada"""
    },
    "NUNCA VENDEU": {
        "assunto": "Sua loja está configurada — veja como atrair os primeiros compradores",
        "corpo": """Olá, {nome_loja}!

Sua loja está configurada e pronta para vender. Parabéns pela dedicação!

Agora o próximo passo é atrair os primeiros visitantes e converter em vendas.

Dicas para sua primeira venda:
→ Compartilhe o link da sua loja no WhatsApp e Instagram
→ Poste fotos dos seus produtos nas redes sociais
→ Considere anunciar no Mercado Livre e Shopee para mais alcance
→ Peça para amigos e familiares divulgarem

As primeiras vendas geralmente vêm da sua própria rede de contatos. Comece por aí!

— Time Loja Integrada"""
    },
    "SEM VENDAS RECENTES": {
        "assunto": "Sua loja ficou um tempo sem vendas — veja como reativar",
        "corpo": """Olá, {nome_loja}!

Notamos que sua loja está há alguns dias sem registrar vendas. Isso pode acontecer por alguns motivos comuns:

O que pode estar acontecendo:
→ Produtos desatualizados ou sem estoque
→ Falta de divulgação recente
→ Preços fora do mercado

O que fazer agora:
→ Atualize fotos e descrições dos produtos
→ Verifique se os preços estão competitivos
→ Faça uma postagem nas redes sociais hoje
→ Considere criar uma promoção ou frete grátis

Pequenas ações hoje podem reativar suas vendas rapidamente!

— Time Loja Integrada"""
    }
}

# ── METABASE — BUSCA LOJAS ────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def buscar_lojas_para_intervencao():
    """
    Busca todas as lojas ativas criadas nos últimos 30 dias
    que precisam de intervenção (status != LOJA ATIVA).
    """
    sql = """
    SELECT
        loja_id,
        upper(nome_loja)          AS nome_loja,
        dominio_loja,
        email_loja,
        upper(segmento_loja)      AS segmento_loja,
        upper(tipo_pessoa_loja)   AS tipo_pessoa_loja,
        upper(situacao_loja)      AS situacao_loja,
        data_cadastro_loja,
        upper(cidade_endereco_loja) AS cidade,
        upper(estado_endereco_loja) AS estado,
        aquisicao_utm_source,
        CASE
            WHEN aquisicao_utm_source IS NULL THEN 'ORGANICO'
            ELSE 'PAGO'
        END AS origem,
        data_primeira_config_pagamento,
        data_primeira_config_logistica,
        data_primeira_config_produto,
        CASE
            WHEN data_primeira_config_pagamento IS NOT NULL
             AND data_primeira_config_logistica IS NOT NULL
             AND data_primeira_config_produto   IS NOT NULL
            THEN 'CONFIGURADO'
            ELSE 'NAO CONFIGURADO'
        END AS status_config,
        upper(tipo_plano_atual)   AS tipo_plano,
        vlr_plano_mrr_atual,
        CASE
            WHEN data_ini_plano_atual IS NOT NULL THEN 'PAGO'
            ELSE 'GRATIS'
        END AS status_plano,
        data_primeira_visita,
        qtde_visitas_ultimos_30d,
        data_primeira_venda,
        qtd_pedido_ultimos_30d,
        vlr_gmv_ultimos_30d,
        CASE
            WHEN data_primeira_config_pagamento IS NULL
              OR data_primeira_config_logistica IS NULL
              OR data_primeira_config_produto   IS NULL
            THEN 'ONBOARDING INCOMPLETO'
            WHEN data_primeira_venda IS NULL
            THEN 'NUNCA VENDEU'
            WHEN coalesce(vlr_gmv_ultimos_30d, 0) = 0
            THEN 'SEM VENDAS RECENTES'
            ELSE 'LOJA ATIVA'
        END AS status_loja
    FROM analytics_manual.mv_loja
    WHERE situacao_loja = 'ativa'
      AND data_cadastro_loja >= current_date - interval '30' day
      AND (
            data_primeira_config_pagamento IS NULL
         OR data_primeira_config_logistica IS NULL
         OR data_primeira_config_produto   IS NULL
         OR data_primeira_venda            IS NULL
         OR coalesce(vlr_gmv_ultimos_30d, 0) = 0
      )
    ORDER BY data_cadastro_loja DESC
    """

    payload = {
        "database": METABASE_DB_ID,
        "type": "native",
        "native": {"query": sql}
    }

    import time
    r = requests.post(
        f"{METABASE_URL}/api/dataset",
        headers=HEADERS_MB,
        json=payload,
        timeout=120
    )

    # Metabase 202 = query assíncrona, faz polling
    if r.status_code == 202:
        data = r.json()
        job_id = data.get("id")
        if not job_id:
            raise Exception(f"Metabase 202 sem job_id: {r.text[:200]}")
        for _ in range(30):
            time.sleep(2)
            poll = requests.get(
                f"{METABASE_URL}/api/dataset/{job_id}",
                headers=HEADERS_MB,
                timeout=30,
            )
            if poll.status_code == 200:
                data = poll.json()
                break
            if poll.status_code != 202:
                raise Exception(f"Polling falhou {poll.status_code}: {poll.text[:200]}")
        else:
            raise Exception("Timeout: Metabase nao respondeu em 60s")
    elif r.status_code != 200:
        raise Exception(f"Erro Metabase {r.status_code}: {r.text[:200]}")
    else:
        data = r.json()

    if "error" in data:
        raise Exception(data["error"])

    cols = [c["name"] for c in data["data"]["cols"]]
    return pd.DataFrame(data["data"]["rows"], columns=cols)


# ── HUBSPOT — CRIAR CONTATO E DISPARAR EMAIL ──────────────────────────────────

def criar_ou_atualizar_contato_hubspot(loja: dict) -> str:
    """
    Cria ou atualiza o contato no HubSpot.
    Retorna o contact_id.
    """
    email = loja.get("email_loja", "")
    if not email:
        raise Exception(f"Loja {loja['loja_id']} sem e-mail cadastrado")

    payload = {
        "properties": {
            "email":       email,
            "firstname":   str(loja.get("nome_loja", "")),
            "company":     str(loja.get("nome_loja", "")),
            "website":     str(loja.get("dominio_loja", "")),
            "city":        str(loja.get("cidade", "")),
            "state":       str(loja.get("estado", "")),
            "li_loja_id":  str(loja.get("loja_id", "")),
            "li_status_loja": str(loja.get("status_loja", "")),
            "li_segmento": str(loja.get("segmento_loja", "")),
            "li_plano":    str(loja.get("status_plano", "")),
        }
    }

    # Tenta criar — se já existe (409), faz upsert
    r = requests.post(
        "https://api.hubapi.com/crm/v3/objects/contacts",
        headers=HEADERS_HS,
        json=payload
    )

    if r.status_code == 409:
        # Contato já existe — busca pelo e-mail e atualiza
        search = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts/search",
            headers=HEADERS_HS,
            json={"filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}]}
        )
        contact_id = search.json()["results"][0]["id"]
        requests.patch(
            f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}",
            headers=HEADERS_HS,
            json=payload
        )
        return contact_id

    elif r.status_code == 201:
        return r.json()["id"]
    else:
        raise Exception(f"Erro HubSpot {r.status_code}: {r.text[:200]}")


def montar_itens_faltando(loja: dict) -> str:
    """Monta a lista de itens faltando para o e-mail de onboarding incompleto."""
    itens = []
    if not loja.get("data_primeira_config_pagamento"):
        itens.append("→ Configurar forma de pagamento (Pagali)")
    if not loja.get("data_primeira_config_logistica"):
        itens.append("→ Configurar opção de frete (Enviali)")
    if not loja.get("data_primeira_config_produto"):
        itens.append("→ Cadastrar pelo menos 1 produto")
    return "\n".join(itens) if itens else "→ Verificar configurações da loja"


def disparar_email_hubspot(contact_id: str, loja: dict, status: str) -> bool:
    """
    Registra a tarefa de e-mail no HubSpot como engajamento.
    Em produção, substituir pelo ID do template de e-mail do HubSpot.
    """
    email_info = EMAILS.get(status)
    if not email_info:
        return False

    corpo = email_info["corpo"].format(
        nome_loja=loja.get("nome_loja", "Lojista"),
        itens_faltando=montar_itens_faltando(loja)
    )

    # Registra como nota/atividade no contato
    payload = {
        "engagement": {"active": True, "type": "NOTE"},
        "associations": {"contactIds": [int(contact_id)]},
        "metadata": {
            "body": f"[LI Watch] E-mail agendado\n\nAssunto: {email_info['assunto']}\n\n{corpo}"
        }
    }

    r = requests.post(
        "https://api.hubapi.com/engagements/v1/engagements",
        headers=HEADERS_HS,
        json=payload
    )

    return r.status_code in [200, 201]


# ── PIPELINE PRINCIPAL ────────────────────────────────────────────────────────

def rodar_pipeline(df: pd.DataFrame, dry_run: bool = True) -> pd.DataFrame:
    """
    Roda o pipeline de intervenção para todas as lojas do DataFrame.
    dry_run=True → só simula, não dispara de verdade.
    dry_run=False → dispara de verdade no HubSpot.
    """
    resultados = []

    for _, row in df.iterrows():
        loja = row.to_dict()
        status = loja.get("status_loja", "")
        resultado = {
            "loja_id":    loja.get("loja_id"),
            "nome_loja":  loja.get("nome_loja"),
            "email":      loja.get("email_loja"),
            "status":     status,
            "acao":       "nenhuma",
            "resultado":  "ok",
            "timestamp":  datetime.now().strftime("%d/%m/%Y %H:%M"),
        }

        if status not in EMAILS:
            resultado["acao"] = "ignorado"
            resultados.append(resultado)
            continue

        if dry_run:
            resultado["acao"] = f"SIMULAÇÃO — e-mail '{EMAILS[status]['assunto']}'"
            resultados.append(resultado)
            continue

        try:
            contact_id = criar_ou_atualizar_contato_hubspot(loja)
            ok = disparar_email_hubspot(contact_id, loja, status)
            resultado["acao"]    = f"e-mail disparado — {EMAILS[status]['assunto']}"
            resultado["resultado"] = "✅ sucesso" if ok else "⚠️ parcial"
        except Exception as e:
            resultado["acao"]    = "erro"
            resultado["resultado"] = f"❌ {str(e)[:80]}"

        resultados.append(resultado)

    return pd.DataFrame(resultados)


# ── INTERFACE STREAMLIT ───────────────────────────────────────────────────────

st.set_page_config(
    page_title="LI Watch · Pipeline",
    page_icon="👁️",
    layout="wide"
)

st.markdown("""
<style>
body, .block-container { background: #F2EDE4; }
[data-testid="stSidebar"] { background: #0D4F4A !important; }
[data-testid="stSidebar"] * { color: #E8F5F4 !important; }
.stButton > button {
    background: #1ABCB0 !important;
    color: #0D4F4A !important;
    font-weight: 600 !important;
    border: none !important;
    border-radius: 10px !important;
}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("""
    <div style='padding:.5rem 0 1rem'>
        <div style='font-size:11px;color:#9DBDBB;text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px'>Loja Integrada</div>
        <div style='font-size:20px;font-weight:700;color:#D4F53C'>LI Watch 👁️</div>
        <div style='font-size:12px;color:#9DBDBB;margin-top:2px'>Pipeline de Automação</div>
    </div>
    """, unsafe_allow_html=True)
    st.divider()
    st.markdown("<div style='font-size:11px;color:#5A9A96'>Time de Automação · 2025</div>", unsafe_allow_html=True)

st.markdown("## ⚙️ Pipeline de automação")
st.markdown("Identifica lojistas que precisam de intervenção e dispara e-mails via HubSpot.")
st.divider()

# Carrega dados
with st.spinner("Carregando lojas para intervenção..."):
    try:
        df = buscar_lojas_para_intervencao()
        erro_dados = None
    except Exception as e:
        df = pd.DataFrame()
        erro_dados = str(e)

if erro_dados:
    st.error(f"Erro ao carregar dados: {erro_dados}")
    st.stop()

if df.empty:
    st.success("✅ Nenhuma loja precisando de intervenção hoje!")
    st.stop()

# Métricas
total = len(df)
onboarding = len(df[df["status_loja"] == "ONBOARDING INCOMPLETO"])
nunca_vendeu = len(df[df["status_loja"] == "NUNCA VENDEU"])
sem_vendas = len(df[df["status_loja"] == "SEM VENDAS RECENTES"])

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total para intervenção", total)
m2.metric("🔴 Onboarding incompleto", onboarding)
m3.metric("🟡 Nunca vendeu", nunca_vendeu)
m4.metric("🟠 Sem vendas recentes", sem_vendas)

st.divider()

# ── DIAGNÓSTICO INTELIGENTE ───────────────────────────────────────────────────
with st.spinner("Calculando score de risco..."):
    df_diag = diagnosticar_dataframe(df)

# ── DIAGNÓSTICO DE QUEDA (lojas SEM VENDAS RECENTES) ─────────────────────────
n_queda = len(df[df["status_loja"] == "SEM VENDAS RECENTES"])
diag_queda_map = {}

if n_queda > 0:
    with st.expander(f"🔍 Diagnóstico de queda — {n_queda} loja(s) com histórico de vendas", expanded=True):
        st.markdown(
            "<div style='background:#FFFBEB;border:1px solid #FDE68A;border-radius:8px;"
            "padding:.8rem 1rem;font-size:13px;color:#92400E;margin-bottom:.8rem'>"
            "⚙️ Analisando histórico de GMV, mix de pagamento e base de clientes para cada loja. "
            "Isso pode levar alguns segundos..."
            "</div>",
            unsafe_allow_html=True,
        )
        progresso = st.progress(0, text="Iniciando análise de queda...")

        def _atualizar_progresso(atual, total):
            pct = int(atual / total * 100)
            progresso.progress(pct, text=f"Diagnosticando loja {atual} de {total}...")

        try:
            diag_queda_map = diagnosticar_queda_batch(
                df_pipeline=df,
                connector=mb,
                status_alvo="SEM VENDAS RECENTES",
                progress_callback=_atualizar_progresso,
            )
            progresso.empty()

            # Mini-tabela de resultados de queda
            rows_queda = []
            for loja_id, res in diag_queda_map.items():
                rows_queda.append({
                    "ID":           loja_id,
                    "Loja":         res["nome_loja"],
                    "Severidade":   res["severidade"],
                    "Δ GMV":        formatar_variacao_gmv(res),
                    "Causa raiz":   formatar_causa_curta(res),
                })
            df_queda_ui = pd.DataFrame(rows_queda).sort_values(
                "Severidade",
                key=lambda s: s.map({"CRÍTICO": 0, "ATENÇÃO": 1, "INVESTIGAR": 2}),
            )
            st.dataframe(df_queda_ui, use_container_width=True, hide_index=True)

        except Exception as e:
            progresso.empty()
            st.error(f"Erro no diagnóstico de queda: {e}")

    st.divider()

# Enriquece df_diag com causa de queda para lojas SEM VENDAS RECENTES
def _enriquecer_causa(row):
    if row["status_loja"] != "SEM VENDAS RECENTES":
        return row["causa_raiz"]
    res = diag_queda_map.get(int(row["loja_id"]))
    if res:
        return formatar_causa_curta(res)
    return row["causa_raiz"]

def _enriquecer_gmv(row):
    if row["status_loja"] != "SEM VENDAS RECENTES":
        return "—"
    res = diag_queda_map.get(int(row["loja_id"]))
    return formatar_variacao_gmv(res) if res else "—"

if diag_queda_map:
    df_diag["causa_raiz"]  = df_diag.apply(_enriquecer_causa, axis=1)
    df_diag["delta_gmv"]   = df_diag.apply(_enriquecer_gmv, axis=1)
    cols_diag = ["loja_id", "nome_loja", "segmento", "status_loja", "score_risco",
                 "prioridade", "dias_cadastro", "delta_gmv", "causa_raiz", "acao_recomendada"]
else:
    cols_diag = ["loja_id", "nome_loja", "segmento", "status_loja", "score_risco",
                 "prioridade", "dias_cadastro", "causa_raiz", "acao_recomendada"]

st.markdown("### Diagnóstico inteligente — ordenado por risco")
st.caption("Score de risco + causa raiz de queda (para lojas com histórico de vendas, analisadas com dados reais de GMV, pagamento e base de clientes)")
st.dataframe(df_diag[cols_diag], use_container_width=True, hide_index=True)

st.divider()
st.markdown("### Detalhamento individual (top 10 por risco)")
for _, row in df_diag.head(10).iterrows():
    loja_row = df[df["loja_id"] == row["loja_id"]]
    if loja_row.empty:
        continue
    loja_diag = diagnosticar_loja(loja_row.iloc[0].to_dict())
    score = row["score_risco"]
    cor   = "#FEF2F2" if score >= 70 else "#FFFBEB" if score >= 45 else "#F0FDF4"
    borda = "#E24B4A" if score >= 70 else "#F59E0B" if score >= 45 else "#1ABCB0"
    with st.expander(str(row["prioridade"]) + " " + str(row["nome_loja"]) + " — Score " + str(score) + "/100"):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Causa raiz**")
            st.markdown("<div style='background:" + cor + ";border-left:4px solid " + borda + ";border-radius:8px;padding:.8rem;font-size:14px;font-weight:600;color:#1A2E2B'>" + str(row["causa_raiz"]) + "</div>", unsafe_allow_html=True)
            st.markdown("**Insights:**")
            for insight in loja_diag["insights"]:
                st.markdown("<div style='font-size:13px;color:#444;padding:4px 0;border-bottom:1px solid #f0ede8'>→ " + insight + "</div>", unsafe_allow_html=True)
        with c2:
            acao = loja_diag["acoes"][0] if loja_diag["acoes"] else "-"
            st.markdown("**Ação recomendada**")
            st.markdown("<div style='background:#D1FAF6;border:1px solid #1ABCB0;border-radius:8px;padding:.8rem;font-size:13px;color:#0D4F4A'>" + acao + "</div>", unsafe_allow_html=True)
            st.markdown("**Benchmark do segmento:**")
            avg_dias = row["benchmark_dias_venda"]
            taxa     = row["benchmark_taxa_conversao"]
            dias_loja = row["dias_cadastro"] or 0
            ratio = round(dias_loja / avg_dias, 1) if avg_dias else 0
            cor_ratio = "#E24B4A" if ratio > 2 else "#F59E0B" if ratio > 1 else "#1ABCB0"
            st.markdown("<div style='font-size:13px;color:#444'>Média dias para 1ª venda: <strong>" + str(avg_dias) + " dias</strong></div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:13px;color:#444'>Taxa de conversão: <strong>" + str(taxa) + "%</strong></div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:13px;color:" + cor_ratio + ";font-weight:600'>Esta loja: " + str(dias_loja) + " dias (" + str(ratio) + "x a média)</div>", unsafe_allow_html=True)

st.divider()

# Preview dos e-mails
st.markdown("### Preview dos e-mails")
for status, info in EMAILS.items():
    n = len(df[df["status_loja"] == status])
    if n > 0:
        with st.expander(f"**{status}** — {n} loja(s) · Assunto: {info['assunto']}"):
            exemplo = df[df["status_loja"] == status].iloc[0].to_dict()
            corpo_ex = info["corpo"].format(
                nome_loja=exemplo.get("nome_loja", "Lojista"),
                itens_faltando=montar_itens_faltando(exemplo)
            )
            st.markdown(f"""<div style='background:white;border:1px solid #e0ddd6;border-radius:10px;padding:1rem;font-size:13px;line-height:1.7'>
                <div style='font-size:11px;color:#888;border-bottom:1px solid #f0ede8;padding-bottom:8px;margin-bottom:12px'>
                    <strong>Para:</strong> {exemplo.get('email_loja','lojista@email.com')} &nbsp;·&nbsp;
                    <strong>Assunto:</strong> {info['assunto']}
                </div>
                {corpo_ex.replace(chr(10),'<br>')}
            </div>""", unsafe_allow_html=True)

st.divider()

# Execução
st.markdown("### Executar pipeline")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div style='background:#FFFBEB;border:1px solid #FDE68A;border-radius:10px;padding:1rem;font-size:13px;color:#92400E'>
        <strong>🧪 Modo simulação</strong><br>
        Mostra o que seria disparado sem enviar nada de verdade.
        Use para validar antes de executar.
    </div>
    """, unsafe_allow_html=True)
    if st.button("▶️ Simular pipeline", use_container_width=True):
        with st.spinner("Simulando..."):
            df_result = rodar_pipeline(df, dry_run=True)
        st.success(f"Simulação concluída — {len(df_result)} lojas processadas")
        st.dataframe(df_result, use_container_width=True, hide_index=True)

with col2:
    st.markdown("""
    <div style='background:#FEF2F2;border:1px solid #FCA5A5;border-radius:10px;padding:1rem;font-size:13px;color:#991B1B'>
        <strong>🚀 Modo produção</strong><br>
        Cria contatos no HubSpot e dispara os e-mails de verdade.
        Só execute após validar a simulação.
    </div>
    """, unsafe_allow_html=True)
    if st.button("🚀 Executar pipeline real", use_container_width=True, type="primary"):
        confirmado = st.checkbox(f"Confirmo o disparo de e-mails para {total} lojistas")
        if confirmado:
            with st.spinner(f"Executando pipeline para {total} lojas..."):
                df_result = rodar_pipeline(df, dry_run=False)
            sucessos = len(df_result[df_result["resultado"].str.contains("sucesso", na=False)])
            erros    = len(df_result[df_result["resultado"].str.contains("erro|❌", na=False)])
            st.success(f"Pipeline concluído — {sucessos} enviados · {erros} erros")
            st.dataframe(df_result, use_container_width=True, hide_index=True)

            # Download do log
            log_excel = df_result.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Baixar log de execução",
                data=log_excel,
                file_name=f"pipeline_log_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )
