import requests
import pandas as pd
import streamlit as st
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# ── CONFIGURAÇÃO ──────────────────────────────────────────────────────────────

def _cfg():
    return (
        st.secrets["metabase"]["url"],
        st.secrets["metabase"]["token"],
        int(st.secrets["metabase"]["db_id"]),
    )

def _headers():
    _, token, _ = _cfg()
    return {"X-Metabase-Session": token, "Content-Type": "application/json"}

# ── EXECUTAR SQL ──────────────────────────────────────────────────────────────

def _rodar_sql(sql: str) -> pd.DataFrame:
    url, _, db_id = _cfg()
    payload = {"database": db_id, "type": "native", "native": {"query": sql}}
    r = requests.post(f"{url}/api/dataset", headers=_headers(), json=payload, timeout=120)
    if r.status_code != 200:
        raise Exception(f"Erro {r.status_code}: {r.text[:300]}")
    data = r.json()
    if "error" in data:
        raise Exception(data["error"])
    cols = [c["name"] for c in data["data"]["cols"]]
    return pd.DataFrame(data["data"]["rows"], columns=cols)

# ── QUERY 1: FUNIL DE ONBOARDING ──────────────────────────────────────────────

def buscar_funil(data_inicio: str, data_fim: str) -> pd.DataFrame:
    """
    Retorna o funil agregado de onboarding para o período.
    Usa a query corrigida que montamos.
    """
    sql = f"""
WITH

cte_onboarding AS (
    SELECT
        loja_id
        ,desc_nome_loja                                                         AS loja_nome
        ,desc_segmento                                                          AS segmento_loja
        ,CASE
            WHEN desc_tipo_pessoa_cadastro_loja IS NULL
              OR desc_tipo_pessoa_cadastro_loja = 'nao informado'
            THEN NULL
            ELSE desc_tipo_pessoa_cadastro_loja
         END                                                                    AS flag_pf_pj
        ,CASE
            WHEN cast(json_query(desc_endereco_loja, '$.desc_loja_cep') AS VARCHAR) IS NOT NULL
            THEN 1 ELSE 0
         END                                                                    AS flag_passo_3
        ,CASE
            WHEN cast(json_query(desc_endereco_loja, '$.desc_endereco')    AS VARCHAR) IS NULL
              OR cast(json_query(desc_endereco_loja, '$.desc_loja_cep')    AS VARCHAR) IS NULL
              OR cast(json_query(desc_endereco_loja, '$.desc_loja_cidade') AS VARCHAR) IS NULL
              OR cast(json_query(desc_endereco_loja, '$.desc_loja_estado') AS VARCHAR) IS NULL
              OR cast(json_query(desc_endereco_loja, '$.desc_loja_numero') AS VARCHAR) IS NULL
            THEN 0 ELSE 1
         END                                                                    AS flag_passo_5
        ,CASE
            WHEN desc_aquisicao_canal IN ('Orgânico', 'Indicação') THEN desc_aquisicao_canal
            ELSE 'Pago'
         END                                                                    AS flag_origem
        ,desc_aquisicao_subcanal                                                AS origem_subcanal
        ,date(dt_criacao_loja)                                                  AS data_criacao
        ,date_trunc('month', date(dt_criacao_loja))                             AS safra_criacao
        ,date(dt_primeira_configuracao_envio)                                   AS data_config_envio
        ,date(dt_primeira_configuracao_pagamento)                               AS data_config_pagamento
        ,date(dt_primeira_configuracao_produto)                                 AS data_config_produto
        ,date(dt_primeira_visita_valida)                                        AS data_primeira_visita
    FROM analytics_gold.dim_loja
    WHERE date(dt_criacao_loja) BETWEEN '{data_inicio}' AND '{data_fim}'
)

,cte_plano AS (
    SELECT DISTINCT
        a.loja_id
        ,a.nr_ciclo                                                             AS ciclo_id
        ,a.dt_plano_assinatura_ciclo_inicio                                     AS data_plano
        ,date_trunc('month', a.dt_plano_assinatura_ciclo_inicio)                AS safra_plano
        ,a.desc_ciclo_plano                                                     AS ciclo_plano
        ,a.desc_tipo_plano                                                      AS tipo_plano
        ,b.desc_forma_pagamento                                                 AS forma_pagamento_plano
        ,CASE
            WHEN a.desc_ciclo_plano = 'Mensal' THEN a.vlr_plano_assinatura
            WHEN a.desc_ciclo_plano = 'Anual'  THEN a.vlr_plano_assinatura / 12.0
         END                                                                    AS vlr_plano_nmrr
        ,CASE
            WHEN a.desc_ciclo_plano = 'Mensal' THEN a.vlr_plano_assinatura * 12.0
            WHEN a.desc_ciclo_plano = 'Anual'  THEN a.vlr_plano_assinatura
         END                                                                    AS vlr_plano_arr
    FROM analytics_gold.ft_assinatura AS a
    INNER JOIN (
        SELECT DISTINCT fatura_id, desc_forma_pagamento
        FROM analytics_gold.ft_fatura
        WHERE flag_fatura_paga = 1
    ) AS b ON a.fatura_id = b.fatura_id
    WHERE a.dt_plano_assinatura_ciclo_inicio BETWEEN '{data_inicio}' AND '{data_fim}'
)

,cte_ultimo_plano AS (
    SELECT
        loja_id
        ,max(ciclo_id)                           AS ciclo_id
        ,max_by(data_plano,            ciclo_id) AS data_plano
        ,max_by(safra_plano,           ciclo_id) AS safra_plano
        ,max_by(ciclo_plano,           ciclo_id) AS ciclo_plano
        ,max_by(tipo_plano,            ciclo_id) AS tipo_plano
        ,max_by(forma_pagamento_plano, ciclo_id) AS forma_pagamento_plano
        ,max_by(vlr_plano_nmrr,        ciclo_id) AS vlr_plano_nmrr
        ,max_by(vlr_plano_arr,         ciclo_id) AS vlr_plano_arr
    FROM cte_plano
    GROUP BY loja_id
)

,cte_visita AS (
    SELECT
        store_id                                        AS loja_id
        ,date(timestamp)                                AS data_visita
        ,date_trunc('month', date(timestamp))           AS safra_visita
        ,user_session_id                                AS usuario_id
        ,count(DISTINCT session_id)                     AS qtde_visita
    FROM kafka.li_analytics_lake
    WHERE date(timestamp) BETWEEN '{data_inicio}' AND '{data_fim}'
      AND store_test_account = false
      AND store_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

,cte_venda_captada AS (
    SELECT DISTINCT
        loja_id
        ,dt_pedido_criacao                              AS data_venda_captada
        ,date_trunc('month', dt_pedido_criacao)         AS safra_venda_captada
    FROM analytics_gold.ft_pedido
    WHERE dt_pedido_criacao BETWEEN '{data_inicio}' AND '{data_fim}'
)

,cte_venda_aprovada AS (
    SELECT
        a.loja_id
        ,a.dt_pedido_criacao                            AS data_venda_aprovada
        ,date_trunc('month', a.dt_pedido_criacao)       AS safra_venda_aprovada
        ,count(DISTINCT a.pedido_id)                    AS qtde_pedido_aprovado
        ,sum(a.vlr_gmv)                                 AS vlr_gmv_aprovado
    FROM analytics_gold.ft_pedido AS a
    INNER JOIN (
        SELECT DISTINCT pedido_id
        FROM analytics_gold.dim_pedido
        WHERE flag_aprovado_pedido = 1
    ) AS b ON a.pedido_id = b.pedido_id
    WHERE a.dt_pedido_criacao BETWEEN '{data_inicio}' AND '{data_fim}'
    GROUP BY 1, 2, 3
)

,cte_join AS (
    SELECT
        a.loja_id
        ,a.data_criacao, a.safra_criacao, a.segmento_loja, a.flag_pf_pj
        ,CASE WHEN a.loja_nome IS NOT NULL AND a.segmento_loja IS NOT NULL THEN 1 ELSE 0 END AS flag_passo_1
        ,CASE WHEN a.flag_pf_pj IS NOT NULL THEN 1 ELSE 0 END                               AS flag_passo_2
        ,a.flag_passo_3, a.flag_passo_5
        ,a.flag_origem, a.origem_subcanal
        ,CASE WHEN a.data_config_envio     IS NOT NULL THEN 1 ELSE 0 END AS flag_config_envio
        ,a.data_config_envio
        ,CASE WHEN a.data_config_pagamento IS NOT NULL THEN 1 ELSE 0 END AS flag_config_pagamento
        ,a.data_config_pagamento
        ,CASE WHEN a.data_config_produto   IS NOT NULL THEN 1 ELSE 0 END AS flag_config_produto
        ,a.data_config_produto
        ,CASE
            WHEN a.data_config_envio IS NOT NULL
             AND a.data_config_pagamento IS NOT NULL
             AND a.data_config_produto IS NOT NULL
            THEN 1 ELSE 0
         END AS flag_config
        ,greatest(a.data_config_envio, a.data_config_pagamento, a.data_config_produto) AS data_config
        ,CASE WHEN a.data_primeira_visita IS NOT NULL THEN 1 ELSE 0 END AS flag_primeira_visita
        ,a.data_primeira_visita
        ,b.data_plano
        ,CASE WHEN b.data_plano IS NOT NULL THEN 'PAGO' ELSE 'GRÁTIS' END AS flag_plano
        ,ifnull(b.ciclo_plano, 'GRÁTIS')          AS ciclo_plano
        ,ifnull(b.tipo_plano, 'GRÁTIS')           AS tipo_plano
        ,ifnull(b.forma_pagamento_plano, 'GRÁTIS') AS forma_pagamento_plano
        ,ifnull(b.vlr_plano_nmrr, 0)              AS vlr_plano_nmrr
        ,ifnull(b.vlr_plano_arr, 0)               AS vlr_plano_arr
        ,count(DISTINCT c.usuario_id)             AS qtde_usuario_mes
        ,round(cast(sum(c.qtde_visita) AS BIGINT), 0) AS qtde_visita_mes
        ,CASE WHEN min(d.data_venda_captada)  IS NOT NULL THEN 1 ELSE 0 END AS flag_primeira_venda_captada
        ,min(d.data_venda_captada) AS data_primeira_venda_captada
        ,CASE WHEN min(e.data_venda_aprovada) IS NOT NULL THEN 1 ELSE 0 END AS flag_primeira_venda_aprovada
        ,min(e.data_venda_aprovada) AS data_primeira_venda_aprovada
        ,round(cast(sum(e.qtde_pedido_aprovado) AS BIGINT), 0) AS qtde_pedido_aprovado_mes
        ,round(cast(sum(e.vlr_gmv_aprovado) AS DOUBLE), 2)     AS vlr_gmv_aprovado_mes
    FROM cte_onboarding AS a
    LEFT JOIN cte_ultimo_plano AS b ON a.loja_id = b.loja_id AND a.safra_criacao = b.safra_plano
    LEFT JOIN cte_visita AS c ON a.loja_id = c.loja_id AND a.safra_criacao = c.safra_visita AND a.data_primeira_visita <= c.data_visita
    LEFT JOIN cte_venda_captada AS d ON a.loja_id = d.loja_id AND a.safra_criacao = d.safra_venda_captada
    LEFT JOIN cte_venda_aprovada AS e ON a.loja_id = e.loja_id AND a.safra_criacao = e.safra_venda_aprovada
    GROUP BY
        a.loja_id, a.data_criacao, a.safra_criacao, a.segmento_loja, a.flag_pf_pj,
        a.loja_nome, a.flag_passo_3, a.flag_passo_5, a.flag_origem, a.origem_subcanal,
        a.data_config_envio, a.data_config_pagamento, a.data_config_produto,
        a.data_primeira_visita, b.data_plano, b.ciclo_plano, b.tipo_plano,
        b.forma_pagamento_plano, b.vlr_plano_nmrr, b.vlr_plano_arr
)

SELECT
    safra_criacao, upper(segmento_loja) AS segmento_loja,
    upper(flag_pf_pj) AS flag_pf_pj, upper(flag_origem) AS flag_origem,
    upper(flag_plano) AS flag_plano, upper(ciclo_plano) AS ciclo_plano,
    upper(tipo_plano) AS tipo_plano,
    count(DISTINCT loja_id)                                                          AS qtde_loja_criada
    ,count(DISTINCT CASE WHEN flag_passo_1 = 1          THEN loja_id END)            AS qtde_loja_passo_1
    ,count(DISTINCT CASE WHEN flag_passo_2 = 1          THEN loja_id END)            AS qtde_loja_passo_2
    ,count(DISTINCT CASE WHEN flag_passo_3 = 1          THEN loja_id END)            AS qtde_loja_passo_3
    ,count(DISTINCT CASE WHEN flag_passo_5 = 1          THEN loja_id END)            AS qtde_loja_passo_5
    ,count(DISTINCT CASE WHEN flag_config_envio = 1     THEN loja_id END)            AS qtde_loja_config_envio
    ,count(DISTINCT CASE WHEN flag_config_pagamento = 1 THEN loja_id END)            AS qtde_loja_config_pagamento
    ,count(DISTINCT CASE WHEN flag_config_produto = 1   THEN loja_id END)            AS qtde_loja_config_produto
    ,count(DISTINCT CASE WHEN flag_config = 1           THEN loja_id END)            AS qtde_loja_config
    ,count(DISTINCT CASE WHEN flag_primeira_visita = 1  THEN loja_id END)            AS qtde_loja_primeira_visita
    ,count(DISTINCT CASE WHEN flag_primeira_venda_captada  = 1 THEN loja_id END)     AS qtde_loja_primeira_venda_captada
    ,count(DISTINCT CASE WHEN flag_primeira_venda_aprovada = 1 THEN loja_id END)     AS qtde_loja_primeira_venda_aprovada
    ,round(cast(sum(vlr_plano_nmrr) AS DOUBLE), 2)  AS vlr_plano_nmrr
    ,round(cast(sum(vlr_gmv_aprovado_mes) AS DOUBLE), 2) AS vlr_gmv_aprovado_mes
FROM cte_join
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 DESC
    """
    return _rodar_sql(sql)


# ── QUERY 2: TOP LOJAS POR FATURAMENTO (para lista de queda) ─────────────────

def buscar_top_lojas(limite: int = 50) -> pd.DataFrame:
    """Retorna as lojas com maior GMV nos últimos 6 meses para a lista de seleção."""
    hoje = date.today()
    data_inicio = (hoje - relativedelta(months=6)).strftime("%Y-%m-%d")
    data_fim = hoje.strftime("%Y-%m-%d")

    sql = f"""
SELECT
    a.loja_id                           AS conta_id
    ,l.desc_nome_loja                   AS nome_loja
    ,upper(l.desc_segmento)             AS segmento
    ,round(sum(a.vlr_gmv), 2)           AS gmv_6m
    ,count(DISTINCT a.pedido_id)        AS pedidos_6m
FROM analytics_gold.ft_pedido AS a
INNER JOIN (
    SELECT DISTINCT pedido_id
    FROM analytics_gold.dim_pedido
    WHERE flag_aprovado_pedido = 1
) AS b ON a.pedido_id = b.pedido_id
INNER JOIN analytics_gold.dim_loja AS l ON a.loja_id = l.loja_id
WHERE a.dt_pedido_criacao BETWEEN '{data_inicio}' AND '{data_fim}'
GROUP BY 1, 2, 3
ORDER BY gmv_6m DESC
LIMIT {limite}
    """
    return _rodar_sql(sql)


# ── QUERIES 3-9: DIAGNÓSTICO DE QUEDA (skill do chefe) ───────────────────────

def buscar_tendencia(conta_id: int, data_inicio: str, data_fim: str) -> pd.DataFrame:
    """Query 1 da skill — evolução mensal de volume, receita e ticket."""
    sql = f"""
SELECT
    DATE_FORMAT(CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo'),'%Y-%m') AS mes
    ,COUNT(DISTINCT A.pedido_venda_id)           AS total_pedidos
    ,ROUND(SUM(A.pedido_venda_valor_total), 2)   AS receita_total
    ,ROUND(AVG(A.pedido_venda_valor_total), 2)   AS ticket_medio
    ,ROUND(AVG(A.pedido_venda_valor_desconto), 2) AS desconto_medio
    ,ROUND(AVG(A.pedido_venda_valor_envio), 2)   AS frete_medio
FROM pedido_tb_pedido_venda A
INNER JOIN pedido_tb_pedido_venda_situacao D ON A.pedido_venda_situacao_id = D.pedido_venda_situacao_id
WHERE A.conta_id = {conta_id}
  AND CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') >= '{data_inicio}'
  AND CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') <= '{data_fim}'
  AND D.pedido_venda_situacao_nome != 'Pedido Cancelado'
GROUP BY mes ORDER BY mes ASC
    """
    return _rodar_sql(sql)


def buscar_novos_recorrentes(conta_id: int, data_inicio: str, data_fim: str) -> pd.DataFrame:
    """Query 5 da skill — novos vs recorrentes por mês."""
    sql = f"""
SELECT
    DATE_FORMAT(CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo'),'%Y-%m') AS mes
    ,CASE
        WHEN B.cliente_data_criacao >= DATE_FORMAT(
            CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo'),'%Y-%m-01')
        THEN 'Novo' ELSE 'Recorrente'
     END AS tipo_cliente
    ,COUNT(DISTINCT A.pedido_venda_id) AS total_pedidos
    ,ROUND(AVG(A.pedido_venda_valor_total), 2) AS ticket_medio
FROM pedido_tb_pedido_venda A
INNER JOIN cliente_tb_cliente B ON A.cliente_id = B.cliente_id
INNER JOIN pedido_tb_pedido_venda_situacao D ON A.pedido_venda_situacao_id = D.pedido_venda_situacao_id
WHERE A.conta_id = {conta_id}
  AND CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') >= '{data_inicio}'
  AND CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') <= '{data_fim}'
  AND D.pedido_venda_situacao_nome != 'Pedido Cancelado'
GROUP BY mes, tipo_cliente ORDER BY mes ASC
    """
    return _rodar_sql(sql)


def buscar_mix_pagamento(conta_id: int, data_inicio: str, data_fim: str) -> pd.DataFrame:
    """Query 6 da skill — mix de pagamento por mês."""
    sql = f"""
SELECT
    DATE_FORMAT(CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo'),'%Y-%m') AS mes
    ,F.pagamento_nome AS forma_pagamento
    ,COUNT(DISTINCT A.pedido_venda_id) AS total_pedidos
    ,ROUND(AVG(A.pedido_venda_valor_total), 2) AS ticket_medio
FROM pedido_tb_pedido_venda A
INNER JOIN pedido_tb_pedido_venda_situacao D ON A.pedido_venda_situacao_id = D.pedido_venda_situacao_id
INNER JOIN pedido_tb_pedido_venda_pagamento E ON A.pedido_venda_id = E.pedido_venda_id
INNER JOIN configuracao_tb_pagamento F ON E.pagamento_id = F.pagamento_id
WHERE A.conta_id = {conta_id}
  AND CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') >= '{data_inicio}'
  AND CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') <= '{data_fim}'
  AND D.pedido_venda_situacao_nome != 'Pedido Cancelado'
GROUP BY mes, forma_pagamento ORDER BY mes ASC, total_pedidos DESC
    """
    return _rodar_sql(sql)


def buscar_clientes_churned(conta_id: int, ref_inicio: str, ref_fim: str, corte: str) -> pd.DataFrame:
    """Query 7 da skill — clientes que sumiram após o período de referência."""
    sql = f"""
SELECT
    B.cliente_id, B.cliente_nome, B.cliente_email
    ,COUNT(DISTINCT A.pedido_venda_id) AS total_pedidos_historico
    ,ROUND(SUM(A.pedido_venda_valor_total), 2) AS receita_total_historico
    ,ROUND(AVG(A.pedido_venda_valor_total), 2) AS ticket_medio
    ,MAX(CONVERT_TZ(A.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo')) AS ultimo_pedido
FROM cliente_tb_cliente B
INNER JOIN pedido_tb_pedido_venda A ON B.cliente_id = A.cliente_id
INNER JOIN pedido_tb_pedido_venda_situacao D ON A.pedido_venda_situacao_id = D.pedido_venda_situacao_id
WHERE A.conta_id = {conta_id}
  AND D.pedido_venda_situacao_nome != 'Pedido Cancelado'
  AND B.cliente_id IN (
    SELECT DISTINCT A2.cliente_id
    FROM pedido_tb_pedido_venda A2
    INNER JOIN pedido_tb_pedido_venda_situacao D2 ON A2.pedido_venda_situacao_id = D2.pedido_venda_situacao_id
    WHERE A2.conta_id = {conta_id}
      AND CONVERT_TZ(A2.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') BETWEEN '{ref_inicio}' AND '{ref_fim}'
      AND D2.pedido_venda_situacao_nome != 'Pedido Cancelado'
  )
  AND B.cliente_id NOT IN (
    SELECT DISTINCT A3.cliente_id
    FROM pedido_tb_pedido_venda A3
    INNER JOIN pedido_tb_pedido_venda_situacao D3 ON A3.pedido_venda_situacao_id = D3.pedido_venda_situacao_id
    WHERE A3.conta_id = {conta_id}
      AND CONVERT_TZ(A3.pedido_venda_data_criacao,'+00:00','America/Sao_Paulo') >= '{corte}'
      AND D3.pedido_venda_situacao_nome != 'Pedido Cancelado'
  )
GROUP BY B.cliente_id, B.cliente_nome, B.cliente_email
ORDER BY receita_total_historico DESC
LIMIT 100
    """
    return _rodar_sql(sql)
