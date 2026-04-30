# LI Pulse · Automação de Onboarding
**Loja Integrada · Time de Automação · 2025**

---

## Como rodar localmente

**1. Instala as dependências**
```
pip install -r requirements.txt
```

**2. Cria as credenciais locais**

Cria a pasta e o arquivo:
```
mkdir .streamlit
```

Cria `.streamlit/secrets.toml`:
```toml
[metabase]
url   = "https://metabase.network.awsli.com.br"
token = "seu_token_aqui"
db_id = 11
```

**3. Roda**
```
streamlit run app.py
```

---

## Como subir pro Streamlit Cloud

1. Sobe `app.py`, `requirements.txt` e `metabase_connector.py` pro GitHub
2. Conecta o repo no Streamlit Cloud
3. Vai em **Settings → Secrets** e cola:

```toml
[metabase]
url   = "https://metabase.network.awsli.com.br"
token = "seu_token_aqui"
db_id = 11
```

O `.gitignore` já bloqueia o `secrets.toml` de ir pro GitHub.
O token nunca aparece no código nem no repositório.
