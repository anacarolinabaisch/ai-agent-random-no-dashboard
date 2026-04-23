"""
update_dashboard.py
Busca dados atualizados do Databricks e reescreve o index.html do dashboard.
Roda toda sexta-feira via GitHub Actions.
"""

import os
import json
import re
from datetime import datetime, date
from databricks import sql

# ── Configuração ────────────────────────────────────────────────────────────
DATABRICKS_HOST  = os.environ["DATABRICKS_HOST"]   # nubank-e2-general.cloud.databricks.com
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]  # dapi...
HTTP_PATH        = os.environ["DATABRICKS_HTTP_PATH"]  # /sql/1.0/warehouses/...

TABLE = "etl.br__series_contract.nelson_muntz_labels"
HTML_PATH = "index.html"

# ── Mapeamento de e-mail → chave interna ────────────────────────────────────
EMAIL_TO_KEY = {
    "michelle.santos@nubank.com.br":          "michelle",
    "camila.bomfim@nubank.com.br":            "camila",
    "angel.roberto@nubank.com.br":            "angel",
    "carolina.staianov@nubank.com.br":        "carolina",
    "nathalia.gomes@nubank.com.br":           "nathalia",
    "lucas.fernandessantos@nubank.com.br":    "lucas",
    "douglas.vilela@nubank.com.br":           "douglas",
}

COLORS = {
    "michelle": "#8b5cf6",
    "camila":   "#06b6d4",
    "angel":    "#22c55e",
    "carolina": "#f59e0b",
    "nathalia": "#38bdf8",
    "lucas":    "#a78bfa",
    "douglas":  "#64748b",
}

INITIALS = {
    "michelle": "MS",
    "camila":   "CB",
    "angel":    "AR",
    "carolina": "CS",
    "nathalia": "NG",
    "lucas":    "LF",
    "douglas":  "DV",
}

SHORT_NAMES = {
    "michelle": "Michelle Santos",
    "camila":   "Camila Bomfim",
    "angel":    "Angel Roberto",
    "carolina": "Carolina Staianov",
    "nathalia": "Nathalia Gomes",
    "lucas":    "Lucas F. Santos",
    "douglas":  "Douglas Vilela",
}

# ── Busca de dados ───────────────────────────────────────────────────────────
def fetch_data():
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cur:

            # 1. Dados por analista
            cur.execute(f"""
                SELECT
                    annotator_email,
                    COUNT(DISTINCT case_id)                                         AS total_cases,
                    COUNT(*)                                                         AS total_labels,
                    COUNT(DISTINCT DATE(created_at))                                 AS days_active,
                    ROUND(AVG(CASE WHEN decision = 'Yes' THEN 1.0 ELSE 0.0 END)*100,1) AS assertiveness,
                    ROUND(AVG(CASE WHEN error_process = 'Yes' THEN 1.0 ELSE 0.0 END)*100,1) AS error_process,
                    ROUND(AVG(CASE WHEN check_side_effects = 'NOK' THEN 1.0 ELSE 0.0 END)*100,1) AS side_effect_nok,
                    ROUND(AVG(time_spent_seconds),1)                                AS avg_time_sec,
                    MAX(time_spent_seconds)                                          AS max_time_sec,
                    MIN(DATE(created_at))                                            AS first_date,
                    MAX(DATE(created_at))                                            AS last_date,
                    SUM(CASE WHEN decision = 'Yes' THEN 1 ELSE 0 END)               AS decision_yes,
                    SUM(CASE WHEN decision = 'No'  THEN 1 ELSE 0 END)               AS decision_no,
                    SUM(CASE WHEN error_process = 'No'  THEN 1 ELSE 0 END)          AS process_ok,
                    SUM(CASE WHEN error_process = 'Yes' THEN 1 ELSE 0 END)          AS process_err,
                    SUM(CASE WHEN check_side_effects = 'OK'  THEN 1 ELSE 0 END)     AS side_ok,
                    SUM(CASE WHEN check_side_effects = 'NOK' THEN 1 ELSE 0 END)     AS side_nok
                FROM {TABLE}
                GROUP BY annotator_email
            """)
            agent_rows = cur.fetchall()
            agent_cols = [d[0] for d in cur.description]

            # 2. Dados diários por analista
            cur.execute(f"""
                SELECT
                    annotator_email,
                    DATE(created_at)                 AS day,
                    COUNT(DISTINCT case_id)          AS cases,
                    ROUND(SUM(time_spent_seconds)/60.0, 1) AS minutes
                FROM {TABLE}
                GROUP BY annotator_email, DATE(created_at)
                ORDER BY annotator_email, day
            """)
            daily_rows = cur.fetchall()

            # 3. Erros de classificação por analista
            cur.execute(f"""
                SELECT
                    annotator_email,
                    ai_error_classification,
                    COUNT(*) AS cnt
                FROM {TABLE}
                WHERE ai_error_classification IS NOT NULL
                  AND ai_error_classification != ''
                GROUP BY annotator_email, ai_error_classification
            """)
            err_rows = cur.fetchall()

            # 4. Totais globais
            cur.execute(f"""
                SELECT
                    COUNT(DISTINCT case_id)  AS total_cases,
                    COUNT(*)                  AS total_labels,
                    MIN(DATE(created_at))     AS min_date,
                    MAX(DATE(created_at))     AS max_date,
                    ROUND(AVG(CASE WHEN decision = 'Yes' THEN 1.0 ELSE 0.0 END)*100,1) AS assertiveness,
                    ROUND(AVG(CASE WHEN error_process = 'Yes' THEN 1.0 ELSE 0.0 END)*100,1) AS error_process,
                    ROUND(AVG(CASE WHEN check_side_effects = 'NOK' THEN 1.0 ELSE 0.0 END)*100,1) AS side_effect_nok,
                    SUM(CASE WHEN decision = 'Yes' THEN 1 ELSE 0 END) AS decision_yes,
                    SUM(CASE WHEN decision = 'No'  THEN 1 ELSE 0 END) AS decision_no,
                    SUM(CASE WHEN error_process = 'No'  THEN 1 ELSE 0 END) AS process_ok,
                    SUM(CASE WHEN error_process = 'Yes' THEN 1 ELSE 0 END) AS process_err,
                    SUM(CASE WHEN check_side_effects = 'OK'  THEN 1 ELSE 0 END) AS side_ok,
                    SUM(CASE WHEN check_side_effects = 'NOK' THEN 1 ELSE 0 END) AS side_nok,
                    SUM(CASE WHEN contestation IS NOT NULL AND contestation != '' THEN 1 ELSE 0 END) AS with_contestation,
                    SUM(CASE WHEN information_not_considered = 'No' THEN 1 ELSE 0 END) AS info_not_considered
                FROM {TABLE}
            """)
            global_row = dict(zip([d[0] for d in cur.description], cur.fetchone()))

    return agent_rows, agent_cols, daily_rows, err_rows, global_row


# ── Processamento ────────────────────────────────────────────────────────────
def process(agent_rows, agent_cols, daily_rows, err_rows, global_row):
    agents = {}

    for row in agent_rows:
        r = dict(zip(agent_cols, row))
        email = r["annotator_email"]
        key = EMAIL_TO_KEY.get(email)
        if not key:
            continue

        first = str(r["first_date"])
        last  = str(r["last_date"])
        period = f"{first[8:]}/{first[5:7]} → {last[8:]}/{last[5:7]}"

        agents[key] = {
            "key":           email,
            "short":         SHORT_NAMES[key],
            "initials":      INITIALS[key],
            "color":         COLORS[key],
            "totalCases":    int(r["total_cases"]),
            "totalLabels":   int(r["total_labels"]),
            "daysActive":    int(r["days_active"]),
            "assertiveness": float(r["assertiveness"]),
            "errorProcess":  float(r["error_process"]),
            "sideEffectNok": float(r["side_effect_nok"]),
            "avgTimeSec":    float(r["avg_time_sec"] or 0),
            "maxTimeSec":    int(r["max_time_sec"] or 0),
            "period":        period,
            "decisionYes":   int(r["decision_yes"]),
            "decisionNo":    int(r["decision_no"]),
            "processOk":     int(r["process_ok"]),
            "processErr":    int(r["process_err"]),
            "sideOk":        int(r["side_ok"]),
            "sideNok":       int(r["side_nok"]),
            "daily":         {},
            "errClass":      {},
        }

    for row in daily_rows:
        email, day, cases, minutes = row
        key = EMAIL_TO_KEY.get(email)
        if not key or key not in agents:
            continue
        agents[key]["daily"][str(day)] = {"cases": int(cases), "min": float(minutes)}

    for row in err_rows:
        email, err_class, cnt = row
        key = EMAIL_TO_KEY.get(email)
        if not key or key not in agents:
            continue
        if err_class:
            agents[key]["errClass"][err_class] = int(cnt)

    return agents, global_row


# ── Geração do bloco JS ──────────────────────────────────────────────────────
def build_agents_js(agents):
    lines = ["const AGENTS = {"]
    for key, a in agents.items():
        daily_str = json.dumps(a["daily"], ensure_ascii=False)
        err_str   = json.dumps(a["errClass"], ensure_ascii=False)
        lines.append(f"""  {key}: {{
    key:'{a["key"]}', short:'{a["short"]}', initials:'{a["initials"]}', color:'{a["color"]}',
    totalCases:{a["totalCases"]}, totalLabels:{a["totalLabels"]}, daysActive:{a["daysActive"]},
    assertiveness:{a["assertiveness"]}, errorProcess:{a["errorProcess"]}, sideEffectNok:{a["sideEffectNok"]},
    avgTimeSec:{a["avgTimeSec"]}, maxTimeSec:{a["maxTimeSec"]}, period:'{a["period"]}',
    decisionYes:{a["decisionYes"]}, decisionNo:{a["decisionNo"]}, processOk:{a["processOk"]}, processErr:{a["processErr"]}, sideOk:{a["sideOk"]}, sideNok:{a["sideNok"]},
    daily:{daily_str},
    errClass:{err_str}
  }},""")
    lines.append("};")
    return "\n".join(lines)


# ── Atualização do HTML ──────────────────────────────────────────────────────
def update_html(agents, global_row):
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    # Substitui bloco AGENTS
    new_agents_js = build_agents_js(agents)
    html = re.sub(
        r"const AGENTS = \{.*?\};",
        new_agents_js,
        html,
        flags=re.DOTALL
    )

    # Atualiza data no hero
    today = date.today().strftime("%-d %b %Y")
    html = re.sub(
        r"(br__series_contract\.nelson_muntz_labels · Análise completa · )[\d\w ]+",
        rf"\g<1>{today}",
        html
    )

    # Atualiza período no filtro
    min_date = str(global_row["min_date"])
    max_date = str(global_row["max_date"])

    html = re.sub(r'value="2026-\d\d-\d\d" min="\S+" max="\S+"',
                  f'value="{max_date}" min="{min_date}" max="{max_date}"', html)

    # Atualiza métricas globais nos KPIs estáticos
    total_cases = global_row["total_cases"]
    assertiveness = global_row["assertiveness"]
    error_process = global_row["error_process"]
    side_nok = global_row["side_effect_nok"]
    decision_yes = global_row["decision_yes"]
    decision_no  = global_row["decision_no"]
    process_ok   = global_row["process_ok"]
    process_err  = global_row["process_err"]
    side_ok      = global_row["side_ok"]
    side_nok_cnt = global_row["side_nok"]
    with_contest = global_row["with_contestation"]
    total_labels = global_row["total_labels"]

    # Tag assertividade global no hero
    html = re.sub(r"\d+,\d+% assertividade global", f"{str(assertiveness).replace('.',',')}% assertividade global", html)

    # Donuts legenda
    html = re.sub(r"● \d+,\d+% Correto \(\d+\)", f"● {str(assertiveness).replace('.',',')}% Correto ({decision_yes})", html)
    html = re.sub(r"● \d+,\d+% Incorreto \(\d+\)", f"● {str(100-assertiveness).replace('.',',')}% Incorreto ({decision_no})", html)

    error_pct = error_process
    html = re.sub(r"● \d+,\d+% Sem erro \(\d+\)", f"● {str(100-error_pct).replace('.',',')}% Sem erro ({process_ok})", html)
    html = re.sub(r"● \d+,\d+% Com erro \(\d+\)", f"● {str(error_pct).replace('.',',')}% Com erro ({process_err})", html)

    side_ok_pct = round(100 - side_nok, 1)
    html = re.sub(r"● \d+,\d+% OK \(\d+\)", f"● {str(side_ok_pct).replace('.',',')}% OK ({side_ok})", html)
    html = re.sub(r"● \d+,\d+% NOK \(\d+\)", f"● {str(side_nok).replace('.',',')}% NOK ({side_nok_cnt})", html)

    # Hero tag total de casos
    html = re.sub(r"\d[\d\.]+ casos", f"{total_cases:,} casos".replace(",", "."), html)

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Dashboard atualizado: {total_cases} casos, período {min_date} → {max_date}")


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🔄 Buscando dados no Databricks...")
    agent_rows, agent_cols, daily_rows, err_rows, global_row = fetch_data()

    print("⚙️  Processando...")
    agents, global_row = process(agent_rows, agent_cols, daily_rows, err_rows, global_row)

    print("📝 Atualizando HTML...")
    update_html(agents, global_row)
