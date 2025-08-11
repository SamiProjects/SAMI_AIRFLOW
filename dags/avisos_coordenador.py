from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import os
import psycopg2
import requests
import difflib
from openai import OpenAI
import matplotlib.dates as mdates

# --- gráfico/base64 ---
import base64
from io import BytesIO
import matplotlib
matplotlib.use("Agg")  # backend sem GUI
import matplotlib.pyplot as plt
from datetime import datetime as dt

# ========= Config =========
TABLE_TXT = "texto_gerado_avisos_coordenadores"   # tabela onde salvamos as mensagens geradas
MODEL = "gpt-4o-mini"                             # modelo de geração
SIMILARIDADE_MAX = 0.80                           # quão diferente precisa ser da última
TENTATIVAS_GERACAO = 3                            # quantas tentativas (aumenta temperature)

def enviar_email(imagem,texto,assunto):
    url = 'https://prod-163.westus.logic.azure.com:443/workflows/99025d82c4da4a27816570798717d006/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EG5N42dZVZ-9JUdZf5K09WZuIJxBZAK0kWo9A4vcZNI'

    body = {
        "imagem":imagem,
        "texto": texto,
        "assunto": assunto
    }

    requests.post(url=url,json=body)

# ========= Helpers DB =========
def get_conn():
    return psycopg2.connect(os.getenv("PG_CONN"))

def pode_disparar(conn, id_alarme: int, cooldown_minutos: int):
    """
    Usa controle_tempo_avisos_coordenadores como 'agenda' do próximo aviso permitido.
    Se a última data_hora (timestamptz) > agora, está em cooldown.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT data_hora
            FROM controle_tempo_avisos_coordenadores
            WHERE id_alarme = %s
            ORDER BY data_hora DESC
            LIMIT 1
        """, (id_alarme,))
        row = cur.fetchone()

    agora = datetime.now(timezone.utc)
    last_dt = row[0] if row else None
    if last_dt:
        last_dt = last_dt.astimezone(timezone.utc)

    if last_dt and last_dt > agora:
        return False, last_dt

    proximo = agora + timedelta(minutes=cooldown_minutos)
    return True, proximo

def registrar_proximo_disparo(conn, id_alarme: int, proximo_dt: datetime):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO controle_tempo_avisos_coordenadores (data_hora, id_alarme)
            VALUES (%s, %s)
        """, (proximo_dt, id_alarme))
    conn.commit()

# ========= Helpers GPT =========
def _get_client():
    return OpenAI(api_key=os.getenv("TOKEN_GPT"))

def _get_last_text(conn, id_alarme: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT texto
            FROM {TABLE_TXT}
            WHERE id_alarme = %s
            ORDER BY data_hora DESC
            LIMIT 1
        """, (id_alarme,))
        row = cur.fetchone()
        return row[0] if row else None

def _save_text(conn, id_alarme: int, texto: str):
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {TABLE_TXT} (texto, data_hora, id_alarme)
            VALUES (%s, %s, %s)
        """, (texto, datetime.now(timezone.utc), id_alarme))
    conn.commit()

def _gen_once(template_padrao: str, vazao_media: float, temperature: float = 0.8) -> str:
    client = _get_client()
    prompt = f"""
Reescreva o texto abaixo de forma única, natural e profissional em PT-BR,
substituindo 'XXX m³/h' por {vazao_media:.2f} m³/h. Varie vocabulário e estrutura
para não ser idêntico ao original, mantendo o mesmo sentido e tom. Evite clichês de IA.

Texto original:
{template_padrao}
"""
    resp = client.chat.completions.create(
        model=MODEL,
        temperature=temperature,
        messages=[{"role": "user", "content": prompt}],
    )
    return (resp.choices[0].message.content or "").strip()

def gerar_mensagem_unica_e_salvar(conn, id_alarme: int, template_padrao: str, vazao_media: float,
                                  similaridade_max: float = SIMILARIDADE_MAX, tentativas: int = TENTATIVAS_GERACAO) -> str:
    """Gera mensagem única (não parecida com a última desse alarme) e salva em TABLE_TXT."""
    ultimo = _get_last_text(conn, id_alarme) or ""
    melhor_texto = None
    menor_sim = 1.0

    for i in range(tentativas):
        temp = 0.8 + i * 0.1  # aumenta a diversidade a cada tentativa
        texto = _gen_once(template_padrao, vazao_media, temperature=temp)
        # failsafe: se o modelo não trocar, força a troca do marcador
        texto = texto.replace("XXX m³/h", f"{vazao_media:.2f} m³/h")

        if not texto:
            continue

        sim = difflib.SequenceMatcher(None, texto, ultimo).ratio() if ultimo else 0.0
        if sim < similaridade_max:
            _save_text(conn, id_alarme, texto)
            return texto

        if sim < menor_sim:
            menor_sim = sim
            melhor_texto = texto

    # Se não passou do limiar, salva a menos parecida mesmo assim
    if melhor_texto:
        _save_text(conn, id_alarme, melhor_texto)
        return melhor_texto

    raise RuntimeError("Não foi possível gerar texto válido.")

def gerar_grafico_trend_base64(items: list[dict], titulo: str | None = None) -> str:
    if not items:
        return ""

    xs, ys = [], []
    for it in items:
        v = it.get("Value")
        ts = it.get("Timestamp")
        if not isinstance(v, (int, float)) or not isinstance(ts, str):
            continue
        try:
            d = dt.fromisoformat(ts.replace("Z", "+00:00")) - timedelta(hours=3)
        except Exception:
            continue
        xs.append(d)
        ys.append(float(v))

    if not xs:
        return ""

    fig = plt.figure(figsize=(8, 3))
    plt.plot(xs, ys, marker='o', linewidth=1.8)
    plt.xlabel("Tempo")
    plt.ylabel("Vazão (m³/h)")

    # Formata o eixo X para mostrar só hora:minuto
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Rotaciona para não sobrepor
    plt.xticks(rotation=45, ha='right')

    if titulo:
        plt.title(titulo)

    plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)

    # Adiciona folga no topo do gráfico
    ymin, ymax = plt.ylim()
    plt.ylim(ymin, ymax + (ymax - ymin) * 0.15)

    # Adiciona os valores acima dos pontos
    for x, y in zip(xs, ys):
        plt.annotate(f"{y:.0f}",
                     (x, y),
                     textcoords="offset points",
                     xytext=(0, 8),
                     ha='center',
                     fontsize=8)

    plt.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    img_b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{img_b64}"

# ========= Tarefas =========
def buscar_variaveis_avisos(**kwargs):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM variaveis_avisos_coordenadores
            """)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            variaveis = [dict(zip(colnames, row)) for row in rows]
        return variaveis  # vira XCom
    finally:
        conn.close()

def verificar_valor_tag(webid: str):
    url = os.getenv("URL_PI")
    token = os.getenv("TOKEN_PI")
    headers = {
        "Cache-Control": "no-cache",
        "Ocp-Apim-Subscription-Key": token,
    }
    params = {
        "startTime": "*-2h",
        "endTime": "*",
        "interval": "10m",
    }
    return requests.get(
        url=f"{url}/streams/{webid}/interpolated",
        headers=headers,
        params=params,
        timeout=30,
    )

def consultar_valores_pi(**kwargs):
    ti = kwargs["ti"]
    variaveis = ti.xcom_pull(task_ids="buscar_variaveis_avisos") or []
    if not variaveis:
        print("Nenhuma variável encontrada.")
        return

    conn = get_conn()
    try:
        for var in variaveis:
            descritivo_alarme = var.get("descritivo")
            webid = var.get("webid")
            valor_meta = var.get("valor_meta")
            id_alarme = var.get("id")
            horas_aviso = var.get("horas_aviso")     # se quiser usar cooldown da tabela
            template_padrao = var.get("texto_padrao")
            assunto_email = var.get("assunto_email")

            if not webid or valor_meta is None or id_alarme is None:
                print(f"⚠️ Registro inválido em variaveis_avisos_coordenadores: {var}")
                continue

            resp = verificar_valor_tag(webid)
            if resp.status_code != 200:
                print(f"❌ PI {webid}: {resp.status_code}")
                continue

            data = resp.json()
            itens = data.get("Items", [])
            valores = [it.get("Value") for it in itens if isinstance(it.get("Value"), (int, float))]

            if not valores:
                print(f"Sem valores numéricos para {id_alarme}")
                continue

            media = sum(valores) / len(valores)

            if media > valor_meta:
                cooldown_min = int(horas_aviso*60)
                pode, proximo = pode_disparar(conn, id_alarme, cooldown_min)
                if pode:
                    print(f"🚨 Aviso id_alarme={id_alarme} disparado. Silêncio até {proximo.isoformat()}")

                    # 1) Mensagem única e salva
                    msg = gerar_mensagem_unica_e_salvar(
                        conn=conn,
                        id_alarme=id_alarme,
                        template_padrao=template_padrao,
                        vazao_media=media,
                        similaridade_max=SIMILARIDADE_MAX,
                        tentativas=TENTATIVAS_GERACAO,
                    )
                    print("📨 Mensagem gerada:\n", msg)

                    # 2) Gráfico base64 usando a série interpolada
                    grafico_b64 = gerar_grafico_trend_base64(
                        itens,
                        titulo = descritivo_alarme
                    )
                    if grafico_b64:
                        enviar_email(imagem=grafico_b64,texto=msg,assunto=assunto_email)
                    registrar_proximo_disparo(conn, id_alarme, proximo)
                else:
                    print(f"⏳ Em cooldown até {proximo.isoformat()} (id_alarme={id_alarme})")
            else:
                print("Dentro da meta, sem aviso.")
    finally:
        conn.close()

# ========= DAG =========
with DAG(
    dag_id="avisos_coordenador",
    schedule_interval=None,
    start_date=datetime(2025, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["aviso", "ia", "coordenacao"],
) as dag:
    task_buscar_variaveis = PythonOperator(
        task_id="buscar_variaveis_avisos",
        python_callable=buscar_variaveis_avisos,
    )

    task_consultar_pi = PythonOperator(
        task_id="consultar_valores_pi",
        python_callable=consultar_valores_pi,
    )

    task_buscar_variaveis >> task_consultar_pi
