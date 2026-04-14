from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10
)

# ===============================
# TESTE
# ===============================
@app.get("/")
def home():
    return {"status": "API ONLINE"}

# ===============================
# VALIDAR TABELA
# ===============================
def validar_tabela(conn, tabela):
    result = conn.execute(text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema='public'
        AND table_name = :tabela
    """), {"tabela": tabela}).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Tabela não existe")

# ===============================
# DESCOBRIR CHAVE PRIMÁRIA
# ===============================
def get_primary_key(conn, tabela):
    result = conn.execute(text("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = :tabela
        AND tc.constraint_type = 'PRIMARY KEY'
    """), {"tabela": tabela}).fetchone()

    if not result:
        raise HTTPException(status_code=400, detail="Tabela sem chave primária")

    return result[0]

# ===============================
# API GENÉRICA (UPSERT)
# ===============================
@app.post("/api")
def api_dinamica(payload: dict):

    tabela = payload.get("tabela")
    dados = payload.get("dados")

    if not tabela or not dados:
        raise HTTPException(status_code=400, detail="Payload inválido")

    with engine.connect() as conn:
        validar_tabela(conn, tabela)
        pk = get_primary_key(conn, tabela)

        colunas = ", ".join(dados.keys())
        valores = ", ".join([f":{k}" for k in dados.keys()])

        update_set = ", ".join([f"{k}=EXCLUDED.{k}" for k in dados.keys()])

        query = text(f"""
            INSERT INTO {tabela} ({colunas})
            VALUES ({valores})
            ON CONFLICT ({pk}) DO UPDATE SET
            {update_set}
        """)

        with engine.begin() as trans:
            trans.execute(query, dados)

    return {"status": "ok"}
