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

@app.get("/")
def home():
    return {"status": "API ONLINE"}

@app.get("/tabelas")
def listar_tabelas():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
        """))
        return [row[0] for row in result]

TABELAS_PERMITIDAS = ["cad_projetos", "cad_canteiros", "cad_trechos"]

@app.get("/{tabela}")
def listar_dados(tabela: str):
    if tabela not in TABELAS_PERMITIDAS:
        raise HTTPException(status_code=403, detail="Tabela não permitida")

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {tabela} LIMIT 100"))
        return [dict(row._mapping) for row in result]
