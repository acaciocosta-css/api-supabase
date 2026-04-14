from fastapi import FastAPI, HTTPException, Query
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
# LISTAR TABELAS
# ===============================
@app.get("/tabelas")
def listar_tabelas():
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name
        """))
        return [row[0] for row in result]

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
# GET (LISTAR)
# ===============================
@app.get("/{tabela}")
def listar_dados(
    tabela: str,
    limite: int = Query(100, ge=1, le=1000)
):
    with engine.connect() as conn:
        validar_tabela(conn, tabela)

        result = conn.execute(
            text(f"SELECT * FROM {tabela} LIMIT :limite"),
            {"limite": limite}
        )

        return [dict(row._mapping) for row in result]

# ===============================
# POST (INSERIR)
# ===============================
@app.post("/{tabela}")
def inserir_dados(tabela: str, dados: dict):
    with engine.connect() as conn:
        validar_tabela(conn, tabela)

        colunas = ", ".join(dados.keys())
        valores = ", ".join([f":{k}" for k in dados.keys()])

        query = text(f"""
            INSERT INTO {tabela} ({colunas})
            VALUES ({valores})
        """)

        with engine.begin() as trans:
            trans.execute(query, dados)

        return {"status": "inserido com sucesso"}

# ===============================
# PUT (ATUALIZAR)
# ===============================
@app.put("/{tabela}/{id}")
def atualizar_dados(tabela: str, id: str, dados: dict):
    with engine.connect() as conn:
        validar_tabela(conn, tabela)

        pk = get_primary_key(conn, tabela)

        sets = ", ".join([f"{k} = :{k}" for k in dados.keys()])

        query = text(f"""
            UPDATE {tabela}
            SET {sets}
            WHERE {pk} = :id
        """)

        dados["id"] = id

        with engine.begin() as trans:
            trans.execute(query, dados)

        return {"status": "atualizado com sucesso"}

# ===============================
# DELETE
# ===============================
@app.delete("/{tabela}/{id}")
def deletar_dados(tabela: str, id: str):
    with engine.connect() as conn:
        validar_tabela(conn, tabela)

        pk = get_primary_key(conn, tabela)

        query = text(f"""
            DELETE FROM {tabela}
            WHERE {pk} = :id
        """)

        with engine.begin() as trans:
            trans.execute(query, {"id": id})

        return {"status": "deletado com sucesso"}
