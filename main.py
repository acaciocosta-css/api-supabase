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
# DESCOBRIR PK
# ===============================
def get_pk(conn, tabela):
    result = conn.execute(text("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = :tabela
        AND tc.constraint_type = 'PRIMARY KEY'
    """), {"tabela": tabela}).fetchone()

    if not result:
        raise HTTPException(status_code=400, detail="Tabela sem PK")

    return result[0]

# ===============================
# INSERT (POST)
# ===============================
@app.post("/{tabela}")
def inserir(tabela: str, dados: dict):

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

    return {"status": "inserido"}

# ===============================
# UPDATE (PUT)
# ===============================
@app.put("/{tabela}/{id}")
def atualizar(tabela: str, id: str, dados: dict):

    with engine.connect() as conn:
        validar_tabela(conn, tabela)
        pk = get_pk(conn, tabela)

        update_set = ", ".join([f"{k}=:{k}" for k in dados.keys()])

        dados[pk] = id

        query = text(f"""
            UPDATE {tabela}
            SET {update_set}
            WHERE {pk} = :{pk}
        """)

        with engine.begin() as trans:
            result = trans.execute(query, dados)

            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="Registro não encontrado")

    return {"status": "atualizado"}

# ===============================
# DELETE
# ===============================
@app.delete("/{tabela}/{id}")
def deletar(tabela: str, id: str):

    with engine.connect() as conn:
        validar_tabela(conn, tabela)
        pk = get_pk(conn, tabela)

        query = text(f"""
            DELETE FROM {tabela}
            WHERE {pk} = :id
        """)

        with engine.begin() as trans:
            result = trans.execute(query, {"id": id})

            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="Registro não encontrado")

    return {"status": "deletado"}
