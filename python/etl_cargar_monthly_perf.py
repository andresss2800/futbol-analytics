import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# -------------------------------------------------
# Cargar variables
# -------------------------------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "futbol_portafolio")

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

EXCEL_PATH = r"C:\Users\OmarAndresMontanezMu\OneDrive - ASOCIACION DE FIDUCIARIAS\Documentos\futbol-analytics\data_raw\CONVOCATORIAS.xlsx"


# -------------------------------------------------
# Función carga a monthly player performance
# -------------------------------------------------
def cargar_fact_monthly_player_perf(xls):
    print("Cargando fact_monthly_player_perf (rendimiento mensual por jugador)...")

    # --------------------
    # a) Hoja RENDIMIENTO
    # --------------------
    df_r = pd.read_excel(xls, sheet_name="Rendimiento")
    print("\nColumnas hoja Rendimiento:")

    print(df_r.columns.tolist())

    
    rename_r = {
        "Jugador": "player_name",
    }
    df_r = df_r.rename(columns=rename_r)

    # Pivotear las columnas de meses
    posibles_meses = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
    ]
    meses_r = [m for m in posibles_meses if m in df_r.columns]

    
    r_long = df_r.melt(
        id_vars=["player_name"],
        value_vars=meses_r,
        var_name="month_name",
        value_name="avg_rating",
    )

    # --------------------
    # b) Hoja PARTIDOS
    # --------------------
    df_p = pd.read_excel(xls, sheet_name="Partidos")
    print("\nColumnas hoja Partidos:")
    print(df_p.columns.tolist())

    rename_p = {
        "Jugador": "player_name",
    }
    df_p = df_p.rename(columns=rename_p)

    # Pivotear las columnas de meses

    meses_p = [m for m in posibles_meses if m in df_p.columns]

    p_long = df_p.melt(
        id_vars=["player_name"],
        value_vars=meses_p,
        var_name="month_name",
        value_name="matches_played",
    )

    # --------------------
    # c) Unir rendimiento y partidos por jugador
    # --------------------
    df = r_long.merge(
        p_long,
        on=["player_name", "month_name"],
        how="outer"
    )

    # --------------------
    # d) Combinar nombre de mes -> número de mes
    # --------------------
    month_map = {
        "Enero": 1,
        "Febrero": 2,
        "Marzo": 3,
        "Abril": 4,
        "Mayo": 5,
        "Junio": 6,
        "Julio": 7,
        "Agosto": 8,
        "Septiembre": 9,
        "Octubre": 10,
        "Noviembre": 11,
        "Diciembre": 12,
    }

    df["month"] = df["month_name"].map(month_map)

    
    df["year"] = 2025

    # --------------------
    # e) Combinar player_name -> player_id (desde dim_player)
    # --------------------
    dim_player = pd.read_sql("SELECT player_id, player_name FROM dim_player", con=engine)
    df = df.merge(dim_player, on="player_name", how="left")

    sin_id = df["player_id"].isna().sum()
    print(f"\nFilas sin player_id (nombres que no matchean dim_player): {sin_id}")

    # Quitar filas sin player_id o sin mes
    
    df = df.dropna(subset=["player_id", "month"])

    # Formatear columnas

    df["player_id"] = df["player_id"].astype(int)
    df["month"] = df["month"].astype(int)
    df["year"] = df["year"].astype(int)

    df["avg_rating"] = pd.to_numeric(df["avg_rating"], errors="coerce")
    df["matches_played"] = pd.to_numeric(df["matches_played"], errors="coerce")

    # Eliminar  filas donde no hay ni rating ni partidos

    df = df.dropna(subset=["avg_rating", "matches_played"], how="all")

    # --------------------
    # f) Seleccionar columnas finales para fact_monthly_player_perf
    # --------------------

    fact = df[["player_id", "year", "month", "avg_rating", "matches_played"]].copy()

    print(f"\nRegistros a insertar en fact_monthly_player_perf: {len(fact)}")

    fact.to_sql("fact_monthly_player_perf", con=engine, if_exists="append", index=False)

    print("✔ Carga fact_monthly_player_perf completada.")

# Función que corre la función anterior

def main():
    xls = pd.ExcelFile(EXCEL_PATH)
    cargar_fact_monthly_player_perf(xls)


if __name__ == "__main__":
    main()
