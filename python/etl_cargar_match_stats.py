import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# -------------------------------------------
# Cargar variables
# -------------------------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

EXCEL_PATH = r"C:\Users\OmarAndresMontanezMu\OneDrive - ASOCIACION DE FIDUCIARIAS\Documentos\futbol-analytics\data_raw\CONVOCATORIAS.xlsx"


# -------------------------------------------
# Función principal de carga
# -------------------------------------------

def cargar_fact_match_stats(xls):
    print("Cargando fact_match_stats (Estadísticas por partido)...")

    df = pd.read_excel(xls, sheet_name="Estadisticas")

    print("\nColumnas hoja Estadisticas:")
    print(df.columns.tolist())

    # ---------------------------------------------------------
    # Renombrar columnas
    # ---------------------------------------------------------
    rename_cols = {
        "Fecha": "match_id",
        "Jugador": "player_name",                   
        "Calificacion": "rating",
        "Minutos Jugados": "minutes_played",
        "Goles": "goals",
        "Tiros Totales": "total_shots",
        "Remates a Puerta": "shots_on_target",
        "Remates Fuera": "shots_off_target",
        "Remates al palo": "shots_hit_post",
        "Ocasiones Claras Falladas": "big_chances_missed",
        "Fueras de Juego": "offsides",
        "Asistencias": "assists",
        "Ocasiones Claras Creadas": "big_chances_created",
        "Pases Clave": "key_passes",
        "Centros efectivos": "accurate_crosses",
        "Centros Totales": "total_crosses",
        "Pases Precisos": "accurate_passes",
        "Pases Intentados": "attempted_passes",
        "Pases precisos en campo rival": "accurate_passes_opponent_half",
        "Pases en campo rival": "passes_opponent_half",
        "Pases precisos en campo propio": "accurate_passes_own_half",
        "Pases en campo propio": "passes_own_half",
        "Pases Largos completados": "long_balls_completed",
        "Pases Largos intentados": "long_balls_attempted",
        "Toques": "touches",
        "Toques Fallidos": "miscontrols",
        "Regates Ganados": "dribbles_won",
        "Regates Intentados": "dribbles_attempted",
        "Faltas Recibidas": "fouls_won",
        "Posesiones Perdidas": "possessions_lost",
        "Entradas ganadas": "tackles_won",
        "Entradas Totales": "tackles_total",
        "Interceptaciones": "interceptions",
        "Despejes": "clearances",
        "Recuperaciones": "recoveries",
        "Duelos en el suelo ganados": "ground_duels_won",
        "Duelos en el suelo": "ground_duels_total",
        "Duelos Aereos Ganados": "aerial_duels_won",
        "Duelos Aereos": "aerial_duels_total",
        "Faltas Cometidas": "fouls_committed",
        "Regateado": "times_dribbled_past",
        "Penalti cometido": "penalties_committed",
        "Penalti Provocado": "penalties_won",
        "Penalti Recibido": "penalties_received",
        "Salvadas": "saves",
        "Autogoles": "own_goals",
    }

    # Renombrar columnas

    df = df.rename(columns=rename_cols)

    # Quitar columnas duplicadas por nombre
    
    df = df.loc[:, ~df.columns.duplicated()]

    print("\nColumnas después de rename + drop duplicados:")
    print(df.columns.tolist())

    # ---------------------------------------------------------
    # renombrar player_name 
    # ---------------------------------------------------------
    if "player_name" not in df.columns:
        raise ValueError("No se encontró la columna 'player_name' después del rename. Verifica nombres.")

    df = df.rename(columns={"player_name": "player_name_stats"})

    # ---------------------------------------------------------
    # Combinar player_name_stats -> player_id desde dim_player
    # ---------------------------------------------------------
    dim_player = pd.read_sql("SELECT player_id, player_name FROM dim_player", con=engine)

    # Hacer merge usando nombres distintos izquierda/derecha

    df = df.merge(
        dim_player,
        left_on="player_name_stats",
        right_on="player_name",
        how="left",
    )

   
    # Comprobar cuántos no se combinaron:

    sin_match = df["player_id"].isna().sum()
    print(f"Filas sin player_id (no matchean nombre): {sin_match}")


    # Eliminar nombres de jugador
    for col_drop in ["player_name_stats", "player_name"]:
        if col_drop in df.columns:
            df = df.drop(columns=[col_drop])

    # Asegurar tipos numéricos para todas menos match_id y player_id
    numericas = [c for c in df.columns if c not in ["match_id", "player_id"]]
    for col in numericas:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ---------------------------------------------------------
    # Cargar a la tabla fact_match_stats
    # ---------------------------------------------------------
    df.to_sql("fact_match_stats", con=engine, if_exists="append", index=False)

    print(f"\nInsertados {len(df)} registros en fact_match_stats.")
    print("✔ Carga completada.")

# Función que se encarga de correr la función anterior

def main():
    xls = pd.ExcelFile(EXCEL_PATH)
    cargar_fact_match_stats(xls)


if __name__ == "__main__":
    main()
