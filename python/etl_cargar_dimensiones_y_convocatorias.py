import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ----------------------------
# Cargar variables de entorno
# ----------------------------

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "futbol_portafolio")

# Crear engine de SQLAlchemy para MySQL
engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

# Ruta al Excel fuente
EXCEL_PATH = r"C:\Users\OmarAndresMontanezMu\OneDrive - ASOCIACION DE FIDUCIARIAS\Documentos\futbol-analytics\data_raw\CONVOCATORIAS.xlsx"

# -----------------------------------------------------
# Función para cargar información a la tabla dim_player
# -----------------------------------------------------

def cargar_dim_player(xls):
    
    print("Cargando dim_player (Jugadores)...")
    
    df = pd.read_excel(xls, sheet_name="Jugadores")

    # Columnas disponibles del archivo
    print("Columnas Jugadores:", df.columns.tolist())

    # Renombrar columnas
    rename_cols = {
        "Jugador": "player_name",
        "Edad": "age",
        "Estatura (m)": "height_m",
        "Perfil": "foot",
        "Posicion": "position",
        "Posicion en campo": "field_role",
        "Lesiones Ultimo Año": "injuries_last_year",
        "Atacante": "attack_rating",
        "Creatividad": "creativity_rating",
        "Defensa": "defense_rating",
        "Táctica": "tactical_rating",
        "Técnica": "technical_rating",
        "Partidos Jugados Selección": "matches",
        "Goles Selección": "national_team_goals",
        "Partidos Temporada": "season_matches",
        "Partidos Titular": "season_starts",
        "Total Minutos": "season_minutes",
        "Valoracion Temporada": "season_rating",
        "Valor Mercado (M€)": "market_value_millions",
    }

    df = df.rename(columns=rename_cols)

    # Seleccionar las columnas que existan en el df

    df = df[[c for c in rename_cols.values() if c in df.columns]]

    # Quitar duplicados

    df = df.drop_duplicates(subset=["player_name"])

    # Cargar a MySQL

    df.to_sql("dim_player", con=engine, if_exists="append", index=False)

    print(f"dim_player: {len(df)} registros insertados.")

# -----------------------------------------------------
# Función para cargar información a la tabla dim_match
# -----------------------------------------------------

def cargar_dim_match(xls):
    print("Cargando dim_match (Fechas/Partidos)...")
    df = pd.read_excel(xls, sheet_name="Fechas")

    print("Columnas Fechas:", df.columns.tolist())

    # 🔧 Renombrar columnas
    rename_cols = {
        "Fecha": "match_id",             
        "Fecha Compromiso": "match_date",
        "Rival": "opponent",
        "Condicion": "home_away",
        "Resultado": "result",
    }

    df = df.rename(columns=rename_cols)

    # Seleccionar las columnas que existan en el df

    df = df[[c for c in rename_cols.values() if c in df.columns]]

    # Formato de fecha a la columna match_date

    if "match_date" in df.columns:

        df["match_date"] = pd.to_datetime(df["match_date"]).dt.date

    # Cargar a MySQL

    df.to_sql("dim_match", con=engine, if_exists="append", index=False)

    print(f"dim_match: {len(df)} registros insertados.")


# -----------------------------------------------------------------
# Función para cargar información a la tabla dim_club y fact_callup 
# -----------------------------------------------------------------

def cargar_dim_club_y_fact_callup(xls):

    print("Cargando dim_club y fact_callup (Convocatorias)...")
    
    df = pd.read_excel(xls, sheet_name="Convocatorias")

    print("Columnas Convocatorias:", df.columns.tolist())

    # 🔧 Renombrar columnas

    rename_cols = {
        "Jugador": "player_name",
        "Equipo": "club_name",
        "Pais": "country_code",
        "Fecha Convocatoria": "callup_date",
        "Numero Fecha Clasificación": "qualifier_round",
        "Fecha": "match_id",  # si esa columna existe y enlaza con dim_match
    }

    df = df.rename(columns=rename_cols)

    # 1) Cargar a dim_club los clubes únicos

    df_clubs = df[["club_name", "country_code"]].drop_duplicates()
    
    df_clubs = df_clubs.dropna(subset=["club_name"])

    # Cargar clubes nuevos a dim_club

    df_clubs.to_sql("dim_club", con=engine, if_exists="append", index=False)
    
    print(f"dim_club: {len(df_clubs)} registros insertados (posibles duplicados ignorados por la BD si tienes restricciones).")

    # 2) Combinar player_name -> player_id
    
    dim_player = pd.read_sql("SELECT player_id, player_name FROM dim_player", con=engine)
    
    df = df.merge(dim_player, on="player_name", how="left")

    # 3) Combinar club_name -> club_id
    
    dim_club = pd.read_sql("SELECT club_id, club_name FROM dim_club", con=engine)
    
    df = df.merge(dim_club, on="club_name", how="left")

    # 4) Preparar fact_callup

    cols_fact = ["callup_date", "qualifier_round", "match_id", "player_id", "club_id"]
    
    cols_fact = [c for c in cols_fact if c in df.columns]

    fact = df[cols_fact].copy()

    if "callup_date" in fact.columns:
    
        fact["callup_date"] = pd.to_datetime(fact["callup_date"]).dt.date

    # Quitar filas sin player_id
    
    fact = fact.dropna(subset=["player_id"])

    fact.to_sql("fact_callup", con=engine, if_exists="append", index=False)

    print(f"fact_callup: {len(fact)} registros insertados.")

# ----------------------------------------------------------------------
# Función main que hace el llamado a las funciones creadas anteriormente
# ----------------------------------------------------------------------

def main():
    print("Leyendo Excel:", EXCEL_PATH)
    xls = pd.ExcelFile(EXCEL_PATH)

    cargar_dim_player(xls)
    cargar_dim_match(xls)
    cargar_dim_club_y_fact_callup(xls)

    print("Proceso ETL completado.")


if __name__ == "__main__":
    main()