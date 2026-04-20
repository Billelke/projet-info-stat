import sqlite3
import pandas as pd
import os.path

# Script de creation de la base de donnees pour le projet pollution

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOSSIER_DATA = os.path.join(BASE_DIR, "data")
DOSSIER_BDD = os.path.join(BASE_DIR, "bdd")

F_POLLUTION = os.path.join(DOSSIER_DATA, "mesures_occitanie_journaliere_pollution.csv")
F_GEO = os.path.join(DOSSIER_DATA, "donnees_geo_climatiques.csv")
F_SOCIO = os.path.join(DOSSIER_DATA, "donnees_socio_economiques.csv")
NOM_BDD = os.path.join(DOSSIER_BDD, "bdd_pollution_occitanie.db")

print("Lecture des fichiers...")

df_pollution = pd.read_csv(F_POLLUTION)
df_geo = pd.read_csv(F_GEO, low_memory=False)
df_socio = pd.read_csv(F_SOCIO)

print("pollution:", len(df_pollution), "lignes")
print("geo:", len(df_geo), "lignes")
print("socio:", len(df_socio), "lignes")

# On met tout en Int64 pour les jointures
print("\nConversion des codes INSEE...")

df_pollution['code_insee_com'] = pd.to_numeric(
    df_pollution['code_insee_com'], errors='coerce'
).astype('Int64')

df_geo['code_insee_com'] = pd.to_numeric(
    df_geo['code_insee_com'], errors='coerce'
).astype('Int64')

df_socio['code_insee_com'] = pd.to_numeric(
    df_socio['code_insee_com'], errors='coerce'
).astype('Int64')

# Petite verif des correspondances
codes_poll = set(df_pollution['code_insee_com'].dropna().unique())
codes_geo = set(df_geo['code_insee_com'].dropna().unique())
codes_socio = set(df_socio['code_insee_com'].dropna().unique())
print("codes communs pollution/geo:", len(codes_poll & codes_geo))
print("codes communs pollution/socio:", len(codes_poll & codes_socio))

print("\nCreation de la BDD:", NOM_BDD)

conn = sqlite3.connect(NOM_BDD)

df_pollution.to_sql("mesures_pollution", conn, if_exists="replace", index=False)
df_geo.to_sql("donnees_geo_climatiques", conn, if_exists="replace", index=False)
df_socio.to_sql("donnees_socio_economiques", conn, if_exists="replace", index=False)

print("3 tables creees")

# ETUDE UNIVARIEE 
print("\nCreation table etude univariee...")

req_univariate = """
SELECT
    nom_poll                           AS polluant,
    COUNT(*)                           AS nb_observations,
    ROUND(AVG(valeur_poll), 2)         AS moyenne,
    ROUND(MIN(valeur_poll), 2)         AS minimum,
    ROUND(MAX(valeur_poll), 2)         AS maximum,
    ROUND(AVG(valeur_poll), 2)         AS mediane,
    ROUND(
        SQRT(
            AVG((valeur_poll - (SELECT AVG(valeur_poll) FROM mesures_pollution)) * 
                (valeur_poll - (SELECT AVG(valeur_poll) FROM mesures_pollution)))
        ), 2
    ) AS ecart_type,
    COUNT(DISTINCT code_insee_com)     AS nb_communes,
    COUNT(DISTINCT nom_station)        AS nb_stations,
    COUNT(DISTINCT annee)              AS nb_annees
FROM mesures_pollution
GROUP BY nom_poll
ORDER BY nom_poll
"""
table_univariate = pd.read_sql_query(req_univariate, conn)
table_univariate.to_csv(os.path.join(DOSSIER_BDD, "table_analyse_univariee.csv"), index=False)
print("Table etude univariee exportee:", len(table_univariate), "lignes")

# Requete 1
req1 = """
SELECT
    p.nom_dept,
    p.nom_com,
    p.code_insee_com,
    p.nom_poll                     AS polluant,
    ROUND(AVG(p.valeur_poll), 2)   AS concentration_moyenne,
    ROUND(MIN(p.valeur_poll), 2)   AS concentration_min,
    ROUND(MAX(p.valeur_poll), 2)   AS concentration_max,
    COUNT(*)                       AS nb_mesures
FROM mesures_pollution p
GROUP BY p.code_insee_com, p.nom_poll
ORDER BY p.nom_dept, p.nom_com, p.nom_poll
"""
table_1 = pd.read_sql_query(req1, conn)
table_1.to_csv(os.path.join(DOSSIER_BDD, "table_stats_par_commune_polluant.csv"), index=False)
print("Table 1 exportee:", len(table_1), "lignes")

# Requete P1
req_p1 = """
SELECT
    p.nom_dept,
    p.nom_com,
    p.code_insee_com,
    p.nom_station,
    p.typologie,
    p.influence,
    p.nom_poll                AS polluant,
    p.valeur_poll             AS concentration,
    p.jour,
    p.mois,
    p.annee,
    g.alti_med                AS altitude,
    g.Tmin_med                AS temperature_min,
    g.Tmax_med                AS temperature_max,
    g.Force_vent_med          AS force_vent,
    g.RR_med                  AS precipitations,
    g.Insolation_med          AS insolation,
    g.Rayonnement_med         AS rayonnement,
    g.Tens_vap_med            AS tension_vapeur,
    g.densite_cat             AS type_territoire
FROM mesures_pollution p
LEFT JOIN donnees_geo_climatiques g
    ON p.code_insee_com = g.code_insee_com
ORDER BY p.nom_com, p.nom_poll, p.annee, p.mois, p.jour
"""
table_p1 = pd.read_sql_query(req_p1, conn)
table_p1.to_csv(os.path.join(DOSSIER_BDD, "table_climat_pollution.csv"), index=False)
print("Table P1 exportee:", len(table_p1), "lignes")

# Requete 3
req3 = """
SELECT
    p.nom_dept,
    p.code_insee_com,
    p.nom_com,
    p.nom_poll                    AS polluant,
    p.annee,
    p.mois,
    ROUND(AVG(p.valeur_poll), 2)  AS conc_moyenne,
    ROUND(MIN(p.valeur_poll), 2)  AS conc_min,
    ROUND(MAX(p.valeur_poll), 2)  AS conc_max,
    COUNT(*)                      AS nb_mesures
FROM mesures_pollution p
GROUP BY p.nom_dept, p.code_insee_com, p.nom_poll, p.annee, p.mois
ORDER BY p.nom_dept, p.nom_poll, p.annee, p.mois
"""
table_3 = pd.read_sql_query(req3, conn)
table_3.to_csv(os.path.join(DOSSIER_BDD, "table_pollution_par_departement.csv"), index=False)
print("Table 3 exportee:", len(table_3), "lignes")

# Requete 4
req4 = """
SELECT
    p.nom_dept,
    p.nom_com,
    p.code_insee_com,
    p.nom_poll                    AS polluant,
    ROUND(AVG(p.valeur_poll), 2)  AS conc_moyenne,
    s.niveau_vie_median_2021      AS niveau_vie,
    s.Pourcentage_appartements_2022   AS pct_appartements,
    s.population_municipale_2023  AS population
FROM mesures_pollution p
LEFT JOIN donnees_socio_economiques s
    ON p.code_insee_com = s.code_insee_com
GROUP BY p.code_insee_com, p.nom_poll
ORDER BY p.nom_com, p.nom_poll
"""
table_4 = pd.read_sql_query(req4, conn)
table_4.to_csv(os.path.join(DOSSIER_BDD, "table_pollution_socioeco.csv"), index=False)
print("Table 4 exportee:", len(table_4), "lignes")

print("\nResume final")
cursor = conn.cursor()
for table in ["mesures_pollution", "donnees_geo_climatiques", "donnees_socio_economiques"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    n = cursor.fetchone()[0]
    print(table, ":", n, "lignes")

conn.close()
print("\nTermine, base fermee")
print("CSV crees !")