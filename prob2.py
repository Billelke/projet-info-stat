import sqlite3 
import sys
import csv 

sys.stdout.reconfigure(encoding='utf-8')

# Problematique 4 : Les conditions climatiques sont-elles liees aux niveaux de pollution ?

db = sqlite3.connect("bdd/pollution_occitanie.db")
curs = db.cursor()

def export_table_to_csv(table_name, csv_name):
    curs.execute(f"SELECT * FROM {table_name};")
    rows = curs.fetchall()

    curs.execute(f"PRAGMA table_info({table_name});")
    cols = [c[1] for c in curs.fetchall()]

    with open(csv_name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

# table 21 : pollution moyenne par commune + temperature max
curs.execute("DROP TABLE IF EXISTS pollution_climat_21")
curs.executescript("""
    CREATE TABLE pollution_climat_21 AS
    SELECT m.nom_com,
           m.code_insee_com,
           m.nom_dept,
           ROUND(AVG(m.valeur_poll), 2) AS moyenne_pollution,
           g.Tmax_med
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_geo_climatiques g ON m.code_insee_com = g.code_insee_com
    GROUP BY m.code_insee_com
""")
db.commit()

curs.execute("SELECT COUNT(*) FROM pollution_climat_21")
print("Table 21 :", curs.fetchone()[0], "communes")
curs.execute("SELECT * FROM pollution_climat_21 LIMIT 5")
for l in curs.fetchall():
    print(l)

# table 22 : pollution moyenne par commune + force du vent
curs.execute("DROP TABLE IF EXISTS pollution_climat_22")
curs.executescript("""
    CREATE TABLE pollution_climat_22 AS
    SELECT m.nom_com,
           m.code_insee_com,
           m.nom_dept,
           ROUND(AVG(m.valeur_poll), 2) AS moyenne_pollution,
           g.Force_vent_med
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_geo_climatiques g ON m.code_insee_com = g.code_insee_com
    GROUP BY m.code_insee_com
""")
db.commit()

curs.execute("SELECT COUNT(*) FROM pollution_climat_22")
print("\nTable 22 :", curs.fetchone()[0], "communes")
curs.execute("SELECT * FROM pollution_climat_22 LIMIT 5")
for l in curs.fetchall():
    print(l)

# table 23 : pollution par polluant + temperature + vent
curs.execute("DROP TABLE IF EXISTS pollution_climat_23")
curs.executescript("""
    CREATE TABLE pollution_climat_23 AS
    SELECT m.nom_com,
           m.code_insee_com,
           m.nom_poll,
           ROUND(AVG(m.valeur_poll), 2) AS moyenne_pollution,
           g.Tmax_med,
           g.Force_vent_med
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_geo_climatiques g ON m.code_insee_com = g.code_insee_com
    GROUP BY m.code_insee_com, m.nom_poll
""")
db.commit()

curs.execute("SELECT COUNT(*) FROM pollution_climat_23")
print("\nTable 23 :", curs.fetchone()[0], "lignes")
curs.execute("SELECT * FROM pollution_climat_23 LIMIT 5")
for l in curs.fetchall():
    print(l)

# table 24 : toutes les variables climatiques
curs.execute("DROP TABLE IF EXISTS pollution_climat_24")
curs.executescript("""
    CREATE TABLE pollution_climat_24 AS
    SELECT m.nom_com,
           m.code_insee_com,
           m.nom_dept,
           m.nom_poll,
           ROUND(AVG(m.valeur_poll), 2) AS moyenne_pollution,
           g.Tmax_med,
           g.Tmin_med,
           g.Force_vent_med,
           g.RR_med,
           g.Insolation_med,
           g.Rayonnement_med
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_geo_climatiques g ON m.code_insee_com = g.code_insee_com
    GROUP BY m.code_insee_com, m.nom_poll
""")
db.commit()

curs.execute("SELECT COUNT(*) FROM pollution_climat_24")
print("\nTable 24 :", curs.fetchone()[0], "lignes")
curs.execute("SELECT * FROM pollution_climat_24 LIMIT 5")
for l in curs.fetchall():
    print(l)

# export en csv pour R
export_table_to_csv("pollution_climat_21", "pollution_climat_21.csv")
export_table_to_csv("pollution_climat_22", "pollution_climat_22.csv")
export_table_to_csv("pollution_climat_23", "pollution_climat_23.csv")
export_table_to_csv("pollution_climat_24", "pollution_climat_24.csv")
print("\ncsv exportes")

curs.close()
db.close()
print("reussi")
