import sqlite3 
import sys
import csv 

sys.stdout.reconfigure(encoding='utf-8')

# Problématique 3 : Existe-t-il un lien entre le niveau de vie d'une commune et sa pollution ?

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

# On supprime la table si elle existe deja
curs.execute("DROP TABLE IF EXISTS pollution_niveau_vie_11")

# On cree une table avec la moyenne de pollution par commune + niveau de vie
# Jointure entre mesures et socio_economiques sur code_insee_com
curs.executescript("""
    CREATE TABLE pollution_niveau_vie_11 AS
    SELECT m.nom_com,
           m.code_insee_com,
           m.nom_dept,
           ROUND(AVG(m.valeur_poll), 2) AS moyenne_pollution,
           s.niveau_vie_median_2021,
           s.population_municipale_2023
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_socio_economiques s ON m.code_insee_com = s.code_insee_com
    GROUP BY m.code_insee_com
""")
db.commit()

# On affiche le resultat
curs.execute("SELECT COUNT(*) FROM pollution_niveau_vie_11")
print("Nombre de communes :", curs.fetchone()[0])

print("\nContenu de la table :")
curs.execute("SELECT * FROM pollution_niveau_vie_11 ORDER BY moyenne_pollution DESC")
lignes = curs.fetchall()
for l in lignes:
    print(l)

# On exporte en CSV pour R
export_table_to_csv("pollution_niveau_vie_11","pollution_niveau_vie_11.csv")
print("ruessi")
curs.close()
db.close()