import sqlite3
import csv
import sys
sys.stdout.reconfigure(encoding='utf-8')

db = sqlite3.connect("bdd/bdd_socio_economiques.db")
curs = db.cursor()

curs.execute("DROP TABLE IF EXISTS donnees_socio_economiques")

curs.executescript("""
    CREATE TABLE IF NOT EXISTS donnees_socio_economiques (
        code_insee_com varchar(5) PRIMARY KEY,
        nom_com varchar(255) NOT NULL,
        niveau_vie_median_2021 REAL,
        nb_logements_2022 INT,
        Pourcentage_appartements_2022 REAL,
        pourcentage_locataires_residence_principale_2022 REAL,
        evolution_annuelle_moy_population_2017_2023 REAL,
        population_municipale_2023 INT,
        Taux_activite_tranche_15_64_en_2022 REAL
    )
""")

with open("data/donnees_socio_economiques.csv", newline='', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    next(reader)
    data = [tuple(row) for row in reader]
    curs.executemany("""
        INSERT INTO donnees_socio_economiques VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)

db.commit()

curs.execute("SELECT COUNT(*) FROM donnees_socio_economiques")
print("Nombre de lignes :", curs.fetchone()[0])

curs.execute("PRAGMA table_info(donnees_socio_economiques)")
for col in curs.fetchall():
    print(col)

curs.execute("SELECT * FROM donnees_socio_economiques LIMIT 5")
for l in curs.fetchall():
    print(l)

curs.close()
db.close()
print("Import socio_economiques terminé !")
