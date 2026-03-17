import sqlite3
import csv
import sys
sys.stdout.reconfigure(encoding='utf-8')

db = sqlite3.connect("bdd/bdd_mesures_pollution.db")
curs = db.cursor()

curs.execute("DROP TABLE IF EXISTS mesures_occitanie_journaliere_pollution")

curs.executescript("""
    CREATE TABLE IF NOT EXISTS mesures_occitanie_journaliere_pollution (
        nom_dept varchar(255) NOT NULL,
        nom_com varchar(255) NOT NULL,
        code_insee_com varchar(5) NOT NULL,
        nom_station varchar(255) NOT NULL,
        code_station varchar(10) NOT NULL,
        typologie varchar(255) NOT NULL,
        influence varchar(255) NOT NULL,
        nom_poll varchar(255) NOT NULL,
        valeur_poll REAL NOT NULL,
        jour INT NOT NULL,
        mois INT NOT NULL,
        annee INT NOT NULL
    )
""")

with open("data/mesures_occitanie_journaliere_pollution.csv", newline='', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    next(reader)
    data = [tuple(row) for row in reader]
    curs.executemany("""
        INSERT INTO mesures_occitanie_journaliere_pollution VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)

db.commit()

curs.execute("SELECT COUNT(*) FROM mesures_occitanie_journaliere_pollution")
print("Nombre de lignes :", curs.fetchone()[0])

curs.execute("PRAGMA table_info(mesures_occitanie_journaliere_pollution)")
for col in curs.fetchall():
    print(col)

curs.execute("SELECT * FROM mesures_occitanie_journaliere_pollution LIMIT 5")
for l in curs.fetchall():
    print(l)

curs.close()
db.close()
print("Import mesures_pollution terminé !")
