import sqlite3
import csv
import sys
sys.stdout.reconfigure(encoding='utf-8')

db = sqlite3.connect("bdd/bdd_geo_climatiques.db")
curs = db.cursor()

curs.execute("DROP TABLE IF EXISTS donnees_geo_climatiques")

curs.executescript("""
    CREATE TABLE IF NOT EXISTS donnees_geo_climatiques (
        code_insee_com varchar(5) PRIMARY KEY,
        nom_com varchar(255) NOT NULL,
        reg_code INT,
        reg_nom varchar(255),
        dep_code varchar(3),
        dep_nom varchar(255),
        population INT,
        superficie_km2 REAL,
        densite REAL,
        latitude REAL,
        longitude REAL,
        densite_cat varchar(255),
        alti_med REAL,
        RR_med REAL,
        NBJRR1_med INT,
        NBJRR5_med INT,
        NBJRR10_med INT,
        Tmin_med REAL,
        Tmax_med REAL,
        Tens_vap_med REAL,
        Force_vent_med REAL,
        Insolation_med REAL,
        Rayonnement_med REAL
    )
""")

with open("data/donnees_geo_climatiques.csv", newline='', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    next(reader)
    data = [tuple(row) for row in reader]
    curs.executemany("""
        INSERT INTO donnees_geo_climatiques VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)

db.commit()

curs.execute("SELECT COUNT(*) FROM donnees_geo_climatiques")
print("Nombre de lignes :", curs.fetchone()[0])

curs.execute("PRAGMA table_info(donnees_geo_climatiques)")
for col in curs.fetchall():
    print(col)

curs.execute("SELECT * FROM donnees_geo_climatiques LIMIT 5")
for l in curs.fetchall():
    print(l)

curs.close()
db.close()
print("Import geo_climatiques terminé !")
