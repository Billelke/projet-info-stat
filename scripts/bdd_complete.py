import sqlite3
import csv
import sys
sys.stdout.reconfigure(encoding='utf-8')

db = sqlite3.connect("bdd/pollution_occitanie.db")
curs = db.cursor()

# On desactive les cles etrangeres pendant l'import
curs.execute("PRAGMA foreign_keys = OFF")

# ============================================================
# CREATION DES TABLES (LDD)
# ============================================================

curs.execute("DROP TABLE IF EXISTS mesures_occitanie_journaliere_pollution")
curs.execute("DROP TABLE IF EXISTS donnees_socio_economiques")
curs.execute("DROP TABLE IF EXISTS donnees_geo_climatiques")

# Table geo_climatiques (table de reference, pas de cle etrangere)
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
    );

    -- Table socio_economiques (cle etrangere vers geo_climatiques)
    CREATE TABLE IF NOT EXISTS donnees_socio_economiques (
        code_insee_com varchar(5) PRIMARY KEY,
        nom_com varchar(255) NOT NULL,
        niveau_vie_median_2021 REAL,
        nb_logements_2022 INT,
        Pourcentage_appartements_2022 REAL,
        pourcentage_locataires_residence_principale_2022 REAL,
        evolution_annuelle_moy_population_2017_2023 REAL,
        population_municipale_2023 INT,
        Taux_activite_tranche_15_64_en_2022 REAL,
        FOREIGN KEY (code_insee_com) REFERENCES donnees_geo_climatiques(code_insee_com)
    );

    -- Table mesures pollution (cle etrangere vers geo_climatiques)
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
        annee INT NOT NULL,
        FOREIGN KEY (code_insee_com) REFERENCES donnees_geo_climatiques(code_insee_com)
    );
""")

db.commit()
print("Tables creees avec succes.")

# ============================================================
# INSERTION DES DONNEES (LMD)
# ============================================================

# 1) geo_climatiques (on insere en premier car les autres la referencent)
print("\n--- Import donnees_geo_climatiques ---")
with open("data/donnees_geo_climatiques.csv", newline='', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    next(reader)
    data = [tuple(row) for row in reader]
    curs.executemany("INSERT INTO donnees_geo_climatiques VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
db.commit()
curs.execute("SELECT COUNT(*) FROM donnees_geo_climatiques")
print("Nombre de lignes :", curs.fetchone()[0])

# 2) socio_economiques
print("\n--- Import donnees_socio_economiques ---")
with open("data/donnees_socio_economiques.csv", newline='', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    next(reader)
    data = [tuple(row) for row in reader]
    curs.executemany("INSERT INTO donnees_socio_economiques VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
db.commit()
curs.execute("SELECT COUNT(*) FROM donnees_socio_economiques")
print("Nombre de lignes :", curs.fetchone()[0])

# 3) mesures pollution
print("\n--- Import mesures_occitanie_journaliere_pollution ---")
with open("data/mesures_occitanie_journaliere_pollution.csv", newline='', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    next(reader)
    data = [tuple(row) for row in reader]
    curs.executemany("INSERT INTO mesures_occitanie_journaliere_pollution VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
db.commit()
curs.execute("SELECT COUNT(*) FROM mesures_occitanie_journaliere_pollution")
print("Nombre de lignes :", curs.fetchone()[0])

# On reactive les cles etrangeres
curs.execute("PRAGMA foreign_keys = ON")

# ============================================================
# VERIFICATION DES JOINTURES (LID)
# ============================================================

print("\n--- Test jointure : mesures + geo_climatiques ---")
curs.execute("""
    SELECT m.nom_com, m.nom_poll, m.valeur_poll, g.dep_nom, g.population, g.Tmax_med
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_geo_climatiques g ON m.code_insee_com = g.code_insee_com
    LIMIT 5
""")
for l in curs.fetchall():
    print(l)

print("\n--- Test jointure : mesures + socio_economiques ---")
curs.execute("""
    SELECT m.nom_com, m.nom_poll, m.valeur_poll, s.niveau_vie_median_2021, s.population_municipale_2023
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_socio_economiques s ON m.code_insee_com = s.code_insee_com
    LIMIT 5
""")
for l in curs.fetchall():
    print(l)

print("\n--- Test jointure des 3 tables ---")
curs.execute("""
    SELECT m.nom_com, m.nom_poll, m.valeur_poll,
           g.dep_nom, g.Tmax_med,
           s.niveau_vie_median_2021
    FROM mesures_occitanie_journaliere_pollution m
    INNER JOIN donnees_geo_climatiques g ON m.code_insee_com = g.code_insee_com
    INNER JOIN donnees_socio_economiques s ON m.code_insee_com = s.code_insee_com
    LIMIT 5
""")
for l in curs.fetchall():
    print(l)

curs.close()
db.close()
print("\nBDD complete sauvegardee : bdd/pollution_occitanie.db")
