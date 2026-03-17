# Dossier de soutenance (mi-parcours)

## 1) Base de données (BDD)

### Objectif
Construire une base relationnelle unique permettant de relier la pollution de l'air, le contexte géo-climatique et les indicateurs socio-économiques des communes.

### Sources utilisées
- `data/mesures_occitanie_journaliere_pollution.csv`
- `data/donnees_geo_climatiques.csv`
- `data/donnees_socio_economiques.csv`

### Tables créées
- `mesures_occitanie_journaliere_pollution` : mesures journalières par station/commune/polluant.
- `donnees_geo_climatiques` : caractéristiques géographiques et climatiques (température, vent, pluie, etc.).
- `donnees_socio_economiques` : niveau de vie, logements, population, activité, etc.

### Clé de jointure
- `code_insee_com` (code commune), utilisé pour relier les 3 tables.

### Implémentation technique
- Script principal : `scripts/bdd_complete.py`
- SGBD : SQLite
- Base générée : `bdd/pollution_occitanie.db`

### Requête d'extraction utilisée pour la problématique choisie
Script : `prob1.py`

```sql
CREATE TABLE pollution_niveau_vie_11 AS
SELECT m.nom_com,
       m.code_insee_com,
       m.nom_dept,
       ROUND(AVG(m.valeur_poll), 2) AS moyenne_pollution,
       s.niveau_vie_median_2021,
       s.population_municipale_2023
FROM mesures_occitanie_journaliere_pollution m
INNER JOIN donnees_socio_economiques s ON m.code_insee_com = s.code_insee_com
GROUP BY m.code_insee_com;
```

Cette table est ensuite exportée en CSV : `pollution_niveau_vie_11.csv`.

---

## 2) Problématique traitée

### Question statistique
**Existe-t-il un lien entre le niveau de vie d'une commune et sa pollution moyenne ?**

### Variables étudiées
- Variable 1 : `niveau_vie_median_2021` (quantitative)
- Variable 2 : `moyenne_pollution` (quantitative)

### Méthode descriptive (bivariée)
1. Nuage de points `niveau_vie_median_2021` vs `moyenne_pollution`
2. Coefficient de corrélation de Pearson
3. Comparaison des moyennes de pollution par quartiles de niveau de vie

---

## 3) Résultats obtenus

Calculs réalisés sur `pollution_niveau_vie_11.csv` :

- Taille de l'échantillon : **34 communes**
- Corrélation de Pearson : **r = 0,4229**
- Pollution moyenne globale : **27,12**
- Niveau de vie médian moyen : **21 698,2**
- Pollution moyenne par quartile de niveau de vie :
  - Q1 (plus faible niveau de vie) : **21,04**
  - Q2 : **22,03**
  - Q3 : **23,39**
  - Q4 (plus haut niveau de vie) : **39,04**

### Interprétation (solution à la problématique)
Les résultats indiquent **une relation positive modérée** entre niveau de vie médian et pollution moyenne communale (r ≈ 0,42). Dans cet échantillon, les communes appartenant au quartile de niveau de vie le plus élevé présentent une pollution moyenne plus forte.

### Limites (à dire à l'oral)
- Lien observé ≠ causalité.
- Échantillon réduit (34 communes dans la table extraite).
- Moyenne de pollution agrégée sur plusieurs polluants et périodes.
- D'autres facteurs peuvent intervenir (trafic, typologie urbaine, météo locale).

---

## 4) Ce qu'on annonce pour la suite (sans solution détaillée)

Problématiques complémentaires posées :
1. Les conditions climatiques (température, vent, pluie) influencent-elles la pollution ?
2. La pollution varie-t-elle selon le type de polluant ?
3. La pollution diffère-t-elle selon la densité/typologie des communes ?

Ces problématiques seront traitées dans la seconde phase du projet.
