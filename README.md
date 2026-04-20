# Projet Info-Stat - Pollution de l'air en Occitanie

## Presentation
Ce projet analyse la pollution de l'air en Occitanie a partir de donnees environnementales, climatiques et socio-economiques.

Le travail est structure en deux parties :
- preparation/assemblage des donnees avec Python ;
- analyse statistique et visualisation avec R Markdown.

## Structure du projet

- `data/` : fichiers sources CSV
  - `mesures_occitanie_journaliere_pollution.csv`
  - `donnees_geo_climatiques.csv`
  - `donnees_socio_economiques.csv`
- `scripts/` : scripts Python de construction de la base et des tables
  - `bdd_complete.py`
  - `bdd_mesures_pollution.py`
  - `bdd_geo_climatiques.py`
  - `bdd_socio_economiques.py`
- `bdd/` : tables d'analyse, base SQLite locale, rapports Rmd/html

## Prerequis

### Python
- Python 3.10+
- Packages : `pandas` (et `sqlite3`, inclus dans Python standard)

### R
- R + RStudio (ou `rmarkdown::render` en ligne de commande)
- Packages : `tidyverse`, `ggplot2`, `knitr`, `kableExtra`

## Execution

### 1) Preparation des donnees (Python)
Depuis la racine du projet :

```powershell
python scripts/bdd_complete.py
```

Ce script construit/alimente la base SQLite et exporte les tables CSV utilisees dans l'analyse.

### 2) Analyse et rapport (R Markdown)
Ouvrir puis executer :
- `bdd/rapport_final.Rmd`
- ou `bdd/rapport_pollution_occitanie_v2 .Rmd`

En ligne de commande R :

```r
rmarkdown::render("bdd/rapport_final.Rmd")
```

## Resultats
Le rapport repond a 4 problematiques principales :
1. effet des conditions climatiques sur la pollution ;
2. differences de pollution selon les departements ;
3. variabilite saisonniere des polluants ;
4. lien entre niveau de vie et pollution.

## Auteurs
- Bensmaine Yacine
- Kessaci Bilel
- Khelouat Amine
- Zouaoui Mohamed Elmehdi
