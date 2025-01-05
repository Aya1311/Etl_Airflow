# Etl_Airflow
# Automatisation et Orchestration des Données avec Apache Airflow

Ce projet est une démonstration complète de l'utilisation d'Apache Airflow pour automatiser et orchestrer un pipeline ETL, suivi de la visualisation des données transformées à l'aide de Dash et Streamlit.

## Table des Matières

1. [Présentation](#présentation)
2. [Architecture](#architecture)
3. [Prérequis](#prérequis)
4. [Installation et Configuration](#installation-et-configuration)
5. [Exécution du Projet](#exécution-du-projet)
6. [Fonctionnalités](#fonctionnalités)
7. [Commandes Utiles](#commandes-utiles)
8. [Contributeurs](#contributeurs)

---

## Présentation

Ce projet a été conçu pour :

- **Orchestrer un pipeline ETL** : Extraction, transformation et chargement des données dans une base SQLite via Apache Airflow.
- **Visualiser les données** : Fournir des tableaux de bord interactifs à l'aide de Dash et Streamlit.
- **Simplifier le déploiement** : Conteneuriser tous les services avec Docker et Docker Compose.

---

## Architecture

L'architecture inclut les composants suivants :

- **Apache Airflow** : Orchestration du pipeline ETL.
- **PostgreSQL** : Stockage des métadonnées d'Airflow.
- **SQLite** : Base de données pour les données transformées.
- **Dash** : Visualisation interactive des relations entre variables.
- **Streamlit** : Exploration des données et affichage de graphiques.

---

## Prérequis

- **Systèmes nécessaires** :
  - Docker et Docker Compose installés.
  - Python 3.8 ou supérieur.
- **Outils recommandés** :
  - Git pour la gestion du code.

---

## Installation et Configuration

1. Clonez le dépôt :

   ```bash
   git clone https://github.com/votre-utilisateur/nom-du-repo.git
   cd nom-du-repo
   ```

2. Construisez les conteneurs Docker :

   ```bash
   docker-compose build
   ```

3. Initialisez Apache Airflow :

   ```bash
   docker-compose up airflow-init
   ```

---

## Exécution du Projet

1. Lancez tous les services :

   ```bash
   docker-compose up
   ```

2. Accédez aux interfaces :

   - **Airflow** : [http://localhost:8080](http://localhost:8080)
   - **Dash** : [http://localhost:8050](http://localhost:8050)
   - **Streamlit** : [http://localhost:8501](http://localhost:8501)

---

## Fonctionnalités

- **Apache Airflow** : Orchestration de toutes les étapes du pipeline ETL (Extraction, Nettoyage, Transformation, Chargement).
- **Dash** : Visualisation des relations entre les variables avec des graphiques interactifs.
- **Streamlit** : Exploration des tables SQLite et visualisations basées sur les données transformées.

---
## Commandes Utiles

- **Redémarrer les services Docker** :

  ```bash
  docker-compose restart
  ```

- **Supprimer les conteneurs et volumes** :

  ```bash
  docker-compose down --volumes
  ```

- **Accéder au conteneur Airflow CLI** :

  ```bash
  docker-compose run airflow-cli
  ```

---

## Contributeurs

- **Aya Laadaili**
