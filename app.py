import streamlit as st
import sqlite3
import pandas as pd

# Chemin de la base de données
DB_PATH = '/opt/airflow/data/accidents.db'

# Fonction pour charger les données depuis SQLite
def load_data(table_name, start=0, limit=30):
    """Charge les données depuis une table SQLite avec pagination."""
    try:
        conn = sqlite3.connect(DB_PATH)
        query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {start}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement des données : {e}")
        return pd.DataFrame()

# Configuration de la page Streamlit
st.set_page_config(page_title="Tableaux et Graphiques", layout="wide")

# Barre latérale pour la navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à", ["Visualiser les tables", "Graphiques"])

# Page : Visualiser les tables
if page == "Visualiser les tables":
    st.title("Visualisation des Tables")
    table_options = ["integrated_data", "preprocessed_data", "final_data"]
    selected_table = st.selectbox("Choisissez une table à afficher :", table_options)

    if st.button("Afficher la table"):
        if selected_table:
            # Initialisation de la pagination
            start = st.session_state.get("start", 0)
            data = load_data(selected_table, start=start, limit=30)
            if data.empty:
                st.warning(f"La table '{selected_table}' est vide ou n'existe pas.")
            else:
                st.dataframe(data)

                # Pagination : boutons "Précédent" et "Suivant"
                col1, col2 = st.columns([1, 1])
                if col1.button("Précédent") and start >= 30:
                    st.session_state["start"] = start - 30
                if col2.button("Suivant"):
                    st.session_state["start"] = start + 30

# Page : Graphiques
elif page == "Graphiques":
    st.title("Graphiques")

    # Charger les données finales
    data = load_data("final_data")
    if data.empty:
        st.warning("La table 'final_data' est vide ou n'existe pas.")
    else:
        # Graphique 1 : Distribution des âges des conducteurs
        st.subheader("Distribution des âges des conducteurs")
        if 'age_of_driver' in data.columns:
            st.bar_chart(data['age_of_driver'].value_counts().sort_index())
        else:
            st.warning("La colonne 'age_of_driver' est absente de la table.")