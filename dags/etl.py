import os  # Gestion des opérations sur le système de fichiers
import glob  # Recherche de fichiers correspondant à un motif
import pandas as pd  # Manipulation des données sous forme de DataFrame
import numpy as np  # Calculs numériques
import sqlite3  # Interaction avec une base de données SQLite
from datetime import timedelta  # Gestion des durées et délais
from airflow import DAG  # Définition des DAG (Directed Acyclic Graph) pour Airflow
from airflow.utils.dates import days_ago  # Gestion des dates relatives dans Airflow
from airflow.operators.python import PythonOperator  # Exécution de fonctions Python dans Airflow
from sklearn.preprocessing import OrdinalEncoder, StandardScaler  # Prétraitement des données

# Chemins pour les fichiers et la base de données
DB_PATH = '/opt/airflow/data/accidents.db'  # Base de données SQLite
DATA_PATH = '/opt/airflow/data'  # Répertoire des fichiers source
OUTPUT_PATH = '/opt/airflow/output'  # Répertoire pour les fichiers générés

def save_csv(df, filename):
    """Enregistre un DataFrame en tant que fichier CSV dans le répertoire de sortie."""
    os.makedirs(OUTPUT_PATH, exist_ok=True)  # Crée le répertoire s'il n'existe pas
    file_path = os.path.join(OUTPUT_PATH, filename)
    df.to_csv(file_path, index=False)  # Enregistre sans les index
    print(f"Data saved to {file_path}")

def extract_integrate(filepath):
    """Extraction et intégration des fichiers CSV."""
    # Recherche récursive des fichiers CSV
    all_files = glob.glob(os.path.join(filepath, "**/*.csv"), recursive=True)
    if not all_files:
        raise FileNotFoundError(f"No CSV files found in {filepath}.")

    # Chargement de chaque fichier dans un DataFrame
    dfs = [pd.read_csv(file) for file in all_files]
    df_integrated = dfs[0]
    for df in dfs[1:]:
        # Fusion des fichiers sur la colonne "accident_reference"
        df_integrated = df_integrated.merge(
            df,
            on="accident_reference",
            how="inner",
            suffixes=(None, "_DROP")
        ).filter(regex="^(?!.*_DROP)")  # Supprime les colonnes en doublon

    save_csv(df_integrated, "integrated_data.csv")  # Enregistrement du fichier intégré
    print("Integrated data saved to CSV.")

def data_clean():
    """Nettoyage des données intégrées."""
    input_path = os.path.join(OUTPUT_PATH, "integrated_data.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Integrated data file not found at {input_path}.")

    df = pd.read_csv(input_path)

    # Suppression des colonnes inutiles
    mask_drop1 = df.select_dtypes(object).nunique() > 2000  # Colonnes avec trop de valeurs uniques
    mask_drop2 = df.select_dtypes(object).nunique() == 1  # Colonnes avec une seule valeur
    drop1 = df.select_dtypes(object).loc[:, mask_drop1].columns
    drop2 = df.select_dtypes(object).loc[:, mask_drop2].columns
    df.drop(columns=drop1.union(drop2), axis=1, inplace=True)

    # Remplissage des valeurs manquantes par le mode
    for col in df.loc[:, df.isnull().sum() > 0].columns:
        df[col] = df[col].fillna(df[col].mode()[0])

    # Remplacement des valeurs spécifiques
    df.replace({"-1": "Unclassified", "A(M)": "Motorway"}, inplace=True)

    # Suppression des autres valeurs manquantes
    other_null_values = ["Data missing or out of range"]
    df.replace(other_null_values, np.nan, inplace=True)
    df.dropna(inplace=True)

    # Gestion des valeurs aberrantes dans la colonne "age_of_driver"
    Q1 = df['age_of_driver'].quantile(0.25)
    Q3 = df['age_of_driver'].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    df['age_of_driver'] = df['age_of_driver'].apply(lambda x: min(max(x, lower), upper))

    save_csv(df, "cleaned_data.csv")  # Enregistrement des données nettoyées
    print("Cleaned data saved to CSV.")

def transform_encode_load():
    """Transformation, encodage et prétraitement des données."""
    input_path = os.path.join(OUTPUT_PATH, "cleaned_data.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Cleaned data file not found at {input_path}.")

    df = pd.read_csv(input_path)

    # Transformation des données temporelles
    df['Week_Number'] = pd.to_datetime(df['date']).dt.isocalendar().week
    df['Week_end'] = df['day_of_week'].apply(lambda x: 1 if x in ["Saturday", "Sunday"] else 0)
    df['winter_condition'] = df['road_surface_conditions'].apply(lambda x: 1 if x in ['Frost or ice', 'Snow'] else 0)
    df['month'] = pd.to_datetime(df['date']).dt.month
    df['day'] = pd.to_datetime(df['date']).dt.day
    df.drop(columns=['date'], inplace=True)

    # Encodage des données catégorielles
    labels = ['00:00-05:59', '06:00-11:59', '12:00-17:59', '18:00-23:59']
    bins = [0, 6, 12, 18, 24]
    df['Time Bin'] = pd.cut(pd.to_datetime(df['time'], format="%H:%M").dt.hour, bins=bins, labels=labels, right=False)
    df.drop('time', axis=1, inplace=True)

    encoder = OrdinalEncoder()
    df[['first_road_class', 'second_road_class']] = encoder.fit_transform(df[['first_road_class', 'second_road_class']])
    df_encoded = pd.get_dummies(df)  # Conversion des variables catégorielles en indicateurs

    # Standardisation des colonnes numériques
    sc = StandardScaler()
    cols_to_scale = ['number_of_vehicles', 'number_of_casualties', 'speed_limit', 'age_of_driver']
    df_encoded[cols_to_scale] = sc.fit_transform(df_encoded[cols_to_scale])

    save_csv(df_encoded, "preprocessed_data.csv")  # Enregistrement des données prétraitées
    print("Preprocessed data saved to CSV.")

def load_to_final_table():
    """Chargement du CSV prétraité dans la base de données."""
    input_path = os.path.join(OUTPUT_PATH, "preprocessed_data.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Preprocessed data file not found at {input_path}.")

    df = pd.read_csv(input_path)
    required_columns = ['number_of_vehicles', 'number_of_casualties', 'speed_limit']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # Filtrage des lignes avec au moins une victime
    df = df[df['number_of_casualties'] > 0]

    # Enregistrement dans la base de données SQLite
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        df.to_sql("final_data", conn, if_exists="replace", index=False)
    print("Final data saved to the SQLite database.")

# Définition du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(0),
    "retries": 1,
}

dag = DAG(
    'accidents_data_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for accident data',
)

with dag:
    extract_integrate_task = PythonOperator(
        task_id='extract_integrate_task',
        python_callable=extract_integrate,
        op_kwargs={"filepath": DATA_PATH},
    )
    cleanning_task = PythonOperator(
        task_id='cleanning_task',
        python_callable=data_clean,
    )
    transform_encoding_load_task = PythonOperator(
        task_id='transform_encoding_load_task',
        python_callable=transform_encode_load,
    )
    load_to_final_task = PythonOperator(
        task_id='load_to_final_task',
        python_callable=load_to_final_table,
        execution_timeout=timedelta(minutes=10),
    )

# Ordre des tâches dans le DAG
extract_integrate_task >> cleanning_task >> transform_encoding_load_task >> load_to_final_task
