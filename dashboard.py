import dash
from dash import dcc, html
import pandas as pd
import sqlite3

# Charger les données depuis SQLite
def load_data():
    conn = sqlite3.connect('/opt/airflow/data/accidents.db')
    query = "SELECT * FROM final_data"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Charger les données
data = load_data()

# Créer l'application Dash
app = dash.Dash(__name__)

# Mise en page du tableau de bord
app.layout = html.Div([
    html.H1("Tableau de Bord des Données d'Accidents"),
    dcc.Graph(
        id='graph-example',
        figure={
            'data': [
                {
                    'x': data['age_of_driver'],
                    'y': data['number_of_casualties'],
                    'type': 'scatter',
                    'mode': 'markers',
                    'name': 'Accidents'
                }
            ],
            'layout': {
                'title': 'Relation entre Âge du Conducteur et Victimes'
            }
        }
    )
])

# Lancer l'application
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
