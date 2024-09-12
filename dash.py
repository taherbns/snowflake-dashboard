import streamlit as st
import snowflake.connector as sf
import pandas as pd

# Connexion à Snowflake
def connecter_snowflake(user, password):
    try:
        con = sf.connect(
            user=user,
            password=password,
            account='',
        )
        return con
    except Exception as e:
        st.error(f"Erreur de connexion : {e}")
        return None

# Fonction pour récupérer les bases de données
def get_warhouses (con):
    cur = con.cursor()
    cur.execute("SHOW WAREHOUSES")
    warehouses = [row[0] for row in cur.fetchall()] 
    cur.close()
    return warehouses

def get_databases(con , warehouse):
    cur = con.cursor()
    cur.execute(f"USE WAREHOUSE {warehouse}")
    cur.execute("SHOW DATABASES")
    databases = [row[1] for row in cur.fetchall()]  # Utilisation de l'index 1 pour obtenir le nom
    cur.close()
    return databases

# Fonction pour récupérer les schémas d'une base de données
def get_schemas(con,warehouse,database):
    cur = con.cursor()
    cur.execute(f"USE warehouse {warehouse}")
    cur.execute(f"USE DATABASE {database}")
    cur.execute("SHOW SCHEMAS")
    schemas = [row[1] for row in cur.fetchall()]  # Utilisation de l'index 1 pour obtenir le nom
    cur.close()
    return schemas

# Fonction pour récupérer les tables d'un schéma
def get_tables(con,warehouse , database, schema):
    cur = con.cursor()
    cur.execute(f"USE warehouse {warehouse}")
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"USE SCHEMA {schema}")
    cur.execute("SHOW TABLES")
    tables = [row[1] for row in cur.fetchall()]  # Utilisation de l'index 1 pour obtenir le nom
    cur.close()
    return tables

# Fonction pour récupérer les données d'une table
def get_table_data(con,warehouse , database , schema, table):
    cur = con.cursor()
    cur = con.cursor()
    cur.execute(f"USE warehouse {warehouse}")
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"USE SCHEMA {schema}")
    cur.execute(f"SELECT * FROM {table} LIMIT 100")  # Limitation des données pour l'affichage
    data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    return columns, data

# Fonction pour créer un entrepôt

def create_wh(con, db_wh):
    cur = con.cursor()
    cur.execute(f"CREATE WAREHOUSE {db_wh}")
    cur.close()



def create_db(con,db_name, db_wh):
    cur = con.cursor()
    cur.execute(f"USE WAREHOUSE {db_wh}")
    cur.execute(f"CREATE DATABASE {db_name}")
    cur.close()

# Fonction pour créer un schéma dans une base de données
def create_schema(con, schema_name, db_name):
    cur = con.cursor()
    cur.execute(f"USE DATABASE {db_name}")
    cur.execute(f"CREATE SCHEMA {schema_name}")
    cur.close()

# Fonction pour créer une table dans un schéma
def create_table(con, schema_name, table_name, columns_def):
    cur = con.cursor()
    cur.execute(f"USE SCHEMA {schema_name}")
    cur.execute(f"CREATE TABLE {table_name} ({columns_def})")
    cur.close()

# Fonction principale pour orchestrer le processus
def main():
    st.title("Connexion à Snowflake")

    # Connexion et stockage des données dans session_state
    if 'connection' not in st.session_state:
        st.session_state.connection = None
        st.session_state.databases = []
        st.session_state.schemas = []
        st.session_state.tables = []
        st.session_state.data = ([], [])

    # Entrée utilisateur
    username = st.text_input("Nom d'utilisateur", key='username')
    password = st.text_input("Mot de passe", type="password", key='password')
    connect_button = st.button("Se connecter")

    if connect_button:
        st.session_state.connection = connecter_snowflake(username, password)
        
        if st.session_state.connection:
            st.success("Connecté avec succès à Snowflake !")
            st.session_state.warehouse = get_warhouses(st.session_state.connection)
        else:
            st.error("Erreur de connexion. Veuillez vérifier vos identifiants.")

    if st.session_state.connection:
        # Afficher la liste des bases de données
        selected_warehouse = st.selectbox("Choisir une warehouse", st.session_state.warehouse)
        st.session_state.database = get_databases(st.session_state.connection, selected_warehouse)
        selected_database = st.selectbox("Choisir une database", st.session_state.database)
            
        if selected_database:
                st.session_state.schemas = get_schemas(st.session_state.connection, selected_database)
                selected_schema = st.selectbox("Choisir un schéma", st.session_state.database)
            
        if selected_schema:
                st.session_state.tables = get_tables(st.session_state.connection, selected_schema)
                selected_table = st.selectbox("Choisir une table", st.session_state.shemas)
                
        if selected_table:
                    columns, data = get_table_data(st.session_state.connection, selected_schema, selected_table)
                    st.write(f"Table : {selected_table}")
                    st.write(f"Schéma : {selected_schema}")
                    st.write(f"Base de données : {selected_database}")
                    st.write("Données de la table :")
                    st.dataframe(pd.DataFrame(data, columns=columns))

        # Création d'entrepôt
        st.header("Créer une base de donnée")
        new_bdname = st.text_input("Nom de la base de donnée à créer")
        if st.button("Créer la base de donnée"):
            if new_bdname:
                create_db(st.session_state.connection, new_bdname)
                st.success(f"Entrepôt {new_bdname} créé avec succès !")
            else:
                st.error("Veuillez entrer un nom pour l'entrepôt.")

        # Création de schéma
        st.header("Créer un schéma")
        new_schema_name = st.text_input("Nom du schéma à créer")
        if st.button("Créer le schéma"):
            if new_schema_name and selected_database:
                create_schema(st.session_state.connection, new_schema_name, selected_database)
                st.success(f"Schéma {new_schema_name} créé avec succès dans la base de données {selected_database} !")
            else:
                st.error("Veuillez entrer un nom pour le schéma et sélectionner une base de données.")

        # Création de table
        st.header("Créer une table")
        new_table_name = st.text_input("Nom de la table à créer")
        columns_def = st.text_area("Définition des colonnes (ex. id INT, nom VARCHAR(100))")
        if st.button("Créer la table"):
            if new_table_name and columns_def and selected_schema:
                create_table(st.session_state.connection, selected_schema, new_table_name, columns_def)
                st.success(f"Table {new_table_name} créée avec succès dans le schéma {selected_schema} !")
            else:
                st.error("Veuillez entrer un nom pour la table, définir les colonnes, et sélectionner un schéma.")

if __name__ == "__main__":
    main()
