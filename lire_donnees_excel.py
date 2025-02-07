# -*- coding: utf-8 -*----------------------------------------------------------
# Name:        lire_donnees_excel
# Purpose:     script de lecture de données depuis des fichiers excel pour en extraire des séries temporelles et les rabouter
#
# Author:      Alain Gauthier
#
# Created:     05/02/2025
# Licence:     GPL V3
#-------------------------------------------------------------------------------

import pandas as pd
import glob
import os
import sys

# config .ini
import configparser # Permet de parser le fichier de paramètres

#-------------------------------------------------------------------------------

def get_params(input_file):
    """Renvoie un dict contenant les paramètres lus dans input_file

    Args:
        input_file (str): chemin vers le fichier de paramètres .ini

    Returns:
        dict: paramètres par clé:valeur lus
    """
    config = configparser.RawConfigParser()
    config.read(input_file, encoding='utf-8')

    params = dict()

    # FORMAT_DONNEES : format type des données.
    # Deux cas possibles de fichiers excel sont lus :
    # - 'SALLELES' dont chaque onglet comporte des données d'une station
    # - 'POSTE_CENTRAL' dont chaque fichier correspond à une station et les données sont lues dans l'onglet 'DATA'
    params["FORMAT_DONNEES"]=config.get('params','FORMAT_DONNEES').strip()
    # motif des données à lire
    params["FICHIERS_INPUT"]=config.get('params','FICHIERS_INPUT')
    # dossier de sortie des données lues
    params["RESULTATS"]=config.get('params','RESULTATS')

    # paramètres spéfifiques au format des données des fichiers excel à lire
    params[params['FORMAT_DONNEES']] = lire_param_format(config, params['FORMAT_DONNEES'])
    return params

#-------------------------------------------------------------------------------

def _extraire_col_params(str_colonnes):
    """Fonction interne permettant l'analyse de la chaîne de paramètres du type
    Cesse : CESSE.COMPTEUR.DEBIT.Courant_100
    Cesse : CESSE.COMPTEUR.NIVEAU.Cote
    Cesse : CESSE.COMPTEUR.NIVEAU.Plan1
    Moussoulens : MOUSSOULENS.COMPTEUR.DEBIT.courant
    Moussoulens : MOUSSOULENS.COMPTEUR.NIVEAU.Cote
    Moussoulens : MOUSSOULENS.COMPTEUR.NIVEAU.Plan1

    Args:
        str_colonnes (str): chaîne de paramètres à interpréter

    Returns:
        dict: valeurs analysées rangées sous la forme 'Cesse':[val1, val2...], 'Moussoulens':[val1, val2...]
    """
    resultat = dict()
    # extraction des infos ligne par ligne et selon les clefs incluses
    for ligne in str_colonnes.split('\n'):
        k,v = ligne.split(':')
        # suppression des espaces en trop
        k = k.strip()
        v = v.strip()
        if k not in resultat:
            resultat[k] = list()
        resultat[k].append(v)
    # fin
    return resultat

def lire_param_format(config, nom_section):
    """lecture des paramètres du format excel 'SALLELES' contenant
    'col_params' :
        'nom_onglet 1': nom_colonne_dans_fichier_excel 1
        'nom_onglet'1: nom_colonne_dans_fichier_excel 2
        'nom_onglet'2: nom_colonne_dans_fichier_excel 1
        ...

    Args:
        config (configparser): instance contenant les informations du fichier ini
        nom_section (str): nom de la section à lire

    Returns:
        dict: dictionnaire des données lues
    """
    params = dict()
    str_colonnes = config.get(nom_section, 'col_params').strip()

    params = _extraire_col_params(str_colonnes)

    return params

#-------------------------------------------------------------------------------

def lire_fic_salleles(fic, dico_param):
    """lecture du fichier au format 'SALLELES' passé en paramètre avec les instructions
    de colonne à garder par onglet passées en paramètre. Renvoie les données lues dans un dict par onglet et dernière date lue

    Args:
        fic (str): nom du fichier excel à lire
        dico_param (dict): informations sur les colonnes à extraire par onglet

    Returns:
        dict: données lues sous la forme de dict[onglet][derniere_date] = dataframe_donnees_lues
    """
    # lecture fichier excel selon le format indiqué dans les onglets spécifiés dans le dico param
    resultat = dict()
    for onglet in dico_param:
        data_lue = pd.read_excel(fic,
                                 sheet_name=onglet,
                                 parse_dates=True,
                                 date_format='%d/%m/%Y %H:%M:%S',
                                 index_col=0,
                                 skiprows=[1])
        # on conserve uniquement les colonnes spécifiées en paramètre
        data_lue = data_lue[dico_param[onglet]]
        # tri sur l'onglet
        data_lue = data_lue.sort_index()
        # dernière date
        last_date = data_lue.index[-1]
        resultat[onglet] = dict()
        resultat[onglet][last_date] = data_lue
    # fin
    return resultat

#-------------------------------------------------------------------------------

def lire_fic_poste_central(fic, dico_param):
    """lecture du fichier au format 'POSTE_CENTRAL' passé en paramètre avec les instructions
    de colonne à garder par onglet passées en paramètre. Renvoie les données lues dans un dict par onglet et dernière date lue

    Args:
        fic (str): nom du fichier excel à lire
        dico_param (dict): informations sur les colonnes à extraire par onglet

    Returns:
        dict: données lues sous la forme de dict[onglet][derniere_date] = dataframe_donnees_lues
    """
    # lecture fichier excel selon le format indiqué dans le dico param
    resultat = dict()
    # un seul onglet DATA à lire
    data_lue = pd.read_excel(fic,
                             sheet_name='DATA',
                             parse_dates=True,
                             date_format='%d/%m/%Y %H:%M:%S')
    # nom des paramètres dans la colonne 'rank :
    # on crée un index en pivotant les valeurs de 'value' selon l'index 'date'
    chronique_lue = data_lue.pivot(index='date', columns='rank', values='value')
    # dernière date
    last_date = chronique_lue.index[-1]

    # filtre les données lues selon le rangement souhaité
    for station in dico_param:
        resultat[station] = dict()
        resultat[station][last_date] = chronique_lue[dico_param[station]]
    # fin
    return resultat

#-------------------------------------------------------------------------------

def lire_fichiers_excel(chemin_input, dico_param,format_donnees):
    """lecture de l'ensemble des fichiers du chemin passé en paramètre,
    en faisant l'hypothèse qu'ils ont tous un format homogène et contiennent les paramètres indiqués sur les extractions à faire

    Args:
        chemin_input (str): chemin vers tous les fichiers à extraire, pouvant contenir des caractères génériques du type '*' ou '?'
        dico_param (dict): description des données à extraire selon le format des fichiers excel concernés
        format_donnees (str): format des données des fichiers à lire, tous sont supposés être au même format

    Returns:
        dict: données lues dict[nom_station][derniere_date] = la série temporelle associée au nom de station et rangée dans un dataframe
    """
    # résultat
    tab_data = dict()
    # liste des fichiers à lire
    liste_fic = glob.glob(chemin_input)
    # boucle sur les fichiers et lecture selon le format de données
    for fic in liste_fic:
        print('Lecture du fichier ',fic)
        if format_donnees == 'SALLELES':
            # renvoie les données par clefs onglet/derniere_date
            dico_data = lire_fic_salleles(fic, dico_param)

        elif format_donnees == 'POSTE_CENTRAL':
            dico_data = lire_fic_poste_central(fic, dico_param)
        # aggrégation des données lues
        for nom_station in dico_data:
            if nom_station not in tab_data:
                tab_data[nom_station] = dict()
            # données d'une station
            data_station = tab_data[nom_station]
            # série temporelle lue traitée une seule fois par dernière date (on ne fait rien si doublon - print avertissement)
            for last_date in dico_data[nom_station]:
                if last_date in data_station:
                    print(f"ATTENTION, fichier {fic} non traité : \n\t Doublon de date de fin, dernière date = {last_date}")
                    continue
                # ajout des données au résultat
                data_station[last_date] = dico_data[nom_station][last_date]
    # fin
    return tab_data

#-------------------------------------------------------------------------------

def aggreger_donnees(tab_data):
    """Aggrégation des données passées en paramètre en respectant l'ordre des dernières dates des
    données lues pour chaque fichier (et série de données).
    Les dates de mise à jour sont supposées être la dernière date de chaque série de données. Les séries sont ordonnées par ordre chronologique
    de dernière date et chaque série écrase les données précédentes

    Args:
        tab_data (dict): données lues et rangées par clef de station et clef de date

    Returns:
        dict: données traitées et rangées dans des dataframe par clef de date : dict[station] = dataframe
    """
    # resultat
    resultat = dict()
    # parcours des noms de station
    for nom_station in tab_data:
        # tri des dates lues
        list_dernieres_dates = sorted(tab_data[nom_station])
        # principe : les dernières dates lues écrasent les précédentes
        for derniere_date in list_dernieres_dates:
            # donnée à ajouter ou superposer
            data_station = tab_data[nom_station][derniere_date]
            # si nécessaire on supperpose
            if nom_station in resultat:
                # nouvel index : union des dates déjà lues et à ajouter
                new_index = resultat[nom_station].index.union(data_station.index)
                resultat[nom_station] = resultat[nom_station].reindex(index=new_index)
                resultat[nom_station].update(data_station)
            else:
                # initialisation de l'ajout de la donnée la première fois
                resultat[nom_station] = data_station
    # fin
    return resultat

#-------------------------------------------------------------------------------

def ecrire_donnees_traitees(tab_data, dossier_out):
    """Ecriture des données dans les fichiers dont les noms sont basés sur les stations indiquées en clé

    Args:
        tab_data (dict): données à écrire au format tab_data[station] = df
        dossier_out (str): emplacement de l'écriture des données, les dossiers sont créés si nécessaire
    """
    # vérification de l'existence du chemin
    if not os.path.exists(dossier_out):
        print("crétion des dossiers manquant :", dossier_out)
        os.makedirs(dossier_out, exist_ok=True)
    # parcours des données à écrire et export dans des fichiers par station (clé de tab_data)
    for station in tab_data:
        # nom du fichier d'export
        nom_fic = os.path.join(dossier_out, f"export_{station}_.csv")
        df = tab_data[station]
        df.index.name = 'date'
        print("enregistrement de : ", nom_fic)
        df.to_csv(nom_fic, sep=';')

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

def main():
    # fichier .ini obligatoire
    if len(sys.argv) != 2:
        raise IOError("il manque le fichier de paramètres")
    else:
        inputfile = sys.argv[1]

    print('input file : {}'.format(inputfile))

    # lecture des paramètres
    params = get_params(inputfile)

    # lecture des fichiers selon le format de données
    format_donnees = params['FORMAT_DONNEES']
    tab_data = lire_fichiers_excel(params['FICHIERS_INPUT'], params[format_donnees],format_donnees)

    # aggrégation des fichiers multiples par station
    tab_data = aggreger_donnees(tab_data)

    # écriture des résulats
    ecrire_donnees_traitees(tab_data, params['RESULTATS'])

if __name__ == '__main__':
    main()
