"""mini client consol pour utilise Rotterdb en local"""

import sys
import os

#On ajoute le dossier courant au path Python pour les import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from serveur.moteur_sql import MoteurSQL

def afficher_resultat(resultat): #méthode pour afficher les logs de requête
    """affiche le resultat d'une requete""" 
    print() 
    print("=" * 70)

    if resultat['status'] == 'success': #si requete réussie
        print(f"SUCCESS: {resultat['message']}") #on affiche le msg "success"
    else: 
        print(f"ERROR: {resultat['message']}") #sinon on affiche le msg d'err

    if resultat['data']: #si des données sont présentes
        print()
        print("Données:") 
        print("-" * 70)

        data = resultat['data'] #on les récupère et affiche

        if isinstance(data, list) and len(data) > 0: #si c'est une liste vide
            if isinstance(data[0], dict): #et si le 1er element est un dictionnaire
                afficher_tableau(data) #on 'laffiche sous forme de tableau
            else: #sinon
                for item in data: #pour chaque item
                    print(item) #on l'imprime

        elif isinstance(data, dict): #et si c'est un dcitionnaire
            for cle, valeur in data.items(): #pour chaque paire
                print(f"{cle}: {valeur}") #on les affiche

    print("-" * 70)
    print()

def afficher_tableau(lignes):
    """affcihe une liste de dictionnaires """
    if not lignes:
        print("(aucune donnée)")
        return 
    
    colonnes = list(lignes[0].keys()) #on recupère le nom des colonnes

    entete = " | ".join(colonnes) #on crée l'entete du tableau
    print(entete) #on imprime
    print("-" * len(entete)) #avec une ligne de seperation

    for ligne in lignes: #pour chaque ligne
        valeurs = [] #on crée une liste vide pour les valeurs
        for col in colonnes: #et pour chaque colonne
            valeur = str(ligne.get(col, '')) #on renvoie la valeur convertie en string
            if len(valeur) > 30: #mais si la valeur est trop longue
                valeur = valeur[:27] + '...' #on la tronc
            valeurs.append(valeur) #et on ajoute la valeur a la liste
        print(" | ".join(valeurs)) #puis on l'affiche 

def main():
    """ fontion principale client"""
    print("=" * 70)
    print("ROTTERDB - CLIENT CONSOLE LOCAL")
    print("=" * 70)
    print()

    moteur = MoteurSQL('donnees')
    print("Moteur SQL initialisé")
    print()
    print("Exemples:")
    print(" CREATE TABLE user (nom TEXT, age INT)")
    print(" INSERT INTO users VALUES (nom TEXT, age INT)")
    print(" SELECT * FROM users")
    print(" DESCRIBE users")
    print()

    while True: #on créer une boucle infini de saisie users
        try:
            requete = input("SQL> ") #et on demande des requetes "SQL"

            if requete.strip().lower() == 'quit': #si le user quite
                print("Bye") #on affiche le msg d'au revoir
                break #on sort de la boucle et ferme le prog

            if not requete.strip(): # si la requete est vide
                continue #on continue

            resultat = moteur.executer(requete) #si on execute la requete
            afficher_resultat(resultat) #on renvoie le resultat

        except KeyboardInterrupt: #si le user fait CTRL+C pour sortir
            print("\nBug ...") #affiche le msg de sortie
            break #il sort de la boucle et du prog

        except Exception as exception: # sion pour toute autre exception
            print(f"Erreur: {exception}") #on afficher l'err

if __name__ == '__main__':
    main()