import re #module pour les expressions regex, qui vont etre utile pour parser le texte
from serveur.stockage import GestionnaireDeTable

class MoteurSQL:
    """le moteur pour executer les requete sql"""

    def __init__(self, nom_dossier='nom'):
        self.gestionnaire = GestionnaireDeTable(nom_dossier) #On crée le gestionnaire de table

    
    def nettoyer_requete(self, requete):
        """reformatte la requete proprement"""
        requete = requete.strip() #on enlève les espace au début et fin
        if requete.endswith(';'): #Si la requete se termine par un ;
            requete = requete[:-1] #on enelve le :dernier caractère
        return requete #et on renvoi la requete nettoyée
    
    def executer(self, requete):
        """on execute la commande sql et try/catch les erreurs"""
        try: #pour capturer les erreurs lorsqu'on execute
            requete = self.nettoyer_requete(requete) #on nettoye

            if not requete: #si la requete est vide
                return { #on renvoie un msg d'err
                    'status' : 'error',
                    'message': 'vide',
                    'data' : None
                }
            
            #Parsons maintenant les requetes
            mots = requete.split() #on découpe la requete en tockens
            type_requete = mots[0].upper() #on formatte le premier arg de la commande en MAJ comme SQL

            if type_requete == 'CREATE': #pour créer une table
                nom_table, colonnes = self.parser_create(requete) #on parse la requete
                self.gestionnaire.creer_table(nom_table, colonnes) #on crée la table
                return { #renvoi logs de creations
                    'status': 'success',
                    'message': f"Table '{nom_table}' créée",
                    'data': None
                }
            
            elif type_requete == 'DROP': #pour supprimer une table
                nom_table = self.parser_drop(requete) #on parse
                self.gestionnaire.supprimer_table(nom_table) #on supprime
                return { #on renvoie les logs de suppressions
                    'status': 'success',
                    'message': f"Table '{nom_table}' supprimée",
                    'data': None
                }
            
            elif type_requete == 'INSERT': #pour insérer des valeurs
                nom_table, valeurs = self.parser_insert(requete) #on parse la requete
                id_insert = self.gestionnaire.inserer_ligne(nom_table, valeurs) #on insère la ligne dans la table
                return { #on renvoie les logs d'insertions
                    'status': 'success',
                    'message': f"Ligne insérée, _id = {id_insert}",
                    'data': {'_id':id_insert}
                }
            
            elif type_requete == 'SELECT': #pour selectionner des valeurs
                colonnes, nom_table = self.parser_select(requete) #on parse la requete
                lignes = self.gestionnaire.lire_table(nom_table) #on selectionne toutes les lignes a lire

                if colonnes != ['*']: #pour select des colonnes si pas * : all
                    lignes_filtrees = [] #on declare une liste vide pour les lignes demandées
                    for ligne in lignes: #pour chaque ligne de la table
                        nouvelle_ligne = {} #on declare un dictionnaire vide
                        for col in colonnes: #pour chaque colonne demandée
                            nouvelle_ligne[col] = ligne.get(col) #on copie la valeur de la ligne dans le dictionnaire
                        lignes_filtrees.append(nouvelle_ligne) #on l'ajoute a la liste 
                    lignes = lignes_filtrees #et on remplace seulement par les lignes demandées

                return {
                    'status': 'success',
                    'message': f"{len(lignes)} ligne(s)",
                    'data': lignes
                }
                    
            
            elif type_requete == 'DESCRIBE': #pour retourner la structure de la table
                nom_table = mots[1] #on prends le deuxieme mot = qui est le nom de la table
                structure = self.gestionnaire.lire_struct(nom_table) #on lit la structure de la table
                colonnes = [{'colonne': nom, 'type': typ} for nom, typ in structure] #on formatte la colonnes
                return { #renvoie confirmation de requete
                    'status': 'success',
                    'message': f"Structure de '{nom_table}'",
                    'data': colonnes
                }
            
            else: #sinon
                return { #on renvoie le msg d'err d'une mauvaise requete
                    'status': 'error',
                    'message': f"Mauvaise requete: {type_requete}",
                    'data': None
                }
        except Exception as exceptions:
            return {
                'status': 'error',
                'message': str(exceptions),
                'data': None
            }
    
    def parser_create(self, requete):
        """pour parser lorsqu'on créer"""
        pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)' #regex DROP TABLE groupe(1) groupe(2)
        match = re.search(pattern, requete, re.IGNORECASE | re.DOTALL) #fonction regex pour match avec requette sans casse

        if not match: #si pas de match a été trouvé
            raise Exception("Mauvaise CREATE TABLE syntaxe") #on imprime le msg d'err exception
        
        nom_table = match.group(1) #premier match trouvé = nom_table
        colonnes_texte = match.group(2)#deuxieme match trouvé = colonnes_texte

        colonnes=[] #on déclare une liste vide pour y stocker les colonnes
        parties = colonnes_texte.split(',') #on séparer en tockens les différents colonnes dans le texte 

        for partie in parties: #pour chaque colonne
            partie = partie.strip() #on enleve les espaces
            elements = partie.split() #on les séparer en tocken
            if len(elements) <2: #si moins de 2 tockens
                raise Exception(f"Colonne syntaxe erreur {partie}") #msg d'err d'exception
            nom_col = elements[0] #sinon 1er tocken et le nom de la colonne
            type_col = elements[1].upper() #2eme tocken est le type
            colonnes.append((nom_col, type_col)) #on ajoute le tuple nom, type a la liste vide

        return nom_table, colonnes #et on renvoie nom et colonnes


    def parser_drop(self, requete): #parser pour supprimer
        """pour parser lorsqu'on supprime"""
        pattern = r'DROP\s+TABLE\s+(\w+)' #formule regex DROP TABLE groupe(1)
        match = re.search(pattern, requete, re.IGNORECASE) #fonction regex match avec la requete sans casse

        if not match: #si aucun match
            raise Exception("Mauvaise DROP TABLE syntaxe") #msg d'err d'exception
        
        return match.group(1) ##sinon renvoi le groupe(1) trouvé
    
    def parser_insert(self, requete): #parser pour inserer des données dans les colonnes
        """pour parser les insertion d'écriture"""
        pattern = r'INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)' #regex INSERT INTO groupe(1) groupe(2)
        match = re.search(pattern, requete, re.IGNORECASE) #fonction regex de match avec requette sans casse

        if not match: #si aucun match
            raise Exception("Mauvase syntaxe INSERT") #renvoie le msg d'err d'exception
        
        nom_table = match.group(1) #1er match groupe(1) est le nom de la table
        valeurs_texte = match.group(2) #2eme match est la valeur en texte a mettre dans les colonnes

        valeurs = self.parser_valeurs(valeurs_texte) #parse les valeur second notre fonction pratique

        structure = self.gestionnaire.lire_struct(nom_table) #lit la structure de la table
        colonnes_sans_id = [col for col in structure if col[0] != '_id'] #on enleve _id du tableau pour flag

        if len(valeurs) != len(colonnes_sans_id): #on vérfie que le nombre de valeur correspond sans _id
            raise Exception(f"Mauvais nombre de valeurs") #sinon on renvoie msg erreur d'excpetion
        
        dict_valeurs = {} #on déclare un dictionnaire vide a remplir
        for i, (nom_col, _) in enumerate(colonnes_sans_id): #pour chaque colonne avec nom, dans le nbr de colonne dans id
            dict_valeurs[nom_col] = valeurs[i] #on associe la valeur du nom de la colonne a indice ou il se trouve da

        return nom_table, dict_valeurs #renvoi nom et dictionnaire
    

    def parser_valeurs(self, texte): #parser pour capturer les valeurs
        """pour parser les valeurs types texte, int, foat, booleen """
        valeurs = [] #on déclare une liste vide pour stockage
        partie_actuelle = "" #un buffer pour construire une valeur
        dans_guillemets = False #flag pou savoir si on est dans une streing ou pas
        guillemet_type = None #type de guillement '' ou ""
        
        for caractere in texte: #pour chaque carctère
            if caractere in ('"', "'") and not dans_guillemets: #début d'un string
                dans_guillemets = True #active le flag
                guillemet_type = caractere #et mémorise le type
            elif caractere == guillemet_type and dans_guillemets: #prchain guillement
                dans_guillemets = False #fin de string
                guillemet_type = None
            elif caractere == ',' and not dans_guillemets: #si virgule hors guillemets,
                valeur = self.convertir_valeur(partie_actuelle.strip()) #on nettoye les avant apres
                valeurs.append(valeur) #on ajoute a la liste
                partie_actuelle = "" #on réinitialise le bugger
            else:
                partie_actuelle += caractere #et on réinitialise le buffer
            
        if partie_actuelle.strip(): #si le dernir buffer non vide 
            valeur = self.convertir_valeur(partie_actuelle.strip())
            valeurs.append(valeur) #on ajoute la valeur a la liste

        return valeurs #et on renvoie la liste de valeurs
    
    def convertir_valeur(self, texte):
        """Convertire dans les guillemets"""
        texte = texte.strip() #on enttoye et enleve les espaces

        if texte.upper() == 'NULL':  #si la valeur est NULL
            return None #on retourne rien
        
        if texte.lower() == 'true': #si true
            return True #renvoie true
        
        if texte.lower() == 'false': #si fausse
            return False #renvoie faux
        
        if (texte.startswith("'") and texte.endswith("'")) or (texte.startswith('"') and texte.endswith('"')):
            return texte[1:-1] #si guillemets, on renvoie le texte sans
        
        try: # on essaie de convertir en nombre
            if '.' not in texte: #si pas de decimal
                return int(texte) #on converti en int
            else: #sinon
                return float(texte) #on convertie en float
        except ValueError: #mais si la conversion échoue
            return texte #on renvoi le text sans modif
        
    def parser_select(self, requete):
        """parser pour selectionner la data"""
        pattern = r'SELECT\s+(.*?)\s+FROM\s+(\w+)' #regex SELECT groupe(1) FROM group(2)
        match = re.search(pattern, requete, re.IGNORECASE) #fonction regex pour ignorer la casse

        if not match:  #mais si aucun match
            raise Exception("Mauvaise SELEXT syntax") #msg d'err
        
        colonnes_texte = match.group(1).strip() #on recupere les colonnes du match du grp 1
        nom_table = match.group(2) #et ceux du grp 2

        if colonnes_texte =='*': #si on veut toutes les colonnes
            colonnes = ['*'] #on renvoie la lsite avec etoile
        else: #sinon
            colonnes = [c.strip() for c in colonnes_texte.split(',')] #on separer les colonnes par une virgule

        return colonnes, nom_table #et on renvoie les colonnes avec le nom de la table
    
        

