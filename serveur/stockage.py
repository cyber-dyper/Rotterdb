"""
Gestion du stockage des tables en fichiers binaires
Chaque table est stockée dans un fichier au format .db
"""

import struct #module pour convertir les données python en binaire, un traducteur
import os #module pour gérer les fichiers et dossiers, s'ils existents et les créer
import random #module pour générer des nombres aleatoires
import string #module pour des constantes de caractères a-z et 0-9 pour les ID types SERIAL demandées

#Fonction pratique 1: pour générer un ID unique de type SERIAL
def generer_id():
    """Pour générer un identifiant unique de 16 caractères""" #doctstring, bonne pratique de description
    caracteres = string.ascii_lowercase + string.digits #concatène 'abcdefghijklmnopqrstuvwxyz' + '0123456789'
    identifiant = ''.join(random.choice(caracteres) for _ in range(16)) #boucle 16x sans utiliser de variable ('_')
    return identifiant #choisi un caractere aleatoirement et les colles ensemble (''.join()) et le retourne


#Fonction pratique 2: convertir un type SQL en code numérique (lorsqu'on va écrire)
def type_vers_code(nom_type): #parmètre d'entrée (nom_type)
    """On converti le nom d'un type SQL en code numérique pour le stockage"""
    types_disponibles = { # {}, pour les dictionnaire python en mode table de correspondance
        'INT': 1, #les netiers - clé : valeur correspondante
        'FLOAT': 2, #les quotients
        'TEXT': 3, #du texte
        'BOOL':4, #les vrais ou faux
        'SERIAL':5 #serie unique d'entiers
    } #et .get : la méthode dictionnaire : .get(clé, valeur_par_defaut)
    return types_disponibles.get(nom_type.upper(), 0) #convertit en MAJ, et vérifie si la clé existe sinon 0

#Fonction pratique 3 : code vers type (quand on lit depuis le fichier binaire)
def code_vers_type(code):
    """On convertit un code numerique en nom de type SQL"""
    codes_vers_types = {
        1: 'INT',
        2: 'FLOAT',
        3: 'TEXT',
        4: 'BOOL',
        5: 'SERIAL'
    } #de nouveau la méthode dictionnaire 
    return codes_vers_types.get(code, 'UNKNOWN') #récupère la clé, ou UNKNOWN si non définie


class GestionnaireDeTable: #classe = plan de construction de la db, style PascalCase
    """La classe pour gérer le stockage et la lecture des tables""" 

    #on défini le constructeur ALWAYS avec __init__ , TOUJOURS appelé à la création 
    def __init__(self, nom_dossier='nom'): #on assigne à nom_dossier, une valeur par défaut
        """Initialise le gestionnaire avec l'attribut dossier"""
        self.dossier = nom_dossier #self ALWAYS le 1er param : self.attribut = param 
        #self est par défaut une "instance de la classe" = le nom d'objet qu'on choisira
        #.dossier est un attribut (une variable) de l'objet self
        #on stock le param nom_dossier dans l'attribut .dossier pour pouvoir l'utiliser dans les autres methodes
        #exemple : gestion1 = GestionnaireDeTable('users') <=> gestion1.dossier = 'users'

        #On va utiliser des os.path() fonctions pour que
        if not os.path.exists(self.dossier):  #si le dossier n'existe pas
            os.makedirs(self.dossier) #on puisse le créer avec la methode os.makedirs()

    def chemin_table(self, nom_table): #nouvelle methode, donc 1er param = self, 2e param, ...
        """Renvoie le chemin du fichier de la table"""
        return os.path.join(self.dossier, f'table_{nom_table}.db') #retourne self.dossier/nom_table.db
        #f-string : f' devant {} active et formate la chaine, mais c'est du python récent
        #ex: f'table_{nom_table}.db, est 'table_users.db'
    
    def table_existe(self, nom_table): 
        """Rerifie si la table existe ou non"""
        chemin = self.chemin_table(nom_table) #appelle la méthode précédente (DRY : don't repeat yourself)
        return os.path.exists(chemin) #retourne le resultat du booleen .exists() : true or false
    

    def creer_table(self, nom_table, colonnes):
        """
        pour créer les nouvelles tbales on va utiliser des tuples, 
        c'est à dire des associations du style ('nom', 'type')
        ou ('id', 'SERIAL'), ou encore ('age', 'INT')
        """
        #il faut d'abord verifier la présence de la table et éviter de créer des doublons (=exceptions), donc
        if self.table_existe(nom_table): #si elle existe
            raise Exception(f"Erreur : la table '{nom_table}' existe déjà") #alors, on renvoie un message d'erreur
        
        #on crée aussi la colonne _id par défaut
        noms_colonnes = [col[0] for col in colonnes] #1ere colonne = indice [0]: [expression for element in lsite]
        if '_id' not in noms_colonnes: #si le noms_colonnes n'est pas _id
            colonnes.insert(0, ('_id', 'SERIAL')) #alors, on l'insert avant la 1ere colonne, en type SERIAL
        
        #il faut aussi 'préparer' l'en-tête pour chaque table, donc on défini d'abord son chemin
        chemin = self.chemin_table(nom_table)

        #with open ouvre la table depuis son (chemin, en mode "write binary") et lui assigne une variable : table
        with open(chemin, 'wb') as table:
            #1. Nbr de colonnes
            nbr_colonnes = len(colonnes) #on défini le nombre d'octets, ici colonnes
            table.write(struct.pack('I', nbr_colonnes)) #on encore et assigne 'I' = 4 octets pour le nbr_colonnes

            #et il faut maintenant encoder chaque colonne en binaires, et donc une boucle for 
            for nom, type in colonnes: # pour chaque pair de  ('nom', 'type')
                #2. Pour chaque colonne
                nom_binaire = nom.encode('utf-8') #on convertie le nom en binaire utf-8
                nom_len = len(nom_binaire) #on calcule sa longueur en octet, pour la lecture après
                code_type = type_vers_code(type) #et convertie le type SQL en code avec notre fonction

                #et ensuite, il faut l'écrire avec la méthode write(), module struct et sa fonction .pack()
                #qui converti python au format binaire et dans notre cas necessaire pour stocker la table 
                table.write(struct.pack('I', nom_len)) #de taille 'I' = unsigned INT = 4 octets pour nom_len
                table.write(nom_binaire) #nom_binaire est déjà convertie 
                table.write(struct.pack('B', code_type)) #on écrit sur 1 octet 'B' = unsigned BYTE/char le codetype
            
            #3. Nbr de ligne
            table.write(struct.pack('I', 0)) #et alloue 4 octets pour le nbr de lignes
        
        return True #et on renvoi True si tout s'est bien passé
    
    #Recap : nbr_colonnes = 4o,
    #Pour chaque col : nom_len = 4o,  nom_binaire = variable, code_type = 1o, et nbr_lignes = 4o
    
    def lire_struct(self, nom_table):
        """ça renvoie la structure de la table (nom, type)"""
        if not self.table_existe(nom_table): #si la table n'existe pas 
            raise Exception(f"Il n'y a pas de table '{nom_table}'") #alors, exception et msg d'erreur
        
        chemin = self.chemin_table(nom_table) #on assigne une variable au chemin de la table
        colonnes = [] #on défini une liste vide pour les colonnes

        with open(chemin, "rb") as table: #on ouvre la table en "read bytes" en buffer avec var : table
            #1 Nbr de colonnes (lire)
            data = table.read(4) #on défini data, la variable qui lit 4 octet car, nbr_colonnes (voir recap)
            nbr_colonnes = struct.unpack('I', data)[0] #unpack décode et renvoie un tuple [x,y]
            #on veut seulement x, donc rajouter l'indice [Ø] pour avoir le nombre de colonne

            for _ in range(nbr_colonnes): #une boucle for avec variable jetable car nbr fini de colonne
                #2. Pour chaque colonne
                data = table.read(4) #on lit 4 octets
                nom_len = struct.unpack('I', data)[0] #et on décode la longueur du nom en binaire

                nom_binaire = table.read(nom_len) #on lit le nbr d'octet de la longueur du nom (variable)
                nom_col = nom_binaire.decode('utf-8') #et on convertit le nom du binaire en texte

                data = table.read(1) #on lit un octet pour le code du type 
                code_type = struct.unpack('B', data)[0] #on décode type unsigned char 'B', retrouve le code du type
                type_col = code_vers_type(code_type) #convertie le code en nom type SQL avec notre fonction

                colonnes.append((nom_col, type_col))

        return colonnes #renvoi la liste de tuples par colonne
    
    def encoder_valeur(self, valeur, type_col):
        """code en binaire chaque valeur en fonction du type"""

        if valeur is None: #si la valeur dans la colonne est null, donc vide
            #struct.pack pour convertir données python en binaire et les stocker dans un fichier
            return struct.pack('B', 0) + struct.pack('I', 0) #1 octet pour valeur 0, 4 octets pour valeur 0
        
        elif type_col == 'INT':
            return struct.pack('B', 1) + struct.pack('i', int(valeur)) #1o pour 1, et 4o pour valeur du int
        
        elif type_col == 'FLOAT':
            return struct.pack('B', 1) + struct.pack('d', float(valeur)) #1o pour 1, et 8c pour valeur du float
        
        elif type_col == 'TEXT' or type_col == 'SERIAL':
            text = str(valeur).encode('utf-8') #convertie en binaire la chaine de caractère texte
            text_len = len(text) #calcule le nbr d'octet 
            return struct.pack('B', 1) + struct.pack('I', text_len) + text #1o pour 1, 4o pour longueur, + texte
        
        elif type_col == 'BOOL':
            bool_val = 1 if valeur else 0
            return struct.pack('B', 1) + struct.pack('B', bool_val) #1o pour 1, et 1 pour valeur bool_val
        
        else : #et sinon, msg d'erreur pour les exception de type non défini
            raise Exception(f"Pas du type INT, FLOAT, TEXT, or BOOL : {type_col}")
        
    
    def decoder_valeur(self, table, type_col):
        """decode valeur binaire de la table selon le type"""

        data = table.read(1) #on défini la longueur 1o pour valeur = null
        if len(data) == 0: #si valeur est null, 'vide' = 0 octet
            return None #renvoi rien
        
        marqueur = struct.unpack('B', data)[0] #si on convertit un octet pour valeur du tuple
        if marqueur == 0: #et le tuple == 0
            table.read(4) #on lit 4 octet, padding pour avancer en lecture autant qu'on a écrit
            return None #renvoie rien
        
        if type_col == 'INT':
            data = table.read(4) #lit 4 octets pour les int
            return struct.unpack('i', data)[0] #convertie en int les 4 octet
        
        elif type_col == 'FLOAT':
            data = table.read(8) #lit 8 octets pour les floats
            return struct.unpack('d', data)[0] #convertit en float les 8 octets
        
        elif type_col == 'TEXT' or type_col == 'SERIAL':
            data = table.read(4) #lit 4 octets
            longueur = struct.unpack('I', data)[0] #convertit en int la longueur du text
            text_binaire = table.read(longueur) #lit le nbr d'octet du text
            return text_binaire.decode('utf-8') #convertit en text les octets du text
        
        elif type_col == 'BOOL':
            data = table.read(1) # lit 1 octet
            val = struct.unpack('B', data)[0] #convertit en 0 ou 1 le booleen
            return val == 1
        
        else:
            raise Exception(f"Pas du type INT, FLOAT, TEXT, or BOOL : {type_col}")
        

    def inserer_ligne(self, nom_table, valeurs):
        """ inserer de la data dans les tables"""

        if not self.table_existe(nom_table): #on vérifie que la table existe
            raise Exception(f"Pas de table '{nom_table}'") #sinon msg d'erreur car exception
        
        table = self.lire_struct(nom_table)#on lit la structure de la table avec notre méthode dans le meme ordre 

        if '_id' not in valeurs: #si l'_id manque
            valeurs['_id'] = generer_id() #on la génère

        for nom_col, type_col in table: #pour chaque colonne du tableau
            if nom_col not in valeurs and type_col != 'SERIAL': #s'il manque une colonne
                valeurs[nom_col] = None  #ajouter la valeur null dans cette colonne
        
        donnees_binaires = b'' #Crée un panier vide pour stocker les bytes
        for nom_col, type_col in table: #pour chaque colonne dans la table
            valeur = valeurs.get(nom_col) #récupérer la valeur de la ligne dans la colonne
            donnees_binaires += self.encoder_valeur(valeur, type_col) #et la convertit en binaire puis l'ajoute au panier

        chemin = self.chemin_table(nom_table) #récupre le chemin de la table

        with open(chemin, 'rb') as table: # ouvre en mode lecture
            contenu = table.read() #charge toute la table en memoire

        #Trouver ou inserer les lignes
        position = 4 #on se positionne apres le nombre de colonne, donc apres 4octets
        nbr_colonnes = struct.unpack('I', contenu[:4])[0] #lit de nbr de colonnes, 4o

        #On saute l'en-tête des colonnes
        for _ in range(nbr_colonnes): #pour chaque colonne
            nom_len = struct.unpack('I', contenu[position:position+4])[0] #lit la longueur du nom
            position += 4 + nom_len + 1 #on se positionne apres le saute en octets : 4 + variable + 1
        
        #lire et incrémenter le compteur
        nbr_lignes = struct.unpack('I', contenu[position:position+4])[0] #un décode le nombre de ligne actuel
        nouveau_nbr = nbr_lignes + 1 #on rajoute la ligne supplémentaire

        #Reconstruire le fichier
        nouveau_contenu = (
            contenu[:position] +  #on garde tout l'en-tete jusqu'au nbr de lignes
            struct.pack('I', nouveau_nbr) +  #on convertit en binarie et ajoute le nouveau nombre de ligne
            contenu[position+4:] + #on garde le contenu de la table chargé en mémoire
            donnees_binaires #on ajoute la nouvelle ligne convertie en binaire après
        )

        with open(chemin, 'wb') as table: #on ouvre la table en write binary
            table.write(nouveau_contenu) #on écrit le nouveau contenu
        
        return valeurs['_id'] #et on renvoie bien l'_id de la ligne inséré
    
    def lire_table(self, nom_table):
        """lit toutes les lignes de la table"""

        if not self.table_existe(nom_table):  #verifie que la table existe
            raise Exception(f"Pas de table {nom_table}") #sinon, msg erreur d'exception
        
        structure = self.lire_struct(nom_table) #on recupere la structure des entetes de la table

        chemin = self.chemin_table(nom_table) #le chemin de la table
        lignes = [] #on prepare un liste vide a remplir lors de la lecture

        with open(chemin, 'rb') as table: #on ouvre en lecture bianire
            nbr_colonnes = struct.unpack('I', table.read(4))[0] #on lit le nbr de colonnes

            for _ in range(nbr_colonnes): #pour chaque colonne
                nom_len = struct.unpack('I', table.read(4))[0] #on lit la longueur du nom
                table.read(nom_len) #on saute en octet sa longueur
                table.read(1) #idem pour le code type

            nbr_lignes = struct.unpack('I', table.read(4))[0] #on lit le nbr de lignes

            for _ in range(nbr_lignes): #pour chaque ligne
                ligne = {} #on assigne un dictionnaire vide

                for nom_col, type_col in structure: #pour chaque colonne
                    valeur = self.decoder_valeur(table, type_col) #on décode la valeur binaire(octet et type)
                    ligne[nom_col] = valeur #on stock le contenu décodé dans le dictionnaire

                lignes.append(ligne) #on ajoute la ligne à la liste vide

        return lignes #on renvoie toutes les lignes
    
    def supprimer_table(self, nom_table):
        """supprimer une table"""

        if not self.table_existe(nom_table): #on vérifie que la table existe
            raise Exception(f"Pas de table {nom_table}") #sinon msg d'err d'exception
        
        chemin = self.chemin_table(nom_table) #on récupère le chemin
        os.remove(chemin) #et on le supprime 

        return True #renvoi que tout est ok

    def lister_tables(self): 
        """on liste toutes les tables créées"""
        tables = [] #on crée une liste vide pour y renseigner les noms

        for table in os.listdir(self.dossier): #pour chaque table existante
            if table.startswith('table_') and table.endswith('.db'): #filtrer sur les extensions .db
                nom = table[6:-3] #couper les 6 premiers et 3 derniers caractères
                tables.append(nom) #ajouter le nom de la table a la liste

        return tables #renvoyer la listes des tables
    