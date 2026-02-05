# **Rotterdb**

Bienvenu sur votre Template SQL database fait en language de programmation python! 

## Démarrage
- Afin de lancer la base de donnée, mettez vous dans le répertoire racine du programme et lancez la commande suivante :

```bash
python3 client_local.py
```

## Utilisation

Voici une liste de commande pour vous entrainez aux commandes SQL.
- Pour créer un table :
```bash
CREATE TABLE users (name TEXT, age INT)
```
- Pour insérer des données dans une table : 
```bash
INSERT INTO users VALUES ('Rotter', 32)
```
- Pour selectionner des données depuis une table :
```bash
SELECT * FROM users
```
- Pour afficher la structure de la table :
```bash
DESCRIBE users
```
- Pour supprimer la table : 
```bash
DROP TABLE users
```
- Et enfin, pour quitter la BDD :
```bash
quit
```

