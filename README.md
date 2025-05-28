Monorepo de construction d'une suite logicielle permettant de réaliser des simulations statiques de politiques publiques.
 
# TODO
- écrire une libraire partagée **kalkoto-lib** permettant de construire une politique publique à partir d'un fichier TOML de définition du scénario envisagé et d'un fichier CSV contenant la liste des ménages auxquels appliquer le scénario
- écrire un programme en ligne de commande **kalkoto-cli** permettant de définir les fichiers d'input, de réaliser la simulation et d'exporter les résultats dans des fichiers CSV d'output définis par l'utilisateur 
- écrire une application Web **kalkoto-app** à l'aide du framework *Dioxus* permettant de réaliser toutes les simulations et de visualiser tous les résultats dans un navigateur Web
