CREATE TABLE USERS
(
    uid       VARCHAR(64) NOT NULL, --devrait etre primary key mais comme on insert en continu les meme fichiers, je laisse comme ca pour l'instant
    nom       VARCHAR(64) NOT NULL,
    prenom    VARCHAR(64) NOT NULL
);