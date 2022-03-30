package com.bnp.lafabrique.springbatch;

public class UserCSV {
    private String uid;
    private String nom;
    private String prenom;

    public UserCSV() {
    }

    public UserCSV(String uid, String nom, String prenom) {
        this.uid = uid;
        this.nom = nom;
        this.prenom = prenom;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getPrenom() {
        return prenom;
    }

    public void setPrenom(String prenom) {
        this.prenom = prenom;
    }

    @Override
    public String toString() {
        return "UserCSV{" +
                "uid='" + uid + '\'' +
                ", nom='" + nom + '\'' +
                ", prenom='" + prenom + '\'' +
                '}';
    }
}
