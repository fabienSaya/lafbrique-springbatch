package com.bnp.lafabrique.springbatch;

import org.springframework.batch.item.ItemProcessor;

import java.util.Locale;

/**
 * on va transformer (processer) un UserCSV pour mettre en majuscule certains de ses champs (nom, prenom)
 * et on va retourner un UserOut
 */
public class ToUpperCaseProcessor implements ItemProcessor<UserCSV,UserOut> {
    @Override
    public UserOut process(UserCSV userCSV) throws Exception {
        UserOut out = new UserOut();
        out.setUid(userCSV.getUid());
        out.setNom(userCSV.getNom().toUpperCase());
        out.setPrenom(userCSV.getPrenom().toUpperCase());

        return out;
    }
}
