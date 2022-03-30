package com.bnp.lafabrique.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;



/**
 * on va transformer (processer) un UserCSV pour mettre en majuscule certains de ses champs (nom, prenom)
 * et on va retourner un UserOut
 */
public class ToUpperCaseProcessor implements ItemProcessor<UserCSV,UserOut> {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public UserOut process(UserCSV userCSV) throws Exception {
        log.info("process: {}",userCSV);

        UserOut out = new UserOut();
        out.setUid(userCSV.getUid());
        out.setNom(userCSV.getNom().toUpperCase());
        out.setPrenom(userCSV.getPrenom().toUpperCase());

        return out;
    }
}
