package com.bnp.lafabrique.springbatch;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;


public class MyItemReaderListener implements ItemReadListener<UserCSV> {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public void beforeRead() {
        log.info("START READ");
    }

    @Override
    public void afterRead(UserCSV userCSV) {
        log.info(" READ {}",userCSV);
    }

    @Override
    public void onReadError(Exception e) {
        log.error(e.getMessage());
    }
}
