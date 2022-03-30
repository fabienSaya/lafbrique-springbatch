package com.bnp.lafabrique.springbatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class BatchJobLauncher {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Scheduled(fixedDelay = 8000)
    public void perform() throws Exception {
        JobParameters jobParameters= new JobParametersBuilder()
                .addString("jobID", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();

        jobLauncher.run(job, jobParameters);
    }

}
