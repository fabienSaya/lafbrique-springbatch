package com.bnp.lafabrique.springbatch;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.io.IOException;


/**
 * note: il me fait une exception au démarrage, mais à la 2eme occurence, ca marche. suffit d'attendre 8s pour que le scheduling lance une 2eme fois le job
 */
@Configuration
@EnableBatchProcessing
@EnableScheduling
public class BatchConfiguration {


    @Autowired
    private JobBuilderFactory jobBuilderFactory; //va permettre de créer les jobs

    @Autowired
    private StepBuilderFactory stepBuilderFactory; //va permettre de créer les steps

    //version si je met les fichier dans /resources/input
    //@Value("classpath:input/users_input_*.csv")
    //private Resource[] inputResources;

    //version si on met les fichiers ailleurs que dans le répertoire resource
    @Value("file:c:/temp/input_for_spring_batch_sample/users_input_*.csv")
    private Resource[] inputResources;


    /**
     * notre job a un seul step
     * @return
     */
    @Bean
    public Job sampleJob() {
        return jobBuilderFactory
                .get("sampleJob")
                .incrementer(new RunIdIncrementer())
                .start(sampleStep())
                //.start(moveFilesStep())
                .build();
    }

    @Bean
    public Step sampleStep() {
        return stepBuilderFactory
                .get("sampleStep")
                .<UserCSV,UserOut>chunk(2)//lire les lignes 2 par 2 (userCSV)
                .reader(multiResourceItemReader())
                .writer(writerJDBC())
                .processor(processor())
                .listener(new MyItemReaderListener())
                //.taskExecutor(taskExecutor())
                .build();
    }
/*
    @Bean
    public Step moveFilesStep(){
        return stepBuilderFactory
                .get("moveFilesStep");

    }*/


    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(50);
        return executor;
    }


    /**
     * comme on gère le chargement de plusieurs fichiers on utilise un MultiResourceItemReader.
     * Sinon, un flatFileItemReader aurait été suffisant
     * @return
     */
    @Bean
    public MultiResourceItemReader<UserCSV> multiResourceItemReader() {

        MultiResourceItemReader<UserCSV> reader = new MultiResourceItemReader<>();
        reader.setResources(inputResources);
        reader.setStrict(true);//lancera une exception si pas de resource dans input
        reader.setDelegate(readerFlatFile());

        return reader;
    }

    @Bean
    public FlatFileItemReader<UserCSV> readerFlatFile() {
        FlatFileItemReader<UserCSV> reader = new FlatFileItemReader<>();
        //reader.setStrict(true);
        reader.setLineMapper(makeLineMapper());
        reader.setLinesToSkip(1);//on ignore la première ligne qui contient le header
        reader.setStrict(true);
        return reader;
    }

    @Bean
    public ItemProcessor<UserCSV,UserOut> processor() {
        return new ToUpperCaseProcessor();
    }

    @Bean
    public ItemWriter<UserOut> writerJDBC() {
       return new JdbcBatchItemWriterBuilder<UserOut>()
               .dataSource(dataSource())
               .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
               .sql("INSERT INTO USERS (UID,NOM,PRENOM) VALUES(:uid, :nom, :prenom)")
               .build();
    }

    /**
     * on construit un data souce h2
     * @return
     */
    @Bean
    public DataSource dataSource() {
        //hikari=framework qui implemente un pool de connexion. Il va avec spring batch
        HikariDataSource dataSource=new HikariDataSource();
        //dataSource.setDriverClassName("org.h2.Driver");//nom du driver h2
        dataSource.setDriverClassName("org.postgresql.Driver");//nom du driver h2
        //dataSource.setJdbcUrl("jdbc:h2:tcp://localhost/~/userdb"); //url possible : http://www.h2database.com/html/features.html pour mode server
        //dataSource.setJdbcUrl("jdbc:h2:mem:userdb"); //url possible : http://www.h2database.com/html/features.html pour mode in memory
        dataSource.setJdbcUrl("jdbc:postgresql://localhost:5432/userdb"); //base postgre
        dataSource.setUsername("userjava");
        dataSource.setPassword("userjava");
        return dataSource;
    }

    /**
     * permet de faire des requetes
     * @return
     */
    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource());
    }


    /**
     * décrit comment on lit une ligne
     * @return
     */
    private DefaultLineMapper makeLineMapper(){
        DefaultLineMapper mapper = new DefaultLineMapper();
        mapper.setLineTokenizer(makeLineTokenizer());//comment on lit les champs
        mapper.setFieldSetMapper(makeBeanWrapperFieldSetMapper());//objet vers lequel on map les champs lus
        return mapper;
    }

    /**
     * définit comment on parse les champs
     * @return
     */
    private LineTokenizer makeLineTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter("|");//delimiteur des champs
        tokenizer.setNames(new String[]{"uid","prenom","nom"});//nom des attribut de l'objet dans l'ordre de lecture
        return tokenizer;
    }

    /**
     * définit vers quel objet on va mapper les champs (ici UserCSV)
     * @return BeanWrapperFieldSetMapper pour UserCSV
     */
    private BeanWrapperFieldSetMapper makeBeanWrapperFieldSetMapper() {
        BeanWrapperFieldSetMapper mapper = new BeanWrapperFieldSetMapper();
        mapper.setTargetType(UserCSV.class);
        return mapper;
    }





}
