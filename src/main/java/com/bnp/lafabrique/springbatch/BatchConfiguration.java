package com.bnp.lafabrique.springbatch;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;


@Configuration
public class BatchConfiguration {

    @Value("classpath:input/users_input_*.csv")
    private Resource[] inputResources;

    @Bean
    public MultiResourceItemReader<UserCSV> multiResourceItemReader() {
        MultiResourceItemReader<UserCSV> reader = new MultiResourceItemReader<>();
        reader.setResources(inputResources);
        reader.setStrict(true);//lancera une exception si pas de resource dans input
        reader.setDelegate(reader());

        return reader;
    }

    @Bean
    public FlatFileItemReader<UserCSV> reader() {
        FlatFileItemReader<UserCSV> reader = new FlatFileItemReader<>();
        reader.setStrict(true);
        reader.setLineMapper(makeLineMapper());
        reader.setLinesToSkip(1);//on ignore la première ligne qui contient le header

        return reader;
    }

    @Bean
    public ItemProcessor<UserCSV,UserOut> processor() {
        return new ToUpperCaseProcessor();
    }

//    @Bean
  //  public ItemWriter<UserOut>

    /**
     * décrit comment on lit une ligne
     * @return
     */
    private LineMapper makeLineMapper(){
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
