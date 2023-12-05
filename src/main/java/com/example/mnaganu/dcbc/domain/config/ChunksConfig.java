package com.example.mnaganu.dcbc.domain.config;

import com.example.mnaganu.dcbc.domain.chunks.CopyProcessor;
import com.example.mnaganu.dcbc.domain.chunks.CopySourceReader;
import com.example.mnaganu.dcbc.domain.chunks.CopyToWriter;
import com.example.mnaganu.dcbc.domain.model.SampleModel;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class ChunksConfig {

    private final CopySourceReader copySourceReader;
    private final CopyProcessor copyProcessor;
    private final CopyToWriter copyToWriter;

    @Autowired
    public ChunksConfig(CopySourceReader copySourceReader, CopyProcessor copyProcessor, CopyToWriter copyToWriter) {
        this.copySourceReader = copySourceReader;
        this.copyProcessor = copyProcessor;
        this.copyToWriter = copyToWriter;
    }

    @Bean
    protected Step dataCopyStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        //データをコピーするだけでデータの加工はしないので processor の処理は省略する。
        //3件づつ処理する。
        return new StepBuilder("dataCopyStep", jobRepository)
                .<SampleModel, SampleModel> chunk(3, transactionManager)
                .reader(this.copySourceReader)
                //.processor(this.copyProcessor)
                .writer(this.copyToWriter)
                .build();
    }

    @Bean
    public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        /*
          JobのIDはDB（メタデータ）に登録される。
          JobのIDはプライマリーキーになっているので、重複しないようにインクリメントする必要がある。
         */
        return new JobBuilder("chunksJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(dataCopyStep(jobRepository, transactionManager))
                .build();
    }

}
