package com.example.mnaganu.dcbc.domain.chunks;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class CopyProcessor implements ItemProcessor<SampleModel, SampleModel>, StepExecutionListener, ChunkListener {

    private final Logger logger = LoggerFactory.getLogger(CopyProcessor.class);

    /*
      ItemProcessor<SampleModel, SampleModel>
    */
    @Override
    public SampleModel process(SampleModel item) throws Exception {
        logger.debug("CopyProcessor process");
        //今回は加工せずにそのまま登録する。
        return item;
    }

    /*
      StepExecutionListener
    */
    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.debug("CopyProcessor initialized.");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.debug("CopyProcessor ended.");
        return ExitStatus.COMPLETED;
    }

    /*
      ChunkListener
    */
    @Override
    public void beforeChunk(ChunkContext context) {
        logger.debug("CopyProcessor Before chunk");
    }

    @Override
    public void afterChunk(ChunkContext context) {
        logger.debug("CopyProcessor After chunk");
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        logger.debug("CopyProcessor Error during chunk");
    }
}
