package com.example.mnaganu.dcbc.domain.chunks;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import com.example.mnaganu.dcbc.domain.repository.CopyToSampleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class CopyToWriter implements ItemWriter<SampleModel>, StepExecutionListener, ChunkListener {

    private final Logger logger = LoggerFactory.getLogger(CopyToWriter.class);

    private final CopyToSampleRepository copyToSampleRepository;

    private int offsetId;

    public CopyToWriter(CopyToSampleRepository copyToSampleRepository) {
        this.copyToSampleRepository = copyToSampleRepository;
    }

    /*
      ItemWriter<SampleModel>
    */
    @Override
    public void write(Chunk<? extends SampleModel> chunk) throws Exception {
        logger.debug("CopyToWriter write　offsetId:" + offsetId);
        for (SampleModel model: chunk) {
            Optional<SampleModel> oSampleModel = copyToSampleRepository.selectById(model.getId());
            if (oSampleModel.isEmpty()) {
                logger.debug("CopyToWriter write　insert id:" + model.getId());
                copyToSampleRepository.insert(model);
            }
            offsetId = model.getId();
        }
    }

    /*
      StepExecutionListener
    */
    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.debug("CopyToWriter initialized.");
        copyToSampleRepository.truncate();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.debug("CopyToWriter ended.");
        return ExitStatus.COMPLETED;
    }

    /*
      ChunkListener
    */
    @Override
    public void beforeChunk(ChunkContext context) {
        //Chunk 開始前
        logger.debug("CopyToWriter Before chunk processing...");
    }

    @Override
    public void afterChunk(ChunkContext context) {
        //Chunk 完了後
        logger.debug("CopyToWriter After chunk offsetId:" + offsetId);
        context.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .put("offsetId", Integer.valueOf(offsetId));
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        //Chunkの途中でエラーが発生した場合
        logger.debug("CopyToWriter Error during chunk processing...");
    }

}
