package com.example.mnaganu.dcbc.domain.chunks;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import com.example.mnaganu.dcbc.domain.model.SelectModel;
import com.example.mnaganu.dcbc.domain.repository.CopySourceSampleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class CopySourceReader implements ItemReader<SampleModel>, StepExecutionListener, ChunkListener {
    private final Logger logger = LoggerFactory.getLogger(CopySourceReader.class);

    private final CopySourceSampleRepository copySourceSampleRepository;

    private int offset;
    private int offsetId;

    @Autowired
    public CopySourceReader(CopySourceSampleRepository copySourceSampleRepository) {
        this.copySourceSampleRepository = copySourceSampleRepository;
    }

    /*
      ItemReader<SampleModel>
    */
    @Override
    public SampleModel read() throws Exception {
        logger.debug("CopySourceReader read　offsetId:{} offset:{}", offsetId, offset);
        SelectModel<SampleModel> selectModel = copySourceSampleRepository.selectByOffsetId(offsetId, offset, 1);
        if (!selectModel.getList().isEmpty()) {
            offset++;
            return selectModel.getList().get(0);
        }
        return null;
    }

    /*
      StepExecutionListener
    */
    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.debug("CopySourceReader initialized.");
        offset = 0;
        offsetId = -1;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.debug("CopySourceReader ended.");
        return ExitStatus.COMPLETED;
    }

    /*
      ChunkListener
    */
    @Override
    public void beforeChunk(ChunkContext context) {
        //Chunk 開始前
        offset = 0;
        Object objOffsetId = context.getStepContext().getJobExecutionContext().get("offsetId");
        if (objOffsetId != null && objOffsetId instanceof Integer) {
            offsetId = ((Integer) objOffsetId).intValue();
        }
        logger.debug("CopySourceReader Before chunk offsetId:" + offsetId);
    }

    @Override
    public void afterChunk(ChunkContext context) {
        //Chunk 完了後
        logger.debug("CopySourceReader After chunk offsetId:" + offsetId);
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        //Chunkの途中でエラーが発生した場合
        logger.debug("CopySourceReader Error during chunk processing...");
        String[] names = context.attributeNames();
        if (names != null) {
            Arrays.stream(names)
                    .forEach(
                            name -> logger.error("name:{} attribute:{}", name, context.getAttribute(name).toString())
                    );
        }
    }

}
