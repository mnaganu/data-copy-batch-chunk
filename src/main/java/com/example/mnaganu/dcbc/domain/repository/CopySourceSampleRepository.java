package com.example.mnaganu.dcbc.domain.repository;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import com.example.mnaganu.dcbc.domain.model.SelectModel;

import java.util.Optional;

public interface CopySourceSampleRepository {
    void truncate();
    Optional<SampleModel> selectById(int id);
    SelectModel<SampleModel> selectByOffset(int offset, int limit);
    SelectModel<SampleModel> selectByOffsetId(int offsetId, int offset, int limit);
    int insert(SampleModel model);
}
