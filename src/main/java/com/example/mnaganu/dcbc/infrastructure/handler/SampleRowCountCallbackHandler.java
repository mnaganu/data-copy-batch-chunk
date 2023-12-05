package com.example.mnaganu.dcbc.infrastructure.handler;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import org.springframework.jdbc.core.RowCountCallbackHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SampleRowCountCallbackHandler extends RowCountCallbackHandler {
    private List<SampleModel> sampleModelList;
    private final int offset;
    private final int limit;

    public SampleRowCountCallbackHandler(int offset, int limit) {
        super();
        sampleModelList = new ArrayList<SampleModel>();
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    protected void processRow(ResultSet rs, int rowNum) throws SQLException {
        if (rowNum >= offset & rowNum < offset + limit) {
            sampleModelList.add(
                    SampleModel.builder()
                            .id((Integer) rs.getObject("id"))
                            .name(rs.getString("name"))
                            .build()
            );
        }
    }

    public List<SampleModel> getList() {
        return sampleModelList;
    }
}
