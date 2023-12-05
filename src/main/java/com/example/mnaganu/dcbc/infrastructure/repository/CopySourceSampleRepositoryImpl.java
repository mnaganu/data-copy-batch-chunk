package com.example.mnaganu.dcbc.infrastructure.repository;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import com.example.mnaganu.dcbc.domain.model.SelectModel;
import com.example.mnaganu.dcbc.domain.repository.CopySourceSampleRepository;
import com.example.mnaganu.dcbc.infrastructure.handler.SampleRowCountCallbackHandler;
import com.example.mnaganu.dcbc.infrastructure.mapper.SampleRowMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository()
public class CopySourceSampleRepositoryImpl implements CopySourceSampleRepository {
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final Integer fetchSize;
    
    @Autowired
    public CopySourceSampleRepositoryImpl(
            @Qualifier("copySourceNamedParameterJdbcTemplate")
            NamedParameterJdbcTemplate namedParameterJdbcTemplate,
            @Qualifier("copySourceFetchSize") Integer fetchSize) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.fetchSize = fetchSize;
    }

    @Override
    public void truncate() {
        String sql = "TRUNCATE TABLE sample";
        namedParameterJdbcTemplate.update(sql, new MapSqlParameterSource());
    }

    @Override
    public Optional<SampleModel> selectById(int id) {
        String sql = "SELECT * FROM sample WHERE id = :id";
        SampleRowMapper rowMapper = new SampleRowMapper();
        SqlParameterSource parameters = new MapSqlParameterSource().addValue("id", id);
        List<SampleModel> list = namedParameterJdbcTemplate.query(sql, parameters, rowMapper);
        if (list.size() > 0) {
            return Optional.of(list.get(0));
        }
        return Optional.empty();
    }

    @Override
    public SelectModel<SampleModel> selectByOffset(int offset, int limit) {
        SelectModel<SampleModel> selectModel =
                SelectModel.<SampleModel>builder()
                        .list(new ArrayList<SampleModel>())
                        .offset(0)
                        .limit(0)
                        .total(0)
                        .build();

        if (offset < 0 || limit < 1) {
            return selectModel;
        }

        String sql = "SELECT * FROM sample ORDER BY id";
        SqlParameterSource parameterSource = new MapSqlParameterSource();
        SampleRowCountCallbackHandler handler = new SampleRowCountCallbackHandler(offset, limit);
        namedParameterJdbcTemplate.getJdbcTemplate().setFetchSize(fetchSize);
        namedParameterJdbcTemplate.query(sql, parameterSource, handler);

        return SelectModel.<SampleModel>builder()
                .list(handler.getList())
                .offset(offset)
                .limit(limit)
                .total(handler.getRowCount())
                .build();

    }

    @Override
    public SelectModel<SampleModel> selectByOffsetId(int offsetId, int offset, int limit) {
        SelectModel<SampleModel> selectModel =
                SelectModel.<SampleModel>builder()
                        .list(new ArrayList<SampleModel>())
                        .offset(0)
                        .limit(0)
                        .total(0)
                        .build();

        if (offset < 0 || limit < 1) {
            return selectModel;
        }

        String sql = "SELECT * FROM sample WHERE id > :id ORDER BY id";
        SqlParameterSource parameterSource = new MapSqlParameterSource("id", offsetId);
        SampleRowCountCallbackHandler handler = new SampleRowCountCallbackHandler(offset, limit);
        namedParameterJdbcTemplate.getJdbcTemplate().setFetchSize(fetchSize);
        namedParameterJdbcTemplate.query(sql, parameterSource, handler);

        return SelectModel.<SampleModel>builder()
                .list(handler.getList())
                .offset(offset)
                .limit(limit)
                .total(handler.getRowCount())
                .build();
    }

    @Override
    public int insert(SampleModel model) {
        if (ObjectUtils.isEmpty(model)) {
            return 0;
        }

        String sql = "INSERT INTO sample ( " +
                "id, " +
                "name " +
                " ) VALUES ( " +
                ":id, " +
                ":name" +
                " ) ";

        SqlParameterSource parameters = new MapSqlParameterSource("id", model.getId())
                .addValue("name", model.getName().orElse(null));

        return namedParameterJdbcTemplate.update(sql, parameters);
    }
}
