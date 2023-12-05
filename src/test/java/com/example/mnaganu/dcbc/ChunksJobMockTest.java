package com.example.mnaganu.dcbc;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import com.example.mnaganu.dcbc.domain.model.SelectModel;
import com.example.mnaganu.dcbc.domain.repository.CopySourceSampleRepository;
import com.example.mnaganu.dcbc.infrastructure.mapper.SampleRowMapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;

@SpringBootTest
@SpringBatchTest
public class ChunksJobMockTest {

    private final JobLauncherTestUtils jobLauncherTestUtils;
    private DataSource copySourceDataSource;
    private DataSource copyToDataSource;

    @MockBean
    CopySourceSampleRepository mockCopySourceSampleRepository;

    @Autowired
    public ChunksJobMockTest(
            JobLauncherTestUtils jobLauncherTestUtils,
            @Qualifier("copySourceDataSource") DataSource copySourceDataSource,
            @Qualifier("copyToDataSource") DataSource copyToDataSource) {
        this.jobLauncherTestUtils = jobLauncherTestUtils;
        this.copySourceDataSource = copySourceDataSource;
        this.copyToDataSource = copyToDataSource;
    }


    @Test
    void chunksJob_コピー元のテーブル読み込みに失敗() {
        //CopySourceSampleRepository のモックの設定
        //リストを作成する
        List<SampleModel> copySourceList1 = new ArrayList<>();
        List<SampleModel> copySourceList2 = new ArrayList<>();
        copySourceList1.add(
                SampleModel.builder()
                        .id(1)
                        .name("name1")
                        .build());
        copySourceList2.add(
                SampleModel.builder()
                        .id(1)
                        .name("name2")
                        .build());

        //id が重複したリストを持つ SelectModel を作成
        SelectModel<SampleModel> selectModel1 =
                SelectModel.<SampleModel>builder()
                        .list(copySourceList1)
                        .offset(0)
                        .limit(1)
                        .total(2)
                        .build();
        SelectModel<SampleModel> selectModel2 =
                SelectModel.<SampleModel>builder()
                        .list(copySourceList2)
                        .offset(1)
                        .limit(1)
                        .total(2)
                        .build();

        //selectByOffsetId メソッドが呼ばれたら、キーが重複したデータを返すようにする。
        Mockito.when(mockCopySourceSampleRepository.selectByOffsetId(anyInt(), anyInt(), anyInt()))
                .thenReturn(selectModel1)  //1回目の呼び出し
                .thenReturn(selectModel2)  //2回目の呼び出し
                .thenThrow(new RuntimeException("エラーが発生しました。")); //3回目の呼び出し

        //コピー先のテーブルデータが0件であることの確認
        createTable(copyToDataSource);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertThat(ExitStatus.FAILED.getExitCode()).isEqualTo(jobExecution.getExitStatus().getExitCode());
            assertThat(jobExecution.getExitStatus().getExitDescription().contains("エラーが発生しました。")).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    private void dropTable(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "DROP TABLE IF EXISTS `sample`";
        jdbcTemplate.execute(sql);
    }

    private void createTable(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "DROP TABLE IF EXISTS `sample`";
        jdbcTemplate.execute(sql);

        sql = "CREATE TABLE `sample` (" +
                "`id` int UNIQUE NOT NULL," +
                "`name` text" +
                ");";
        jdbcTemplate.execute(sql);
    }

    private void createTestData(DataSource dataSource, String name, int count) {
        if (count < 1) {
            return;
        }
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        for (int i=0; i< count; i++) {
            String sql = String.format("INSERT INTO sample VALUES( %d, '%s%d');", i, name, i);
            jdbcTemplate.execute(sql);
        }
    }

    private List<SampleModel> getTableData(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "SELECT * FROM sample";
        SampleRowMapper sampleRowMapper = new SampleRowMapper();
        return jdbcTemplate.query(sql, sampleRowMapper);
    }

}
