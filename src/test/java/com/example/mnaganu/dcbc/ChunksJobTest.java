package com.example.mnaganu.dcbc;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import com.example.mnaganu.dcbc.infrastructure.mapper.SampleRowMapper;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@SpringBatchTest
public class ChunksJobTest {
    private final JobLauncherTestUtils jobLauncherTestUtils;
    private final DataSource copySourceDataSource;
    private final DataSource copyToDataSource;

    @Autowired
    public ChunksJobTest(
            JobLauncherTestUtils jobLauncherTestUtils,
            @Qualifier("copySourceDataSource") DataSource copySourceDataSource,
            @Qualifier("copyToDataSource") DataSource copyToDataSource) {
        this.jobLauncherTestUtils = jobLauncherTestUtils;
        this.copySourceDataSource = copySourceDataSource;
        this.copyToDataSource = copyToDataSource;
    }

    @Test
    void chunksJob_コピー元のデータ0件() {
        //コピー元のテーブルデータが0件であることの確認
        createTable(copySourceDataSource);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList).isEmpty();

        //コピー先のテーブルデータが0件であることの確認
        createTable(copyToDataSource);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが0件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList).isEmpty();

        //コピー先のテーブルデータが0件であることの確認
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();
    }


    @Test
    void chunksJob_コピー元のデータ1件コピー先データなし() {
        int index = 0;

        //コピー元のテーブルデータが1件であることの確認
        createTable(copySourceDataSource);
        createTestData(copySourceDataSource, "copySource", 1);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルデータが0件であることの確認
        createTable(copyToDataSource);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが1件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルにデータがコピーされること
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(1);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }
    }

    @Test
    void chunksJob_コピー元のデータ1件コピー先データあり() {
        int index = 0;

        //コピー元のテーブルデータが1件であることの確認
        createTable(copySourceDataSource);
        createTestData(copySourceDataSource, "copySource", 1);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルデータが100件であることの確認
        createTable(copyToDataSource);
        createTestData(copyToDataSource, "copyTo", 100);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(100);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copyTo" + index);
            index++;
        }

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが1件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルにデータがコピーされること
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(1);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }
    }

    @Test
    void chunksJob_コピー元のデータ1000件コピー先データなし() {
        int index = 0;

        //コピー元のテーブルデータが1000件であることの確認
        createTable(copySourceDataSource);
        createTestData(copySourceDataSource, "copySource", 1000);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1000);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルデータが0件であることの確認
        createTable(copyToDataSource);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが1000件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1000);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルにデータがコピーされること
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(1000);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }
    }

    @Test
    void chunksJob_コピー元のデータ1000件コピー先データあり() {
        int index = 0;

        //コピー元のテーブルデータが1件であることの確認
        createTable(copySourceDataSource);
        createTestData(copySourceDataSource, "copySource", 1000);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1000);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルデータが100件であることの確認
        createTable(copyToDataSource);
        createTestData(copyToDataSource, "copyTo", 100);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(100);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copyTo" + index);
            index++;
        }

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが1000件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1000);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルにデータがコピーされること
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(1000);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }
    }

    @Test
    void chunksJob_コピー元のデータ1001件コピー先データなし() {
        int index = 0;

        //コピー元のテーブルデータが1001件であることの確認
        createTable(copySourceDataSource);
        createTestData(copySourceDataSource, "copySource", 1001);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1001);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルデータが0件であることの確認
        createTable(copyToDataSource);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが1000件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1001);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルにデータがコピーされること
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(1001);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }
    }

    @Test
    void chunksJob_コピー元のデータ1001件コピー先データあり() {
        int index = 0;

        //コピー元のテーブルデータが1件であることの確認
        createTable(copySourceDataSource);
        createTestData(copySourceDataSource, "copySource", 1001);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1001);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルデータが100件であることの確認
        createTable(copyToDataSource);
        createTestData(copyToDataSource, "copyTo", 100);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(100);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copyTo" + index);
            index++;
        }

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが1001件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList.size()).isEqualTo(1001);
        index = 0;
        for (SampleModel model : copySourceList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }

        //コピー先のテーブルにデータがコピーされること
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList.size()).isEqualTo(1001);
        index = 0;
        for (SampleModel model : copyToList) {
            assertThat(model.getId()).isEqualTo(index);
            assertThat(model.getName().get()).isEqualTo("copySource" + index);
            index++;
        }
    }

    @Test
    void chunksJob_コピー元のテーブルなし() {
        //コピー元のテーブルを削除
        dropTable(copySourceDataSource);

        //コピー先のテーブルデータが0件であることの確認
        createTable(copyToDataSource);
        List<SampleModel> copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertThat(ExitStatus.FAILED.getExitCode()).isEqualTo(jobExecution.getExitStatus().getExitCode());
            assertThat(jobExecution.getExitStatus().getExitDescription().contains("コピー元の sample テーブルのデータを取得に失敗しました。")).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー先のテーブルデータが0件であることの確認
        copyToList = getTableData(copyToDataSource);
        assertThat(copyToList).isEmpty();

    }

    @Test
    void chunksJob_コピー先のテーブルなし() {
        //コピー元のテーブルデータが0件であることの確認
        createTable(copySourceDataSource);
        List<SampleModel> copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList).isEmpty();

        //コピー先のテーブルを消す
        dropTable(copyToDataSource);

        //Job実行
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob();
            assertThat(ExitStatus.FAILED.getExitCode()).isEqualTo(jobExecution.getExitStatus().getExitCode());
            assertThat(jobExecution.getExitStatus().getExitDescription().contains("コピー先の sample テーブルの truncate に失敗しました。")).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        //コピー元のテーブルデータが0件であることの確認
        copySourceList = getTableData(copySourceDataSource);
        assertThat(copySourceList).isEmpty();
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
