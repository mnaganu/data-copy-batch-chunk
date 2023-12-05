package com.example.mnaganu.dcbc.infrastructure.repository;

import com.example.mnaganu.dcbc.domain.model.SampleModel;
import com.example.mnaganu.dcbc.domain.model.SelectModel;
import com.example.mnaganu.dcbc.infrastructure.mapper.SampleRowMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class CopySourceSampleRepositoryImplTest {
    private final CopySourceSampleRepositoryImpl copySourceSampleRepositoryImpl;
    private final DataSource dataSource;

    @Autowired
    public CopySourceSampleRepositoryImplTest(
            CopySourceSampleRepositoryImpl copySourceSampleRepositoryImpl,
            @Qualifier("copySourceDataSource") DataSource dataSource) {
        this.copySourceSampleRepositoryImpl = copySourceSampleRepositoryImpl;
        this.dataSource = dataSource;
    }

    @BeforeEach
    public void initDb() {
        createTable();
        createTestData();
    }

    @Test
    void selectById_データなし() {
        //存在しない id
        Optional<SampleModel> oSampleModel = copySourceSampleRepositoryImpl.selectById(-1);
        assertThat(oSampleModel.isEmpty()).isTrue();
    }

    @Test
    void selectById_データあり() {
        //存在しない id
        Optional<SampleModel> oSampleModel = copySourceSampleRepositoryImpl.selectById(1);
        assertThat(oSampleModel.isPresent()).isTrue();
        assertThat(oSampleModel.get().getId()).isEqualTo(1);
        assertThat(oSampleModel.get().getName().get()).isEqualTo("name1");

        oSampleModel = copySourceSampleRepositoryImpl.selectById(2);
        assertThat(oSampleModel.isPresent()).isTrue();
        assertThat(oSampleModel.get().getId()).isEqualTo(2);
        assertThat(oSampleModel.get().getName().isEmpty()).isTrue();
    }

    @Test
    void selectByOffset_検索結果0件() {
        //offset が 0よりも小さい
        SelectModel<SampleModel> selectModel = copySourceSampleRepositoryImpl.selectByOffset(-1, 1);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(0);
        assertThat(selectModel.getTotal()).isEqualTo(0);

        //limit が 1よりも小さい
        selectModel = copySourceSampleRepositoryImpl.selectByOffset(0, 0);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(0);
        assertThat(selectModel.getTotal()).isEqualTo(0);

        //offset が 0よりも小さい　
        //limit が 1よりも小さい
        selectModel = copySourceSampleRepositoryImpl.selectByOffset(-1, 0);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(0);
        assertThat(selectModel.getTotal()).isEqualTo(0);

        //テーブルを作り直してデータを全て消す。
        createTable();
        selectModel = copySourceSampleRepositoryImpl.selectByOffset(1, 10);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(1);
        assertThat(selectModel.getLimit()).isEqualTo(10);
        assertThat(selectModel.getTotal()).isEqualTo(0);
    }

    @Test
    void selectByOffset_検索結果1件() {
        //先頭の1件
        SelectModel<SampleModel> selectModel = copySourceSampleRepositoryImpl.selectByOffset(0, 1);
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(1);
        assertThat(selectModel.getTotal()).isEqualTo(3);

        List<SampleModel> list = selectModel.getList();
        assertThat(list.size()).isEqualTo(1);

        SampleModel actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(1);
        assertThat(actual.getName().get()).isEqualTo("name1");

        //中間の1件
        selectModel = copySourceSampleRepositoryImpl.selectByOffset(1, 1);
        assertThat(selectModel.getOffset()).isEqualTo(1);
        assertThat(selectModel.getLimit()).isEqualTo(1);
        assertThat(selectModel.getTotal()).isEqualTo(3);

        list = selectModel.getList();
        assertThat(list.size()).isEqualTo(1);

        actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(2);
        assertThat(actual.getName().isEmpty()).isTrue();

        //最後の1件
        selectModel = copySourceSampleRepositoryImpl.selectByOffset(2, 10);
        assertThat(selectModel.getOffset()).isEqualTo(2);
        assertThat(selectModel.getLimit()).isEqualTo(10);
        assertThat(selectModel.getTotal()).isEqualTo(3);

        list = selectModel.getList();
        assertThat(list.size()).isEqualTo(1);

        actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(3);
        assertThat(actual.getName().get()).isEqualTo("name3");
    }

    @Test
    void selectByOffset_検索結果が複数件() {
        SelectModel<SampleModel> selectModel = copySourceSampleRepositoryImpl.selectByOffset(0, 10);
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(10);
        assertThat(selectModel.getTotal()).isEqualTo(3);

        List<SampleModel> list = selectModel.getList();
        assertThat(list.size()).isEqualTo(3);

        SampleModel actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(1);
        assertThat(actual.getName().get()).isEqualTo("name1");

        actual = list.get(1);
        assertThat(actual.getId()).isEqualTo(2);
        assertThat(actual.getName().isEmpty()).isTrue();

        actual = list.get(2);
        assertThat(actual.getId()).isEqualTo(3);
        assertThat(actual.getName().get()).isEqualTo("name3");
    }

    @Test
    void selectByOffsetId_検索結果0件() {
        //offset が 0よりも小さい
        SelectModel<SampleModel> selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(-1, -1, 1);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(0);
        assertThat(selectModel.getTotal()).isEqualTo(0);

        //limit が 1よりも小さい
        selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(-1,0, 0);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(0);
        assertThat(selectModel.getTotal()).isEqualTo(0);

        //offset が 0よりも小さい　
        //limit が 1よりも小さい
        selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(-1,-1, 0);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(0);
        assertThat(selectModel.getTotal()).isEqualTo(0);

        //テーブルを作り直してデータを全て消す。
        createTable();
        selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(-1,1, 10);
        assertThat(selectModel.getList().isEmpty()).isTrue();
        assertThat(selectModel.getOffset()).isEqualTo(1);
        assertThat(selectModel.getLimit()).isEqualTo(10);
        assertThat(selectModel.getTotal()).isEqualTo(0);
    }

    @Test
    void selectByOffsetId_検索結果1件() {
        //先頭の1件
        SelectModel<SampleModel> selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(0, 0, 1);
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(1);
        assertThat(selectModel.getTotal()).isEqualTo(3);

        List<SampleModel> list = selectModel.getList();
        assertThat(list.size()).isEqualTo(1);

        SampleModel actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(1);
        assertThat(actual.getName().get()).isEqualTo("name1");

        //中間の1件
        selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(1, 0, 1);
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(1);
        assertThat(selectModel.getTotal()).isEqualTo(2);

        list = selectModel.getList();
        assertThat(list.size()).isEqualTo(1);

        actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(2);
        assertThat(actual.getName().isEmpty()).isTrue();

        //最後の1件
        selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(2, 0, 10);
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(10);
        assertThat(selectModel.getTotal()).isEqualTo(1);

        list = selectModel.getList();
        assertThat(list.size()).isEqualTo(1);

        actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(3);
        assertThat(actual.getName().get()).isEqualTo("name3");
    }

    @Test
    void selectByOffsetId_検索結果が複数件() {
        SelectModel<SampleModel> selectModel = copySourceSampleRepositoryImpl.selectByOffsetId(0, 0, 10);
        assertThat(selectModel.getOffset()).isEqualTo(0);
        assertThat(selectModel.getLimit()).isEqualTo(10);
        assertThat(selectModel.getTotal()).isEqualTo(3);

        List<SampleModel> list = selectModel.getList();
        assertThat(list.size()).isEqualTo(3);

        SampleModel actual = list.get(0);
        assertThat(actual.getId()).isEqualTo(1);
        assertThat(actual.getName().get()).isEqualTo("name1");

        actual = list.get(1);
        assertThat(actual.getId()).isEqualTo(2);
        assertThat(actual.getName().isEmpty()).isTrue();

        actual = list.get(2);
        assertThat(actual.getId()).isEqualTo(3);
        assertThat(actual.getName().get()).isEqualTo("name3");
    }

    @Test
    void truncateTest() {
        List<SampleModel> list = getTableData();
        assertThat(list.size()).isEqualTo(3);
        copySourceSampleRepositoryImpl.truncate();
        list = getTableData();
        assertThat(list.size()).isEqualTo(0);
    }

    @Test
    void insertTest() {
        List<SampleModel> list = getTableData();
        assertThat(list.size()).isEqualTo(3);

        //name が null
        SampleModel data1 = SampleModel.builder()
                .id(100)
                .name(null)
                .build();

        int cnt = copySourceSampleRepositoryImpl.insert(data1);
        assertThat(cnt).isEqualTo(1);

        list = getTableData();
        assertThat(list.size()).isEqualTo(4);

        SampleModel actual = list.get(3);
        assertThat(actual.getId()).isEqualTo(100);
        assertThat(actual.getName().isEmpty()).isTrue();

        //name に値が入っている。
        SampleModel data2 = SampleModel.builder()
                .id(200)
                .name("name200")
                .build();

        cnt = copySourceSampleRepositoryImpl.insert(data2);
        assertThat(cnt).isEqualTo(1);

        list = getTableData();
        assertThat(list.size()).isEqualTo(5);

        actual = list.get(4);
        assertThat(actual.getId()).isEqualTo(200);
        assertThat(actual.getName().get()).isEqualTo("name200");
    }

    private void createTable() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        String sql = "DROP TABLE IF EXISTS `sample`";
        jdbcTemplate.execute(sql);

        sql = "CREATE TABLE `sample` (" +
                "`id` int UNIQUE NOT NULL," +
                "`name` text" +
                ");";
        jdbcTemplate.execute(sql);
    }

    private void createTestData() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        String sql = "INSERT INTO sample VALUES(1, 'name1');";
        jdbcTemplate.execute(sql);

        sql = "INSERT INTO sample VALUES(2, NULL);";
        jdbcTemplate.execute(sql);

        sql = "INSERT INTO sample VALUES(3, 'name3');";
        jdbcTemplate.execute(sql);
    }

    private List<SampleModel> getTableData() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "SELECT * FROM sample";
        SampleRowMapper sampleRowMapper = new SampleRowMapper();
        return jdbcTemplate.query(sql, sampleRowMapper);
    }
}
