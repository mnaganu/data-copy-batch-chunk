# data-copy-batch-chunk

## 概要
２つのDBに接続し、test_db1 の Sample テーブルから test_db2 の Sample テーブルへデータをコピーするプログラム。  
[data-copy-batch](https://github.com/mnaganu/data-copy-batch)　を Spring Batch の chunk を利用するように修正を行った。

## Spring batch
Spring Batchとは、Spring Frameworkを中心としたSpringプロジェクトのひとつで、バッチ開発用のフレームワークです。

## Job と step

### job
Spring Batchにおけるバッチアプリケーションの一連の処理をまとめた1実行単位。

### step
Jobを構成する処理の単位。1つのJobに1～N個のStepをもたせることが可能。  
1つのJobを複数のStepに分割して処理することにより、処理の再利用、並列化、条件分岐が可能になる。  
Stepは、チャンクまたはタスクレットのいずれかで実装する。

## タスクレット(Tasklet)とチャンク(Chunk)

### タスクレット(Tasklet)
データ読み書きのタイミングを開発者が自由に決定できる。  
タスクレット(Tasklet)は、`execute` という 1 つのメソッドを持つ単純なインターフェースです。  
`execute` は、`RepeatStatus.FINISHED` を返すか、例外をスローして失敗を通知するまで、TaskletStep によって繰り返し呼び出されます。

### チャンク(Chunk)
コミットインターバルの設定することで、自動で設定した処理件数ごとにコミットを行う。  
チャンク(Chunk)は、データを一度に1つずつ読み取りトランザクション境界内に書き込まれる「チャンク」を作成すること。  
1 つのアイテムが ItemReader から読み込まれ、ItemProcessor に渡されて集約されます。  
読み込まれたアイテムの数がコミット間隔に等しくなると、ItemWriter によってチャンク全体が書き出され、トランザクションがコミットされます。

## 複数データソース
Spring Batchは、内部でメタテーブルを持っている。  
ジョブの実行時に、メタテーブルに書き込みながら実行する。  
タスクレットモデルでも、チャンクモデルでも、メタテーブルに書き込みながら実行する。  
テーブルのDDLは、各プラットフォームに合わせたsqlファイルがSpring Batchのjarに内包されている。  
バッチ処理で利用するDBとバッチ内部で利用するメタテーブルを分ける。
[Metadata Schema](https://spring.pleiades.io/spring-batch/docs/current/reference/html/schema-appendix.html)

### application.yml
`application.yml` に接続したいDBの情報を記載しておく。  
内部で利用するメタテーブルの情報を`batch`に、コピー元のDBの情報を`copy-source`に、コピー先のDBの情報を`copy-to`に記載する。  
メタテーブルを保存したくない場合は、メタテーブルの保存先を H2DB にすることで、バッチ実行終了後削除される。  
メタテーブルの初期化を常に行うようにしている。
```yml
spring:
  datasource:
    batch:
      # Spring Batch メタデータをMySQLに保存する場合の設定
      driver-class-name: com.mysql.cj.jdbc.Driver
      url: jdbc:mysql://localhost:63306/test_db3?userCursorFetch=true
      username: test
      password: testpass
      # Spring Batch メタデータを　H2DB に保存する場合の設定
    # driver-class-name: org.h2.Driver
    # url: jdbc:h2:mem:batch_db:Mode=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false
    # username: test
    # password: testpass
    copy-source:
      driver-class-name: com.mysql.cj.jdbc.Driver
      url: jdbc:mysql://localhost:63306/test_db1?userCursorFetch=true
      username: test
      password: testpass
      fetchsize: 1000
    copy-to:
      driver-class-name: com.mysql.cj.jdbc.Driver
      url: jdbc:mysql://localhost:63306/test_db2?userCursorFetch=true
      username: test
      password: testpass
      fetchsize: 1000
  batch:
    jdbc:
      # メタテーブルの初期化を常に行うように設定
      initialize-schema: always
```
### Configuration
`application.yml` に記載した情報を取得する`BatchDataSourceConfiguration`クラスを作成する。  
メタデータ格納先DBには`@BatchDataSource`を付与する。  
ただし、メタデータ格納先ではない方のDBに`@Primary`を付与しない正常に動かないので    
コピー元のDBの情報を取得する`CopySourceConfiguration`と  
コピー先の情報を取得する`CopyToConfiguration`のどちらか一方に`@Primary`をつける必要がある。  
今回は、コピー元のDBの情報を取得する`CopySourceConfiguration`に`@Primary`を付与した。
```Java
@lombok.Getter
@lombok.Setter
@Component
@ConfigurationProperties(prefix = "spring.datasource.batch")
public class BatchDataSourceConfiguration {
    private String driverClassName;
    private String url;
    private String username;
    private String password;

    @BatchDataSource
    @Bean(name = "batchDataSource")
    public DataSource createDataSource() {
        return DataSourceBuilder.create()
                .driverClassName(driverClassName)
                .url(url)
                .username(username)
                .password(password)
                .build();
    }

}
```
```Java
@lombok.Getter
@lombok.Setter
@Component
@ConfigurationProperties(prefix = "spring.datasource.copy-source")
public class CopySourceDataSourceConfiguration {
    private String driverClassName;
    private String url;
    private String username;
    private String password;
    private int fetchsize;

    @Primary
    @Bean(name = "copySourceDataSource")
    public DataSource createDataSource() {
        return DataSourceBuilder.create()
                .driverClassName(driverClassName)
                .url(url)
                .username(username)
                .password(password)
                .build();
    }

    @Primary
    @Bean(name = "copySourceNamedParameterJdbcTemplate")
    public NamedParameterJdbcTemplate createNamedParameterJdbcTemplate(
            @Qualifier("copySourceDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Primary
    @Bean(name = "copySourceFetchSize")
    public Integer createFetchSize() {
        return Integer.valueOf(fetchsize);
    }
    
}
```

# JobやStepを跨いだデータの受け渡し
JobやStepを跨いだデータの受け渡しには Context を利用する。

## ChunkContext
Chunk 実行中に共有されるデータ。  
次の Chunk 実行時には別の Context が用意される。  
ChunkContext から StepContext を取得できる。

## StepContext
Step 実行中に共有されるデータ。  
Step を通じて Chunk を跨いで参照可能。
次の Step 実行時には別の Context が用意される。  
StepContext から JobContext を取得できる。

## jobContext
Job 実行中に共有されるデータ。  
Job を通じて Step を跨いで参照可能。  
次の Job 実行時には別の Context が用意される。

# Chunk
下記３つの処理を実装していく
- データを読み込む処理（ItemReader）
- データを加工する処理（ItemProcessor）
- データを書き出す処理（ItemWriter）

## CopySourceReader
コピー元のデータをDBから取得する処理を記載している。

### readメソッド（ItemReaderのインタフェース）
ここにデータの読み込み処理を記載していく。  
ID順にソートしたデータから、指定したID以降のデータを順次読み込み１件づつ返す処理を記載している。  
chunk で指定された数だけデータを読み込んでから書き込み処理が行われる。

### beforeStep（StepExecutionListenerのインタフェース）
step 開始前に呼び出される。  
step 開始時の初期化処理を書いておく。  
１件づつ読み込む時の offset の値やコピー済みのIDの最大値を初期化している。

### afterStep（StepExecutionListenerのインタフェース）
step 終了後に呼び出される。    
step 処理の後処理を行う。  
処理の内容に応じて、ExitStatus を返す。

### beforeChunk（ChunkListenerのインタフェース）
chunk 開始前に呼び出される。  
chunk で指定された数だけデータを読み込む前の事前処理を記載する。  
ChunkContext から jobContext を取得し、コピー済みのIDの最大値を読み込んでメンバ変数に設定している。  

### afterChunk（ChunkListenerのインタフェース）
chunk 終了後に呼び出される。    
chunk で指定された数だけデータ処理が終わった後の事後処理を記載する。 

### afterChunkError（ChunkListenerのインタフェース）
chunk 処理中にエラーが発生した時に呼び出される。  

```Java
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
```

## CopyProcessor
コピー元のデータを加工する処理を記載している。  
今回は特に加工の必要ないので、ログを出すだけで何も実装しておりません。

### processメソッド（ItemProcessorのインタフェース）
ここにデータの加工処理を記載していく。  
引数で受け取ったデータ（ItemReaderで読み込んだデータ）を使ってデータを加工して戻り値で返す。  
特に加工が必要ない場合は、引数のデータをそのまま返す。  

### beforeStep（StepExecutionListenerのインタフェース）
step 開始前に呼び出される。  
step 開始時の初期化処理を書いておく。  

### afterStep（StepExecutionListenerのインタフェース）
step 終了後に呼び出される。    
step 処理の後処理を行う。  

### beforeChunk（ChunkListenerのインタフェース）
chunk 開始前に呼び出される。  
chunk で指定された数だけデータを読み込む前の事前処理を記載する。  

### afterChunk（ChunkListenerのインタフェース）
chunk 終了後に呼び出される。    
chunk で指定された数だけデータ処理が終わった後の事後処理を記載する。

### afterChunkError（ChunkListenerのインタフェース）
chunk 処理中にエラーが発生した時に呼び出される。  

```Java
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
```

## CopyToWriter
コピー先のDBにデータを登録処理を記載している。

### processメソッド（ItemWriterのインタフェース）
ここにデータの書き込み処理を記載していく。
chunk で指定された数だけ引数でデータを受け取る。  
コピー先のDBに登録済みかどうか確認し、未登録の場合だけ登録する。  
登録済みの場合は特に何もしない。

### beforeStep（StepExecutionListenerのインタフェース）
step 開始前に呼び出される。  
step 開始時の初期化処理を書いておく。

### afterStep（StepExecutionListenerのインタフェース）
step 終了後に呼び出される。    
step 処理の後処理を行う。

### beforeChunk（ChunkListenerのインタフェース）
chunk 開始前に呼び出される。  
chunk で指定された数だけデータを読み込む前の事前処理を記載する。

### afterChunk（ChunkListenerのインタフェース）
chunk 終了後に呼び出される。    
chunk で指定された数だけデータ処理が終わった後の事後処理を記載する。  
ChunkContext から jobContext を取得し、コピー済みのIDの最大値を設定している。  

### afterChunkError（ChunkListenerのインタフェース）
chunk 処理中にエラーが発生した時に呼び出される。  

```Java
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
```

# ChunksConfig
Chunk の実装が終わったら、`Config`を書いて、job と step の Bean 定義を行う。
step の Bean 作成は、`StepBuilder`を使って行う。    
job の Bean 作成は、`JobBuilder`を使って行う。

```Java
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
        return new JobBuilder("chunksJob", jobRepository)
                .start(dataCopyStep(jobRepository, transactionManager))
                .build();
    }

}
```

# Test
`@SpringBootTest`のアノテーションをつけるとテスト実行時に登録されているジョブが全て実行されてしまうので  
テストで利用する `application.yml` に job が実行されないように設定を追加しておく。

```yml
spring:
  datasource:
    batch:
      driver-class-name: org.h2.Driver
      url: jdbc:h2:mem:batch_db:Mode=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false
      username: test
      password: testpass
    copy-source:
      driver-class-name: org.h2.Driver
      url: jdbc:h2:mem:test_db1:Mode=MySQL;DB_CLOSE_DELAY=-1
      username: test
      password: testpass
      fetchsize: 1000
    copy-to:
      driver-class-name: org.h2.Driver
      url: jdbc:h2:mem:test_db2:Mode=MySQL;DB_CLOSE_DELAY=-1
      username: test
      password: testpass
      fetchsize: 1000
  # @SpringBootTest で job が実行されないようにするための設定
  batch:
    job:
      enabled: false
```

`@SpringBatchTest`のアノテーションをつけることで`JobLauncherTestUtils`が利用できるようになるので  
`jobLauncherTestUtils.launchJob()`でジョブ実行できる。  
特定の`step`だけを実行したい場合は、`jobLauncherTestUtils.launchStep("step名")`で実行できる。
