# 4 РЕАЛИЗАЦИЯ

В настоящей главе описывается реализация решения 2, выбранного в §3.4.4. Изложение следует снизу вверх: сначала рассматриваются вспомогательные компоненты (контекст транзакции, расширение writer-а), затем модифицированные потоковые компоненты (источник, приёмник), и наконец — управляющий слой (декоратор `MicroBatchExecution`, SPI-интерфейс и его реализация).

## 4.1 Структура изменений в репозитории SPYT

Реализация затрагивает пять модулей Gradle-проекта SPYT.

**`data-source`** — базовый модуль записи в динамические таблицы. Изменение: класс [`YtDynamicTableWriter`](data-source/src/main/scala/tech/ytsaurus/spyt/format/YtDynamicTableWriter.scala:20) расширяется необязательным параметром родительской транзакции.

**`data-source-extended`** — модуль потоковой обработки. Изменения: модификация [`YtStreamingSource.commit`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/streaming/YtStreamingSource.scala:112) и [`YtStreamingSink.addBatch`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/streaming/YtStreamingSink.scala:21); новые классы [`YtStreamingTransactionContext`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/streaming/YtStreamingTransactionContext.scala:7) и [`YTsaurusStreamingTransactionSupport`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/adapter/YTsaurusStreamingTransactionSupport.scala:13).

**`spark-adapter`** — модуль версионных адаптеров Spark. Изменение: новый интерфейс [`StreamingTransactionSupport`](spark-adapter/api/src/main/scala/tech/ytsaurus/spyt/adapter/StreamingTransactionSupport.scala:17) и вспомогательный трейт [`StreamingTransactionHandle`](spark-adapter/api/src/main/scala/tech/ytsaurus/spyt/adapter/StreamingTransactionSupport.scala:7) в модуле `api`.

**`spark-patch`** — модуль инструментации байт-кода. Изменение: новый декоратор [`MicroBatchExecutionDecorators`](spark-adapter/impl/spark-3.2.2/bin/main/org/apache/spark/sql/execution/streaming/MicroBatchExecutionDecorators.scala:8), размещённый в `spark-adapter/impl/spark-3.2.2` и применяемый ко всем поддерживаемым версиям Spark через механизм `@Decorate`.

**`yt-wrapper`** — модуль утилит для работы с YTsaurus. Изменение: метод [`attachTransaction`](yt-wrapper/src/main/scala/tech/ytsaurus/spyt/wrapper/transaction/YtTransactionUtils.scala:45) в трейте `YtTransactionUtils`, оборачивающий операцию `AttachTransaction` клиентской библиотеки YT.

## 4.2 Контекст транзакции

Передача идентификатора транзакции и адреса rpc-прокси между методами драйвера реализована через [`YtStreamingTransactionContext`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/streaming/YtStreamingTransactionContext.scala:7) — объект-синглтон, инкапсулирующий `InheritableThreadLocal`.

Хранимый тип — `case class StreamingTxContext(txId: String, stickyAddress: Option[String])`. Использование `InheritableThreadLocal` вместо `ThreadLocal` обеспечивает видимость контекста в дочерних потоках, порождаемых Spark при планировании задач.

Помимо контекста транзакции, объект хранит атомарный флаг `recoveryNeededFlag: AtomicBoolean`. Флаг устанавливается декоратором при сбое батча и сигнализирует о том, что состояние потребителя YTsaurus может расходиться с commit-логом Spark. При следующем батче декоратор логирует предупреждение и сбрасывает флаг после успешной фиксации.

Публичный API объекта:

```scala
object YtStreamingTransactionContext {
  def get: Option[StreamingTxContext]
  def setContext(ctx: StreamingTxContext): Unit
  def currentTransactionId: Option[String]
  def currentStickyProxyAddress: Option[String]
  def clearTransactionId(): Unit
  def isRecoveryNeeded: Boolean
  def markRecoveryNeeded(): Unit
  def clearRecoveryNeeded(): Unit
}
```

Жизненный цикл контекста в рамках одного батча: декоратор устанавливает контекст перед вызовом оригинального `runBatch` и очищает его в блоке `finally` после завершения батча — независимо от того, завершился ли батч успешно или с ошибкой.

## 4.3 Модификация потокового источника

### 4.3.1 Метод getBatch

Метод [`YtStreamingSource.getBatch`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/streaming/YtStreamingSource.scala:92) не изменяется. Он формирует `YtQueueRDD` по диапазонам смещений и возвращает `DataFrame`. Смещение конца батча (`end: Offset`) передаётся Spark-ом в метод `commit` стандартным механизмом и не требует дополнительного сохранения в ThreadLocal.

### 4.3.2 Метод commit

Метод [`YtStreamingSource.commit`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/streaming/YtStreamingSource.scala:112) реализует два режима работы в зависимости от наличия активного контекста транзакции.

**Транзакционный режим** (контекст установлен декоратором): смещение потребителя продвигается в составе уже открытой транзакции. Метод читает `txId` и `stickyAddress` из `YtStreamingTransactionContext`, при наличии `stickyAddress` создаёт клиент, зафиксированный на конкретном rpc-прокси, и вызывает `offsetProvider.advance(..., parentTransactionId)`. Операция `advanceConsumer` выполняется в той же tablet-транзакции, что и запись данных на исполнителях.

**Нетранзакционный режим с включённым флагом** (контекст отсутствует, но `spark.yt.streaming.transactional=true`): метод завершается без действий. Это состояние возникает при сбое батча до установки контекста — транзакция не была открыта, и продвигать смещение не нужно.

**Нетранзакционный режим** (флаг выключен): штатное поведение — `offsetProvider.advance(..., None)` без транзакции.

```scala
override def commit(end: Offset): Unit = {
  val txContext = YtStreamingTransactionContext.get

  if (txContext.isEmpty) {
    val transactionalEnabled = sqlContext.sparkSession.ytConf(Streaming.Transactional)
    if (transactionalEnabled) {
      return
    }
    offsetProvider.advance(consumerPath, YtQueueOffset(end), lastCommittedOffset, maxOffset, None)(yt)
    return
  }

  val parentTransactionId = txContext.map(_.txId)
  val stickyAddr = txContext.flatMap(_.stickyAddress)
  val advanceClient: CompoundClient = stickyAddr match {
    case Some(addr) =>
      val conf = YtClientConfigurationConverter.ytClientConfiguration(...)
      YtClientProvider.ytClientFixedProxy(conf, addr)
    case None => yt
  }

  offsetProvider.advance(consumerPath, YtQueueOffset(end), lastCommittedOffset, maxOffset,
    parentTransactionId)(advanceClient)
}
```

## 4.4 Модификация потокового приёмника

Метод [`YtStreamingSink.addBatch`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/streaming/YtStreamingSink.scala:21) расширен поддержкой транзакционного режима. Изменения сосредоточены в двух местах: подготовка широковещательных переменных и создание клиента YTsaurus на исполнителе.

**На драйвере** метод читает `YtStreamingTransactionContext.get` и извлекает `txId` и `stickyAddress`. Оба значения упаковываются в широковещательные переменные `bcParentTransactionId` и `bcStickyProxyAddress`:

```scala
val txContext = YtStreamingTransactionContext.get
val bcParentTransactionId = txContext.map(ctx => sparkContext.broadcast(ctx.txId))
val bcStickyProxyAddress = txContext.flatMap(_.stickyAddress).map(sparkContext.broadcast)
```

**На исполнителе** в замыкании `foreachPartition` выбор клиента YTsaurus зависит от наличия `stickyAddress`. При его наличии создаётся клиент, зафиксированный на конкретном rpc-прокси (`YtClientProvider.ytClientFixedProxy`), что гарантирует направление запросов `attachTransaction` и `modifyRows` на тот же прокси, где открыта транзакция. При отсутствии — используется клиент из общего пула:

```scala
implicit val partitionYtClient: CompoundClient = stickyAddr match {
  case Some(addr) => YtClientProvider.ytClientFixedProxy(bcYtClientConfiguration.value, addr)
  case None => YtClientProvider.ytClient(bcYtClientConfiguration.value)
}
val attachedTx = txId.map(id => YtWrapper.attachTransaction(id))
val dynamicTableWriter = new YtDynamicTableWriter(bcPath.value, bcSchema.value,
  bcWriterConfig.value, bcParameters.value, attachedTx)
```

Объект `attachedTx: Option[ApiServiceTransaction]` передаётся в конструктор `YtDynamicTableWriter`. При его наличии все запросы `modifyRows` направляются в присоединённую транзакцию; при отсутствии — поведение не изменяется относительно нетранзакционного режима.

## 4.5 Расширение YtDynamicTableWriter

Класс [`YtDynamicTableWriter`](data-source/src/main/scala/tech/ytsaurus/spyt/format/YtDynamicTableWriter.scala:20) расширен необязательным параметром `parentTransaction: Option[ApiServiceTransaction] = None`:

```scala
class YtDynamicTableWriter(richPath: YPathEnriched, schema: StructType,
  wConfig: SparkYtWriteConfiguration, options: Map[String, String],
  parentTransaction: Option[ApiServiceTransaction] = None)
  (implicit ytClient: CompoundClient) extends OutputWriter
```

Параметр имеет значение по умолчанию `None`, что обеспечивает полную обратную совместимость: все существующие вызовы конструктора без явного указания транзакции продолжают работать без изменений.

Внутри класса параметр используется в методе `commitBatch`:

```scala
private def commitBatch(): Unit = {
  val request = modifyRowsRequestBuilder.build()
  YtWrapper.insertRows(request, parentTransaction)
  initBatch()
}
```

Метод [`YtWrapper.insertRows`](yt-wrapper/src/main/scala/tech/ytsaurus/spyt/wrapper/YtWrapper.scala) принимает `Option[ApiServiceTransaction]`: при `Some(tx)` запрос `ModifyRows` направляется через объект транзакции (`tx.modifyRows(request)`), при `None` — через обычный клиент YTsaurus.

Батчирование записи сохраняется: строки накапливаются в `ModifyRowsRequest.Builder` до достижения `wConfig.dynBatchSize`, после чего выполняется `commitBatch`. Параметр `dynBatchSize` управляется конфигурацией `spark.yt.write.dynBatchSize` и позволяет регулировать объём одного запроса `modifyRows` в рамках транзакции.

С точки зрения масштабируемости, батчирование играет ключевую роль. Поскольку все исполнители направляют запросы `modifyRows` в одну общую rpc-прокси, количество запросов к прокси прямо влияет на нагрузку на неё. Слишком малое значение `dynBatchSize` приводит к большому числу мелких запросов, увеличивая накладные расходы на сетевое взаимодействие и обработку на прокси. Слишком большое значение увеличивает размер каждого запроса, что может привести к росту задержек и потреблению памяти на исполнителях. Оптимальное значение зависит от характеристик сети, производительности rpc-прокси и объёма данных в батче, и подбирается экспериментально для конкретной конфигурации кластера.

## 4.6 Декоратор MicroBatchExecution

Декоратор [`MicroBatchExecutionDecorators`](spark-adapter/impl/spark-3.2.2/bin/main/org/apache/spark/sql/execution/streaming/MicroBatchExecutionDecorators.scala:8) подменяет метод `runBatch` класса `MicroBatchExecution` через механизм инструментации байт-кода `spark-patch`. Аннотации `@Decorate` и `@OriginClass` указывают агенту целевой класс и метод для подмены; оригинальный метод остаётся доступным под именем `__runBatch`.

Логика декоратора:

```scala
@DecoratedMethod
private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
  val sts = StreamingTransactionSupport.instance
  val transactionalStreamingEnabled = sts.isTransactionalStreamingEnabled(sparkSessionToRunBatch)

  if (!transactionalStreamingEnabled) {
    __runBatch(sparkSessionToRunBatch)
    return
  }

  if (sts.isRecoveryNeeded) {
    logWarning("...")
  }

  val currentTransaction: StreamingTransactionHandle = sts.createTransaction(sparkSessionToRunBatch)
  sts.setTransaction(currentTransaction)
  val batchIdAtEntry = currentBatchId
  var commitLogWritten = false

  try {
    __runBatch(sparkSessionToRunBatch)
    commitLogWritten = true
    MicroBatchExecutionDecorators.commitOffsets(availableOffsets)
    currentTransaction.commit()
    sts.clearRecoveryNeeded()
  } catch {
    case e: Exception =>
      sts.markRecoveryNeeded()
      try { currentTransaction.abort() } catch { case _: Throwable => () }
      if (commitLogWritten) {
        MicroBatchExecutionDecorators.deleteCommitLogEntry(commitLog, batchIdAtEntry)
      }
      throw e
  } finally {
    sts.clearTransactionId()
  }
}
```

При выключенном флаге декоратор прозрачно делегирует вызов оригинальному методу и не вносит накладных расходов.

При включённом флаге последовательность действий следующая.

1. Через SPI создаётся tablet-транзакция; её идентификатор и адрес rpc-прокси сохраняются в `YtStreamingTransactionContext`.
2. Вызывается оригинальный `__runBatch`, внутри которого Spark вызывает `Source.getBatch`, планирует и исполняет задания записи через `Sink.addBatch`.
3. После успешного возврата из `__runBatch` декоратор вызывает `commitOffsets(availableOffsets)`, который обходит все источники и вызывает `source.commit(offset)`. В `YtStreamingSource.commit` смещение потребителя продвигается в составе открытой транзакции.
4. Транзакция фиксируется вызовом `currentTransaction.commit()`.
5. Флаг `recoveryNeeded` сбрасывается.

При исключении на любом из шагов 2–4:

- устанавливается флаг `recoveryNeeded`;
- транзакция отменяется (`abort`);
- если исключение произошло после записи в commit-лог Spark (`commitLogWritten = true`), запись откатывается через `commitLog.purgeAfter(batchId - 1)`, чтобы Spark переисполнил батч с теми же смещениями.

Вспомогательный метод `commitOffsets` вынесен в объект-компаньон и вызывает `source.commit` только для источников типа `Source` (V1 API), что соответствует реализации `YtStreamingSource`.

## 4.7 SPI-точка расширения

### 4.7.1 Интерфейс StreamingTransactionSupport

Интерфейс [`StreamingTransactionSupport`](spark-adapter/api/src/main/scala/tech/ytsaurus/spyt/adapter/StreamingTransactionSupport.scala:17) объявляет контракт жизненного цикла распределённой транзакции:

```scala
trait StreamingTransactionSupport {
  def isTransactionalStreamingEnabled(sparkSession: SparkSession): Boolean
  def createTransaction(sparkSession: SparkSession): StreamingTransactionHandle
  def setTransaction(handle: StreamingTransactionHandle): Unit
  def setTransactionId(txId: String): Unit
  def setStickyProxyAddress(address: Option[String]): Unit
  def clearTransactionId(): Unit
  def isRecoveryNeeded: Boolean
  def markRecoveryNeeded(): Unit
  def clearRecoveryNeeded(): Unit
}
```

Вспомогательный трейт [`StreamingTransactionHandle`](spark-adapter/api/src/main/scala/tech/ytsaurus/spyt/adapter/StreamingTransactionSupport.scala:7) представляет открытую транзакцию:

```scala
trait StreamingTransactionHandle {
  def getId: String
  def getStickyProxyAddress: Option[String]
  def commit(): Unit
  def abort(): Unit
}
```

Загрузка реализации выполняется через `ServiceLoader` при первом обращении:

```scala
object StreamingTransactionSupport {
  lazy val instance: StreamingTransactionSupport =
    ServiceLoader.load(classOf[StreamingTransactionSupport]).findFirst().get()
}
```

Размещение интерфейса в модуле `spark-adapter/api` (а не в `data-source-extended`) обусловлено тем, что декоратор `MicroBatchExecutionDecorators` в `spark-patch` должен обращаться к нему без циклической зависимости.

### 4.7.2 Реализация YTsaurusStreamingTransactionSupport

Класс [`YTsaurusStreamingTransactionSupport`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/adapter/YTsaurusStreamingTransactionSupport.scala:13) регистрируется в `META-INF/services/tech.ytsaurus.spyt.adapter.StreamingTransactionSupport`.

Метод `createTransaction` создаёт tablet-транзакцию с параметром `sticky=true`, что соответствует `TransactionType.Tablet` в клиентской библиотеке YT:

```scala
override def createTransaction(sparkSession: SparkSession): StreamingTransactionHandle = {
  val ytClient = YtClientProvider.ytClient(
    YtClientConfigurationConverter.ytClientConfiguration(sparkSession.sparkContext.getConf)
  )
  val transaction = YtWrapper.createTransaction(
    parent = None,
    timeout = Duration.ofMinutes(5),
    sticky = true
  )(ytClient)
  new YtStreamingTransactionHandle(transaction)
}
```

Класс [`YtStreamingTransactionHandle`](data-source-extended/src/main/scala/tech/ytsaurus/spyt/adapter/YTsaurusStreamingTransactionSupport.scala:49) оборачивает `ApiServiceTransaction`. Адрес rpc-прокси, на которой открыта транзакция, получается через рефлексию — метод `getRpcProxyAddress` не входит в публичный API `ApiServiceTransaction`, поэтому вызывается через `getDeclaredMethods` с `setAccessible(true)`. При недоступности метода (например, в будущих версиях клиентской библиотеки) возвращается `None`, и исполнители используют клиент из общего пула без фиксации на конкретном прокси.

Методы `commit` и `abort` делегируют вызовы объекту `ApiServiceTransaction` с блокирующим ожиданием (`join()`). В `abort` перехватывается `IllegalStateException`, возникающий при попытке отменить уже завершённую транзакцию.

## 4.8 Конфигурация и обратная совместимость

Транзакционный режим управляется конфигурационным параметром `spark.yt.streaming.transactional` (тип `Boolean`, значение по умолчанию `false`). Параметр объявлен в объекте [`SparkYtConfiguration.Streaming`](data-source/src/main/scala/tech/ytsaurus/spyt/format/conf/SparkYtConfiguration.scala:88):

```scala
object Streaming {
  case object Transactional extends ConfigEntry[Boolean]("streaming.transactional", Some(false))
}
```

При значении `false` (по умолчанию) поведение всех компонентов идентично поведению до введения транзакционного режима:

- декоратор `MicroBatchExecutionDecorators` прозрачно делегирует вызов оригинальному `runBatch` без создания транзакции;
- `YtStreamingSource.commit` выполняет штатное продвижение смещения без транзакции;
- `YtStreamingSink.addBatch` не читает контекст транзакции; `bcParentTransactionId` и `bcStickyProxyAddress` равны `None`; `YtDynamicTableWriter` создаётся с `parentTransaction = None`.

Таким образом, существующие потоковые задания, не устанавливающие параметр `spark.yt.streaming.transactional=true`, продолжают работать без изменений и без каких-либо накладных расходов, связанных с транзакционным режимом.

Для включения транзакционного режима дополнительно требуется конфигурация `spark.ytsaurus.rpc.job.proxy.enabled=false`, переключающая исполнители на общий пул rpc-прокси кластера. Без этой настройки исполнители используют локальные job-proxy, изолированные от прокси драйвера, и операция `attachTransaction` завершится ошибкой «No such transaction» (см. §2.4.2).

\newpage
