# ПРИЛОЖЕНИЕ А

(Обязательное)

Листинги исходного кода ключевых компонентов реализации

В настоящем приложении приведён исходный код классов и методов адаптера SPYT, разработанных или существенно изменённых в рамках настоящей работы. Идентификаторы Scala- и Java-классов, методов и трейтов сохранены в исходной форме. Импорты и пакетные объявления опущены везде, где это не существенно для понимания логики; функционально значимые участки приведены в полном объёме.

Классы, которые не претерпели изменений в рамках настоящей работы (например, утилитные `YtQueueRange`, `YtQueueRDD`, агент байткода `SparkPatchAgent` и сопутствующие аннотации `@Decorate` и `@PatchSource`), в приложении не дублируются — в основном тексте на них даются прямые ссылки на репозиторий SPYT.

**Листинг А.1 — Трейт `StreamingTransactionSupport` и трейт `StreamingTransactionHandle` (модуль `spark-adapter-api`)**

```scala
trait StreamingTransactionHandle {
  def getId: String

  def getStickyProxyAddress: Option[String] = None

  def commit(): Unit

  def abort(): Unit
}

trait StreamingTransactionSupport {
  def isTransactionalStreamingEnabled(sparkSession: SparkSession): Boolean

  def createTransaction(sparkSession: SparkSession): StreamingTransactionHandle

  def setTransaction(handle: StreamingTransactionHandle): Unit = {
    setTransactionId(handle.getId)
    setStickyProxyAddress(handle.getStickyProxyAddress)
  }

  def setTransactionId(txId: String): Unit

  def setStickyProxyAddress(address: Option[String]): Unit = {}

  def clearTransactionId(): Unit

  def isRecoveryNeeded: Boolean = false

  def markRecoveryNeeded(): Unit = {}

  def clearRecoveryNeeded(): Unit = {}
}

object StreamingTransactionSupport {
  lazy val instance: StreamingTransactionSupport =
    ServiceLoader.load(classOf[StreamingTransactionSupport]).findFirst().get()
}
```

**Листинг А.2 — Реализация `YTsaurusStreamingTransactionSupport` и `YtStreamingTransactionHandle` (модуль `data-source-extended`)**

```scala
class YTsaurusStreamingTransactionSupport extends StreamingTransactionSupport {

  override def isTransactionalStreamingEnabled(sparkSession: SparkSession): Boolean =
    sparkSession.ytConf(Streaming.Transactional)

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

  override def setTransaction(handle: StreamingTransactionHandle): Unit =
    YtStreamingTransactionContext.setContext(
      StreamingTxContext(handle.getId, handle.getStickyProxyAddress))

  override def setTransactionId(txId: String): Unit =
    YtStreamingTransactionContext.setTransactionId(txId)

  override def setStickyProxyAddress(address: Option[String]): Unit =
    YtStreamingTransactionContext.setStickyProxyAddress(address)

  override def clearTransactionId(): Unit =
    YtStreamingTransactionContext.clearTransactionId()

  override def isRecoveryNeeded: Boolean = YtStreamingTransactionContext.isRecoveryNeeded
  override def markRecoveryNeeded(): Unit = YtStreamingTransactionContext.markRecoveryNeeded()
  override def clearRecoveryNeeded(): Unit = YtStreamingTransactionContext.clearRecoveryNeeded()
}

class YtStreamingTransactionHandle(transaction: ApiServiceTransaction)
    extends StreamingTransactionHandle {
  private val log = LoggerFactory.getLogger(getClass)

  override def getId: String = transaction.getId.toString

  override def getStickyProxyAddress: Option[String] = {
    try {
      val classes = Iterator.iterate[Class[_]](transaction.getClass)(c => c.getSuperclass)
        .takeWhile(_ != null)
      val mOpt = classes.flatMap(_.getDeclaredMethods.iterator)
        .find(m => m.getName == "getRpcProxyAddress" && m.getParameterCount == 0)
      mOpt.flatMap { m =>
        m.setAccessible(true)
        Option(m.invoke(transaction)).map(_.toString)
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  override def commit(): Unit = transaction.commit().join()

  override def abort(): Unit = {
    try {
      transaction.abort().join()
    } catch {
      case e: IllegalStateException =>
        log.warn("Transaction abort() failed with IllegalStateException: {}", e.getMessage)
    }
  }
}
```

**Листинг А.3 — `YtStreamingTransactionContext` (потокозависимое хранилище контекста)**

```scala
case class StreamingTxContext(txId: String, stickyAddress: Option[String])

object YtStreamingTransactionContext {
  private val currentTransactionContext: InheritableThreadLocal[Option[StreamingTxContext]] =
    new InheritableThreadLocal[Option[StreamingTxContext]]() {
      override def initialValue(): Option[StreamingTxContext] = None
    }

  private val recoveryNeededFlag: AtomicBoolean = new AtomicBoolean(false)

  def isRecoveryNeeded: Boolean = recoveryNeededFlag.get()
  def markRecoveryNeeded(): Unit = recoveryNeededFlag.set(true)
  def clearRecoveryNeeded(): Unit = recoveryNeededFlag.set(false)

  def get: Option[StreamingTxContext] = currentTransactionContext.get()

  def currentTransactionId: Option[String] =
    currentTransactionContext.get().map(_.txId)

  def currentStickyProxyAddress: Option[String] =
    currentTransactionContext.get().flatMap(_.stickyAddress)

  def setContext(ctx: StreamingTxContext): Unit =
    currentTransactionContext.set(Some(ctx))

  def setTransactionId(txId: String): Unit = {
    val sticky = currentTransactionContext.get().flatMap(_.stickyAddress)
    currentTransactionContext.set(Some(StreamingTxContext(txId, sticky)))
  }

  def setStickyProxyAddress(address: Option[String]): Unit = {
    currentTransactionContext.get() match {
      case Some(ctx) => currentTransactionContext.set(Some(ctx.copy(stickyAddress = address)))
      case None =>
    }
  }

  def clearTransactionId(): Unit = currentTransactionContext.remove()
}
```

**Листинг А.4 — Декоратор `MicroBatchExecutionDecorators` (модуль `spark-adapter/impl/spark-3.5.0`)**

```scala
@Decorate
@OriginClass("org.apache.spark.sql.execution.streaming.MicroBatchExecution")
@Applicability(from = "3.5.0")
class MicroBatchExecutionDecorators extends Logging {

  var availableOffsets: StreamProgress = ???
  var commitLog: CommitLog = ???
  var currentBatchId: Long = ???

  @DecoratedMethod
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    val sts = StreamingTransactionSupport.instance
    val transactionalStreamingEnabled = sts.isTransactionalStreamingEnabled(sparkSessionToRunBatch)

    if (!transactionalStreamingEnabled) {
      __runBatch(sparkSessionToRunBatch)
      return
    }

    if (sts.isRecoveryNeeded) {
      logWarning("Transactional streaming entering batch with recoveryNeeded flag set. " +
        "Previous batch failed and the YTsaurus consumer state may diverge from the Spark commit log.")
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
        try { currentTransaction.abort() } catch { case NonFatal(_) => () }
        if (commitLogWritten) {
          MicroBatchExecutionDecorators.deleteCommitLogEntry(commitLog, batchIdAtEntry)
        }
        throw e
    } finally {
      sts.clearTransactionId()
    }
  }

  private def __runBatch(sparkSessionToRunBatch: SparkSession): Unit = ???
}

object MicroBatchExecutionDecorators extends Logging {
  def commitOffsets(availableOffsets: StreamProgress): Unit = {
    for ((src, offset) <- availableOffsets.toList) {
      src match {
        case source: Source => source.commit(offset.asInstanceOf[Offset])
        case _ =>
      }
    }
  }

  def deleteCommitLogEntry(commitLog: CommitLog, batchId: Long): Unit = {
    try {
      commitLog.purgeAfter(batchId - 1)
      logWarning(s"Rolled back commit log entry $batchId because YT transaction commit failed")
    } catch {
      case t: Throwable =>
        logWarning(
          s"Failed to roll back commit log entry $batchId: ${t.getClass.getName}: ${t.getMessage}", t)
    }
  }
}
```

**Листинг А.5 — `YtStreamingSource` (источник потока, модуль `data-source-extended`)**

```scala
class YtStreamingSource(sqlContext: SQLContext, consumerPath: String, queuePath: String,
                        val schema: StructType, parameters: Map[String, String],
                        offsetProvider: YtQueueOffsetProvider = YtQueueOffsetProvider)
                       (implicit val yt: CompoundClient)
  extends Source with Logging with SupportsAdmissionControl {

  protected[streaming] lazy val cluster: String = YtWrapper.clusterName()
  private val includeServiceColumns =
    parameters.get("include_service_columns").exists(_.toBoolean)

  private var lastCommittedOffset: YtQueueOffset = getLastCommittedOffset
  private var maxOffset: Option[YtQueueOffset] = None

  private def transactionalEnabled: Boolean =
    Try(sqlContext.sparkSession.ytConf(Streaming.Transactional)).getOrElse(false)

  protected[streaming] def getMaxOffset: Option[YtQueueOffset] = {
    offsetProvider.getMaxOffset(cluster, queuePath) match {
      case Success(newMaxOffset) =>
        if (!(newMaxOffset >= lastCommittedOffset)) return None
        else {
          if (maxOffset.isDefined && !(newMaxOffset >= maxOffset.get)) {
            logWarning(s"New upper index < old upper index: $newMaxOffset < ${maxOffset.get}.")
          } else {
            maxOffset = Some(newMaxOffset)
          }
        }
      case Failure(exception) =>
        logWarning(s"Failed to get new max offset: ${exception.getMessage}", exception)
    }
    maxOffset
  }

  protected[streaming] def getLastCommittedOffset: YtQueueOffset = {
    lastCommittedOffset = offsetProvider.getCurrentOffset(cluster, consumerPath, queuePath)
    lastCommittedOffset
  }

  override def getDefaultReadLimit: ReadLimit = parameters.get("max_rows_per_partition") match {
    case Some(maxRows) => ReadLimit.compositeLimit(Array(ReadLimit.maxRows(maxRows.toLong)))
    case None => ReadLimit.allAvailable()
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    val startOffsetParsed = if (transactionalEnabled) {
      getLastCommittedOffset
    } else {
      val cached: Option[YtQueueOffset] = Option(startOffset).map(YtQueueOffset.apply)
      if (cached.isEmpty) getLastCommittedOffset
      else if (cached.get >= lastCommittedOffset) cached.get
      else lastCommittedOffset
    }

    val newMaxOffsetOpt = getMaxOffset
    if (newMaxOffsetOpt.isEmpty) return null
    val maxOffset = newMaxOffsetOpt.get

    val maxRows = limit match {
      case c: CompositeReadLimit => c.getReadLimits.collectFirst { case r: ReadMaxRows => r.maxRows() }
      case r: ReadMaxRows => Some(r.maxRows())
      case _ => None
    }
    if (maxRows.isEmpty) return maxOffset

    val partitions = maxOffset.partitions.map { case (i, upper) =>
      val start = startOffsetParsed.partitions.getOrElse(i, -1L)
      i -> math.min(start + maxRows.get, upper)
    }
    YtQueueOffset(cluster, queuePath, partitions)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val rdd = if (start.isDefined && start.get == end) {
      sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty")
    } else {
      val preparedStart = if (transactionalEnabled) lastCommittedOffset
        else start.map(YtQueueOffset.apply).filter(_ >= lastCommittedOffset).getOrElse(lastCommittedOffset)
      val preparedEnd = YtQueueOffset(end)

      if (!(preparedEnd >= preparedStart)) {
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty")
      } else {
        val ranges = YtQueueOffset.getRanges(preparedStart, preparedEnd)
        new YtQueueRDD(sqlContext.sparkContext, schema, consumerPath, queuePath,
                       ranges, includeServiceColumns).setName("yt")
      }
    }
    StreamingUtils.createStreamingDataFrame(sqlContext, rdd, schema)
  }

  override def commit(end: Offset): Unit = {
    val txContext = YtStreamingTransactionContext.get

    if (txContext.isEmpty) {
      val transactionalEnabled = sqlContext.sparkSession.ytConf(Streaming.Transactional)
      if (transactionalEnabled) return
      offsetProvider.advance(consumerPath, YtQueueOffset(end), lastCommittedOffset, maxOffset, None)(yt)
      return
    }

    val parentTransactionId = txContext.map(_.txId)
    val stickyAddr = txContext.flatMap(_.stickyAddress)
    val advanceClient: CompoundClient = stickyAddr match {
      case Some(addr) =>
        val conf = YtClientConfigurationConverter
          .ytClientConfiguration(sqlContext.sparkSession.sparkContext.getConf)
        YtClientProvider.ytClient(conf.copy(fixedProxyAddress = Some(addr)))
      case None => yt
    }

    offsetProvider.advance(consumerPath, YtQueueOffset(end), lastCommittedOffset, maxOffset,
      parentTransactionId)(advanceClient)
  }

  override def stop(): Unit = logDebug("Close YtStreamingSource")
}
```

**Листинг А.6 — `YtStreamingSink` (приёмник потока, модуль `data-source-extended`)**

```scala
class YtStreamingSink(sqlContext: SQLContext, queuePath: String,
                      parameters: Map[String, String]) extends Sink with Logging {
  @volatile private var latestBatchId = -1L

  override def toString: String = "YtStreamingSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val sparkContext = sqlContext.sparkSession.sparkContext

      val ytClientConfiguration =
        YtClientConfigurationConverter.ytClientConfiguration(sparkContext.getConf)
      val bcYtClientConfiguration = sparkContext.broadcast(ytClientConfiguration)

      val wConfig = SparkYtWriteConfiguration(sqlContext)
      val bcWriterConfig = sparkContext.broadcast(wConfig)

      val path = YPathEnriched.fromPath(new Path(queuePath))
      val bcPath = sparkContext.broadcast(path)
      val bcParameters = sparkContext.broadcast(parameters)
      val bcSchema = sparkContext.broadcast(data.schema)

      val txContext = YtStreamingTransactionContext.get
      val bcParentTransactionId = txContext.map(ctx => sparkContext.broadcast(ctx.txId))
      val bcStickyProxyAddress = txContext.flatMap(_.stickyAddress).map(sparkContext.broadcast)

      data.queryExecution.toRdd.foreachPartition { partitionIterator =>
        if (partitionIterator.hasNext) {
          val txId = bcParentTransactionId.map(_.value)
          val stickyAddr = bcStickyProxyAddress.map(_.value)
          implicit val partitionYtClient: CompoundClient = stickyAddr match {
            case Some(addr) =>
              YtClientProvider.ytClient(
                bcYtClientConfiguration.value.copy(fixedProxyAddress = Some(addr)))
            case None => YtClientProvider.ytClient(bcYtClientConfiguration.value)
          }
          val attachedTx = txId.map(id => YtWrapper.attachTransaction(id))
          val dynamicTableWriter = new YtDynamicTableWriter(
            bcPath.value, bcSchema.value, bcWriterConfig.value, bcParameters.value, attachedTx)
          try {
            partitionIterator.foreach { row => dynamicTableWriter.write(row) }
          } finally {
            dynamicTableWriter.close()
          }
        }
      }
      latestBatchId = batchId
    }
  }
}
```

**Листинг А.7 — `YtDynamicTableWriter` (приёмник записи в динамическую таблицу, модуль `data-source`)**

```scala
class YtDynamicTableWriter(richPath: YPathEnriched, schema: StructType,
                           wConfig: SparkYtWriteConfiguration, options: Map[String, String],
                           parentTransaction: Option[ApiServiceTransaction] = None)
                          (implicit ytClient: CompoundClient) extends OutputWriter {

  override val path: String = richPath.toStringPath
  private val writeSchemaConverter = WriteSchemaConverter(options)
  private val typeV3: Boolean = writeSchemaConverter.typeV3Format
  private val tableSchema: TableSchema =
    TableSchema.fromYTree(YtWrapper.attribute(path, "schema"))
  private val rowConverter: DynTableRowConverter =
    new DynTableRowConverter(schema, tableSchema, typeV3)
  private var count = 0
  private var modifyRowsRequestBuilder: ModifyRowsRequest.Builder = _

  initialize()

  def write(row: Seq[Any]): Unit = {
    val preparedRow = rowConverter.convertRow(row)
    modifyRowsRequestBuilder.addInsert(preparedRow.asJava)
    count += 1
    if (count == wConfig.dynBatchSize) commitBatch()
  }

  override def write(row: InternalRow): Unit = write(row.toSeq(schema))

  override def close(): Unit = {
    if (count > 0) commitBatch()
  }

  private def initBatch(): Unit = {
    modifyRowsRequestBuilder = ModifyRowsRequest.builder()
      .setPath(richPath.toStringYPath).setSchema(tableSchema)
    count = 0
  }

  private def commitBatch(): Unit = {
    YtMetricsRegister.time(writeBatchTime, writeBatchTimeSum) {
      val request = modifyRowsRequestBuilder.build()
      YtWrapper.insertRows(request, parentTransaction)
    }
    initBatch()
  }

  private def initialize(): Unit = {
    initBatch()
    YtMetricsRegister.register()
  }
}
```

**Листинг А.8 — `YtTransactionUtils`: создание, прикрепление и пинг транзакций (модуль `yt-wrapper`)**

```scala
trait YtTransactionUtils { self: LogLazy =>

  def createTransaction(timeout: Duration, sticky: Boolean)
                       (implicit yt: CompoundClient): ApiServiceTransaction =
    createTransaction(None, timeout, sticky)

  def createTransaction(parent: Option[String], timeout: Duration,
                        sticky: Boolean = false, title: Option[String] = None,
                        pingPeriod: Duration = Duration.ofSeconds(30))
                       (implicit yt: CompoundClient): ApiServiceTransaction = {
    val request = new StartTransaction(
        if (sticky) TransactionType.Tablet else TransactionType.Master).toBuilder
      .setTransactionTimeout(timeout)
      .setTimeout(timeout)
      .setPing(true)
      .setPingPeriod(pingPeriod)
    title.foreach(t => request.setAttributes(
      Collections.singletonMap("title", YTree.stringNode(t))))
    parent.foreach(p => request.setParentId(GUID.valueOf(p)))
    yt.startTransaction(request.build()).join()
  }

  def attachTransaction(transactionId: String, ping: Boolean = false,
                        pingPeriod: Option[Duration] = None)
                       (implicit yt: CompoundClient): ApiServiceTransaction = {
    val builder = AttachTransaction.builder()
      .setTransactionId(GUID.valueOf(transactionId))
      .setPing(ping)
    pingPeriod.foreach(builder.setPingPeriod)
    yt.attachTransaction(builder.build()).join()
  }

  def abortTransaction(guid: String)(implicit yt: CompoundClient): Unit =
    yt.abortTransaction(GUID.valueOf(guid)).join()

  def commitTransaction(guid: String)(implicit yt: CompoundClient): Unit =
    yt.commitTransaction(GUID.valueOf(guid)).join()

  def pingTransaction(tr: ApiServiceTransaction, interval: Duration)
                     (implicit yt: CompoundClient, ec: ExecutionContext): Cancellable[Unit] = {
    @tailrec
    def ping(cancel: Future[Unit], retry: Int): Boolean = {
      try {
        if (!cancel.isCompleted) { tr.ping().join(); true } else false
      } catch {
        case e: Throwable =>
          if (retry > 0) {
            Thread.sleep(new Random().nextInt(2000) + 100)
            ping(cancel, retry - 1)
          } else false
      }
    }
    cancellable { cancel =>
      var success = true
      while (!cancel.isCompleted && success) {
        success = ping(cancel, 3)
        Thread.sleep(interval.toMillis)
      }
    }
  }
}
```

**Листинг А.9 — Методы записи строк `YtDynTableUtils.insertRows` (модуль `yt-wrapper`)**

```scala
trait YtDynTableUtils { self: YtCypressUtils =>

  def insertRows(path: String, schema: TableSchema, rows: Seq[Seq[Any]],
                 parentTransaction: Option[ApiServiceTransaction] = None)
                (implicit yt: CompoundClient): Unit = {
    processModifyRowsRequest(
      ModifyRowsRequest.builder()
        .setPath(formatPath(path))
        .setSchema(schema)
        .addInserts(rows.map(_.asJava).asJava)
        .build(),
      parentTransaction)
  }

  def insertRows(modifyRowsRequest: ModifyRowsRequest,
                 parentTransaction: Option[ApiServiceTransaction])
                (implicit yt: CompoundClient): Unit =
    processModifyRowsRequest(modifyRowsRequest, parentTransaction)

  def insertRows(modifyRowsRequest: ModifyRowsRequest, parentTransactionId: String)
                (implicit yt: CompoundClient): Unit =
    yt.modifyRows(GUID.valueOf(parentTransactionId), modifyRowsRequest).join()

  private def processModifyRowsRequest(request: ModifyRowsRequest,
                                       transaction: Option[ApiServiceTransaction] = None)
                                      (implicit yt: CompoundClient): Unit = {
    val f: ApiServiceTransaction => Unit = _.modifyRows(request).get(1, TimeUnit.MINUTES)
    runWithDefinedTxOrRetry(f, transaction)
  }

  def runWithDefinedTxOrRetry[T](f: ApiServiceTransaction => T,
                                 tx: Option[ApiServiceTransaction] = None,
                                 attemptLimit: Int = 3)
                                (implicit yt: CompoundClient): T = tx match {
    case Some(tx) => f(tx)
    case None => runWithRetry(f, attemptLimit)
  }
}
```

**Листинг А.10 — `YtQueueUtils`: продвижение consumer-offset очереди (модуль `yt-wrapper`)**

```scala
trait YtQueueUtils { self: YtCypressUtils with YtTransactionUtils =>

  def pullConsumer(consumerPath: String, queuePath: String, partitionIndex: Int,
                   offset: Long, maxRowCount: Long)
                  (implicit yt: CompoundClient): QueueRowset = {
    val options = RowBatchReadOptions.builder()
      .setMaxRowCount(maxRowCount)
      .setMaxDataWeight(DataSize.fromTeraBytes(1)).build()
    val request = PullConsumer.builder()
      .setConsumerPath(YPath.simple(consumerPath))
      .setQueuePath(YPath.simple(queuePath))
      .setPartitionIndex(partitionIndex)
      .setOffset(offset)
      .setRowBatchReadOptions(options)
      .build()
    runWithRetry(() => yt.pullConsumer(request).get(30000, TimeUnit.MILLISECONDS))
  }

  def advanceConsumer(consumerPath: YPath, queuePath: YPath, partitionIndex: Int,
                      newOffset: Long, transaction: ApiServiceTransaction): Unit = {
    val request = AdvanceConsumer.builder()
      .setConsumerPath(consumerPath)
      .setQueuePath(queuePath)
      .setPartitionIndex(partitionIndex)
      .setNewOffset(newOffset)
      .build()
    transaction.advanceConsumer(request).join()
  }

  def advanceConsumer(consumerPath: YPath, queuePath: YPath, partitionIndex: Int,
                      newOffset: Long, parentTransactionId: String)
                     (implicit yt: CompoundClient): Unit = {
    val request = AdvanceConsumer.builder()
      .setConsumerPath(consumerPath)
      .setQueuePath(queuePath)
      .setPartitionIndex(partitionIndex)
      .setNewOffset(newOffset)
      .setTransactionId(GUID.valueOf(parentTransactionId))
      .build()
    yt.advanceConsumer(request).join()
  }

  def registerQueueConsumer(consumerPath: YPath, queuePath: YPath, vital: Boolean = true)
                           (implicit yt: CompoundClient): Unit = {
    val request = RegisterQueueConsumer.builder()
      .setConsumerPath(consumerPath)
      .setQueuePath(queuePath)
      .setVital(vital)
      .build()
    yt.registerQueueConsumer(request).join()
  }
}
```

**Листинг А.11 — Конфигурационная запись `SparkYtConfiguration.Streaming` (модуль `data-source`)**

```scala
object Streaming {
  case object Transactional extends ConfigEntry[Boolean]("streaming.transactional", Some(false))
}
```

\newpage
