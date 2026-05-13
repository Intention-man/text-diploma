# ПРИЛОЖЕНИЕ А

(Обязательное)

Листинги исходного кода ключевых компонентов реализации

В настоящем приложении приведён исходный код наиболее объёмных компонентов реализации, на которые ссылается основной текст главы 4. Идентификаторы Scala-классов, методов и трейтов сохранены в исходной форме.

**Листинг А.1 — Реализация декоратора метода `runBatch` класса `MicroBatchExecution`**

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

**Листинг А.2 — Реализация метода `YtStreamingSource.commit` в транзакционном и нетранзакционном режимах**

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

**Листинг А.3 — Реализация метода `createTransaction` в `YTsaurusStreamingTransactionSupport`**

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

\newpage
