Program to demostrate StreamingFileSink problem with ADLS
```
mvn clean package
```

to build fat-jar




3 different runs, the one with abfs shows exception in flink console

```
flink run -yD security.kerberos.login.keytab=.....keytab -yD security.kerberos.login.principal=....  --detached  flink_adls_problem-1.0-SNAPSHOT.jar  hdfs:///tmp                                           

hadoop fs -ls /tmp/data/2021-02-02--09
```


```
flink run -yD security.kerberos.login.keytab=.....keytab -yD security.kerberos.login.principal=....  --detached  flink_adls_problem-1.0-SNAPSHOT.jar  /tmp/data
shows files on worker nodes

ls -l /tmp/data/2021-02-02--09/  
```


```
flink run -yD security.kerberos.login.keytab=.....keytab -yD security.kerberos.login.principal=....  --detached  flink_adls_problem-1.0-SNAPSHOT.jar  abfs://examples-fs@cdpdlmain.dfs.core.windows.net/tmp 

java.lang.UnsupportedOperationException: Recoverable writers on Hadoop are only supported for HDFS and for Hadoop version 2.7 or newer
    at org.apache.flink.runtime.fs.hdfs.HadoopRecoverableWriter.<init>(HadoopRecoverableWriter.java:61)
    at org.apache.flink.runtime.fs.hdfs.HadoopFileSystem.createRecoverableWriter(HadoopFileSystem.java:202)
    at org.apache.flink.core.fs.SafetyNetWrapperFileSystem.createRecoverableWriter(SafetyNetWrapperFileSystem.java:69)
    at org.apache.flink.streaming.api.functions.sink.filesystem.Buckets.<init>(Buckets.java:117)
    at org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink$BulkFormatBuilder.createBuckets(StreamingFileSink.java:409)
    at org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.initializeState(StreamingFileSink.java:443)
    at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.tryRestoreFunction(StreamingFunctionUtils.java:178)
    at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.restoreFunctionState(StreamingFunctionUtils.java:160)
    at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.initializeState(AbstractUdfStreamOperator.java:96)
    at org.apache.flink.streaming.api.operators.AbstractStreamOperator.initializeState(AbstractStreamOperator.java:284)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.initializeStateAndOpen(StreamTask.java:989)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$beforeInvoke$0(StreamTask.java:453)
    at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor.runThrowing(StreamTaskActionExecutor.java:94)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.beforeInvoke(StreamTask.java:448)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:460)
    at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:707)
    at org.apache.flink.runtime.taskmanager.Task.run(Task.java:532)
    at java.lang.Thread.run(Thread.java:748)
```
