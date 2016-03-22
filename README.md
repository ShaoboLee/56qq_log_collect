该工程包含了3个topology：
1、从kafka读取大json。在storm里面解析后再写回kafka，逻辑在com.wlqq.bigdata.log里面实现
2、从kafka读取各个业务的数据，解析后写往hdfs，逻辑在com.wlqq.bigdata.hdfs里面实现
3、监控storm写往hdfs的临时目录，如果存在没有load到hive的文件，发现后自动load到对应的hive表，逻辑实现ocm.wlqq.bigdata.monitor

往storm的提交方式
storm jar ./kafka-storm-kafka-1.0.0-jar-with-dependencies.jar com.wlqq.bigdata.xxx.yyyyTopology zzzz.yaml