package com.wlqq.bigdata.common;

import java.util.HashMap;
import java.util.Map;

import kafka.server.KafkaConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Utils {
	
	private static final Log logger = LogFactory.getLog(Utils.class);
	
	//storm
	public static final String ZKS = "storm.zk.connection";
	public static final String ZK_ROOT = "storm.zk.root";
	public static final String READER_PARALLELISM = "storm.spout.parallelism";
	public static final String LOADER_PARALLELISM_1 = "storm.bolt1.parallelism";
	public static final String LOADER_PARALLELISM_2 = "storm.bolt2.parallelism";
	public static final String WORKS_NUMBER = "storm.worker.numbers";
	public static final String TOPOLOGY_NAME = "storm.topology.name";
	public static final String NIMBUS_HOST = "storm.nimbus.host";
	public static final String STORM_DEBUG = "storm.debug";
	public static final String SPOUT_SEND_INTERVAL_SEC = "spout.send.interval";
	
	//kafka-storm
	public static final String TOPIC = "kafka-storm.consume.topic";
	public static final String KAFKA_GROUP_ID = "kafka-storm.group.id";
	public static final String START_OFFSET_TIME = "kafka.start.offset.time";
	public static final String Retry_Initial_Delay_Ms = "kafka-storm.retry.initial.delay.ms";
	public static final String Retry_Delay_Multiplier = "kafka-storm.retry.delay.multiplier";
	public static final String FAIL_RECORD_DEAL_CLASS = "kafka-storm.fail.record.deal.class";
	public static final String FAIL_RECORD_STORAGE = "kafka-storm.fail.record.storage.path";
	
	//kafka
	public static final String DEFAULT_UNKNOWN_TOPIC = "kafka.default.unknown.topic";//该topic用于存放未知topic对应的数据
	public static final String DEFAULT_RECEIVE_WRONG_DATA_TOPIC = "kafka.default.wrong.data.topic";//该topic用于存放数据格式错误的数据
	public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
	public static final String ACKS_CONFIG = "acks";
	public static final String RETRIES_CONFIG = "retries";
	public static final String BATCH_SIZE_CONFIG = "batch.size";
	public static final String LINGER_MS_CONFIG = "linger.ms";
	public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";
	public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
	public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
	public static final String IGNORE_ZK_OFFSETS = "ignoreZkOffsets";
	public static final String MAX_OFFSET_BEHIND = "maxOffsetBehind";
	public static final String UPDATE_TOPICS_INFO_INTERVAL_Ms = "update.topics.info.interval.ms";
	
	public static final String HDFS_URL = "hdfs.url";
	public static final String HDFS_BATCH_SIZE = "hdfs.batch.size";
	public static final String HDFS_WRITE_PATH = "hdfs.write.path";
	
	public static final String HIVE_FIELD_DELIMITER = "hive.field.delimiter";
	public static final String HIVE_PARTITION_HOUR_INTERVAL = "hive.partition.hour.interval";
	public static final String HIVE_PARTITION_DAY_NAME = "hive.partition.day.name";
	public static final String HIVE_PARTITION_DAY_FORMAT = "hive.partition.day.format";
	public static final String HIVE_PARTITION_HOUR_NAME = "hive.partition.hour.name";
	public static final String HIVE_DATA_PATH = "hive.data.path";
	public static final String HIVE_FILE_SIZE = "hive.file.size";
	public static final String HIVE_HOST = "hive.host";
	public static final String HIVE_PORT = "hive.port";
	public static final String HIVE_TABLE = "hive.table";
	public static final String HIVE_DATABASE = "hive.database";
	public static final String HIVE_FILE_FORMAT = "hive.file.format";
	public static final String HIVE_FIELD_DELETE_HEAD_UNDERLINE = "hive.field.delete.head.underline";
	public static final String HIVE_JSON_SERDE_JAR_PATH= "hive.json.serde.jar.path";
	public static final String HIVE_FILE_COMPRESS_CLASS = "hive.file.compress.class";
	public static final String HIVE_FILE_SUFFIX_NAME = "hive.file.suffix.name";
	public static final String HIVE_DB_PATH = "hive.db.path";
	public static final String HIVE_LOAD_DELAY_MINUTE = "hive.load.delay.minute";
	
	
	//ES
	public static final String SUCCESS_STREAM = "success";
	public static final String RAWDATA_FORMAT_ERROR_STREAM = "raw-data-format-error";
	public static final String ES_RESULT_ERROR_STREAM = "es-result-error";
	public static final String ES_EXECUTE_EXCEPTION_STREAM = "es-execute-exception";
	public static final String FAILED_RECOVERABLE_STREAM = "failed-recoverable";
	public static final String FAILED_UNEXPECTED_STREAM = "failed-unexpected";
	public static final String QUEUE_TIMEOUT_STREAM = "queue-timeout";
	public static final String FAILED_REJECT_STREAM = "es-rejected-error";
	public static final String FAILED_MAPPING_STREAM = "es-mapping-error";
	public static final String DISTRIBUTION_STREAM = "data-distribution";
	public static final String DISTRIBUTION_PARALLELISM = "storm.bolt.distribution.parallelism";
	public static final String LOADER_PARALLELISM = "storm.bolt.loader.parallelism";
	public static final String TOLERANT_PARALLELISM = "storm.bolt.tolerant.parallelism";
	public static final String OUTPUT_STREAM_FIELDS_NAME = "res";
	
	public static final String METRICS_CONSUMER = "user.metric.consumer";
	public static final String METRICS_PARALLELISM = "user.bolt.metrics.parallelism";
	
	public static final String ES_NODES = "user.es.nodes";
	public static final String ES_INDEX = "user.es.index";
	public static final String ES_TYPE = "user.es.type";
	
	public static final String THREAD_QUEUE_TIMEOUT = "user.bolt.queue.wait.timeout";
	public static final String THREAD_QUEUE_SIZE = "user.bolt.queue.size";
	
	
	public static String getValue(Map<String, Object> yamlConf, String configName, String defaultValue) {

		return yamlConf.get(configName) == null ? defaultValue : yamlConf.get(configName).toString();
	}

	public static int getValue(Map<String, Object> yamlConf, String conf, int defaultValue) {

		return yamlConf.get(conf) == null ? defaultValue : Integer.parseInt(yamlConf.get(conf).toString());
	}
	
	public static long getValue(Map<String, Object> yamlConf, String conf, long defaultValue) {

		//return yamlConf.get(conf) == null ? defaultValue : (Long) yamlConf.get(conf);
		return yamlConf.get(conf) == null ? defaultValue : Long.parseLong(yamlConf.get(conf).toString());
	}
	
	public static double getValue(Map<String, Object> yamlConf, String conf, double defaultValue) {

		//return yamlConf.get(conf) == null ? defaultValue : (Long) yamlConf.get(conf);
		return yamlConf.get(conf) == null ? defaultValue : Double.parseDouble(yamlConf.get(conf).toString());
	}
	
	public static float getValue(Map<String, Object> yamlConf, String conf, float defaultValue) {

		//return yamlConf.get(conf) == null ? defaultValue : (Long) yamlConf.get(conf);
		return yamlConf.get(conf) == null ? defaultValue : Float.parseFloat(yamlConf.get(conf).toString());
	}
	
	public static boolean getValue(Map<String, Object> yamlConf, String conf, boolean defaultValue) {

		//return yamlConf.get(conf) == null ? defaultValue : (Long) yamlConf.get(conf);
		return yamlConf.get(conf) == null ? defaultValue : "true".equalsIgnoreCase(yamlConf.get(conf).toString());
	}
	
	public static void main(String[] args){
		Map<String, Object> yamlConf = new HashMap<String, Object>();
		//yamlConf.put(Utils.START_OFFSET_TIME, "-2");
		System.out.println(Utils.getValue(yamlConf, Utils.START_OFFSET_TIME, -1L));
	}
	
	
}
