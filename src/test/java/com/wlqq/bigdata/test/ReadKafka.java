package com.wlqq.bigdata.test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;

import com.wlqq.bigdata.utils.Utils;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import storm.kafka.BrokerHosts;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.FailedFetchException;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaError;
import storm.kafka.KafkaUtils;
import storm.kafka.Partition;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticCoordinator;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.TopicOffsetOutOfRangeException;
import storm.kafka.ZkCoordinator;
import storm.kafka.ZkHosts;
import storm.kafka.ZkState;

public class ReadKafka {
	
	public static final Logger LOG = LoggerFactory.getLogger(ReadKafka.class);
	
	public static ByteBufferMessageSet fetchMessages(KafkaConfig config, SimpleConsumer consumer, Partition partition, long offset)
            throws TopicOffsetOutOfRangeException, FailedFetchException,RuntimeException {
        ByteBufferMessageSet msgs = null;
        String topic = config.topic;
        int partitionId = partition.partition;
        FetchRequestBuilder builder = new FetchRequestBuilder();
        FetchRequest fetchRequest = builder.addFetch(topic, partitionId, offset, config.fetchSizeBytes).
                clientId(config.clientId).maxWait(config.fetchMaxWait).build();
        FetchResponse fetchResponse;
        try {
            fetchResponse = consumer.fetch(fetchRequest);
        } catch (Exception e) {
            if (e instanceof ConnectException ||
                    e instanceof SocketTimeoutException ||
                    e instanceof IOException ||
                    e instanceof UnresolvedAddressException
                    ) {
                LOG.warn("Network error when fetching messages:", e);
                throw new FailedFetchException(e);
            } else {
                throw new RuntimeException(e);
            }
        }
        if (fetchResponse.hasError()) {
            KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
            if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE) && config.useStartOffsetTimeIfOffsetOutOfRange) {
                String msg = "Got fetch request with offset out of range: [" + offset + "]";
                LOG.warn(msg);
                throw new TopicOffsetOutOfRangeException(msg);
            } else {
                String message = "Error fetching data from [" + partition + "] for topic [" + topic + "]: [" + error + "]";
                LOG.error(message);
                throw new FailedFetchException(message);
            }
        } else {
            msgs = fetchResponse.messageSet(topic, partitionId);
        }
        return msgs;
    }
	
	public static void main(String[] args){
		ByteBufferMessageSet msgs = null;
		
		Map<String, Object> userConfig = backtype.storm.utils.Utils.findAndReadConfigFile(args.length==0?"conf/storm-kafka.yaml":args[0], true);
		BrokerHosts brokerHosts = new ZkHosts(userConfig.get(Utils.ZKS).toString());
		String topicName = userConfig.get(Utils.TOPIC).toString();
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topicName, userConfig.get(Utils.ZK_ROOT).toString(),
				userConfig.get(Utils.KAFKA_GROUP_ID).toString());
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.ignoreZkOffsets=Utils.getValue(userConfig, Utils.IGNORE_ZK_OFFSETS,false);//是否从zookeeper读取offset
        
        //fail的记录重试时间间隔配置
        spoutConf.retryDelayMultiplier = Utils.getValue(userConfig, Utils.Retry_Delay_Multiplier,spoutConf.retryDelayMultiplier);
        spoutConf.retryInitialDelayMs = Utils.getValue(userConfig, Utils.Retry_Initial_Delay_Ms,spoutConf.retryInitialDelayMs);
        //fail的记录掉队多少的时候丢弃 
        spoutConf.maxOffsetBehind = Utils.getValue(userConfig, Utils.MAX_OFFSET_BEHIND,spoutConf.maxOffsetBehind);
        spoutConf.startOffsetTime = Utils.getValue(userConfig, Utils.START_OFFSET_TIME,kafka.api.OffsetRequest.EarliestTime());
		
        Config conf = new Config();
        conf.putAll(userConfig);
        
        DynamicPartitionConnections _connections;
        ZkState _state;
        SimpleConsumer _consumer;
        
        Map stateConf = new HashMap(conf);
        List<String> zkServers = spoutConf.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = spoutConf.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, spoutConf.zkRoot);
        _state = new ZkState(stateConf);

        _connections = new DynamicPartitionConnections(spoutConf, KafkaUtils.makeBrokerReader(conf, spoutConf));


//        _consumer = _connections.register(id.host, id.partition);
//
//        
//        try {
//            msgs = KafkaUtils.fetchMessages(spoutConf, _consumer, _partition, offset);
//        } catch (TopicOffsetOutOfRangeException e) {
//        	e.printStackTrace();
//        }
	}

}
