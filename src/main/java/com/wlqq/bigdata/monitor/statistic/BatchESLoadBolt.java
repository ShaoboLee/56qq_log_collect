package com.wlqq.bigdata.monitor.statistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.wlqq.bigdata.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class BatchESLoadBolt extends BaseRichBolt {
	
	private static final Log logger = LogFactory.getLog(BatchESLoadBolt.class);
	
	private OutputCollector collector;
	private Map<String, Object> userConfig;
	private HTTPClientESLoader loader = null;
	private Thread executor = null;
	private ArrayBlockingQueue<Tuple> tupleQueue;
	private boolean closeFlag = false;
	private boolean closeable = false;
	private int queueTimeout = -1;
	private int queueSize = -1;
	private int timeOutCount = 1;

	/**
	 * 初始化阻塞队列，启动一个子线程，当队列里面有数据时就往es发送
	 * @param conf
	 * @throws IOException
	 */
	private void init(Map conf) throws IOException{
		this.queueSize = Utils.getValue(conf, Utils.THREAD_QUEUE_SIZE, 10);
		this.queueTimeout = Utils.getValue(conf, Utils.THREAD_QUEUE_TIMEOUT, 60);
		loader = new HTTPClientESLoader();
		loader.init(conf);
		tupleQueue = new ArrayBlockingQueue<Tuple>(queueSize);
		final List<Tuple> tmpTuples = new ArrayList<Tuple>(tupleQueue.size());
		executor = new Thread(new Runnable() {
			public void run() {

				while (!closeFlag) {
					try {
						//判断当前队列存放数据的个数是否达到设定的阈值,没有达到就多等一会儿,要不然数据量越小,es写入消耗的时间就越少,
						//这段时间内写入队列的数据就可能更少了,从而形成恶性循环，适当的等下,可以提升一些es的写入率
						if(tupleQueue.size()<queueSize*Utils.getValue(userConfig, Utils.THREAD_QUEUE_PERCENT, 0.5)){
							Thread.sleep(Utils.getValue(userConfig, Utils.THREAD_QUEUE_SLEEP_MS, 100));
						}
						tmpTuples.clear();
						tupleQueue.drainTo(tmpTuples);
						List<Result> results = null;
						if (tmpTuples.isEmpty()) {
							try {
								Thread.sleep(10);
							} catch (InterruptedException e) {
								logger.info(e);
							}
						} else {
							logger.info("+++batch load documents:" + tmpTuples.size());
							long t1 = System.currentTimeMillis();
							results = loader.bulkLoadDocuments(tmpTuples);
							logger.info("+++++++batchloaddocuments cost time:" + (System.currentTimeMillis() - t1)
									+ " ms");
							// 如果不是异常导致的写es失败，例如缺少ID、格式错误等情况，再写ES也将继续失败，因此反馈spout成功，并将失败进行记录，待后续处理
							for (Result res : results) {
								switch (res.getStatus()) {
								case OK: {
									emitResult(Utils.SUCCESS_STREAM, res);
									collector.ack(res.getTuple());
									break;
								}
								case FAILED_RAWDATA_FORMAT_ERROR: {
									emitResult(Utils.RAWDATA_FORMAT_ERROR_STREAM, res);
									collector.ack(res.getTuple());
									break;
								}
								case FAILED_BY_ES_RESULT: {
									emitResult(Utils.ES_RESULT_ERROR_STREAM, res);
									collector.fail(res.getTuple());
									break;
								}
								case FAILED_BY_ES_EXECUTE_EXCEPTION: {
									emitResult(Utils.ES_EXECUTE_EXCEPTION_STREAM, res);
									collector.fail(res.getTuple());
									break;
								}
								case FAILED_RECOVERABLE: {
									emitResult(Utils.FAILED_RECOVERABLE_STREAM, res);
									collector.fail(res.getTuple());
									break;
								}
								case FAILED_REJECT_ERROR: {
									emitResult(Utils.FAILED_REJECT_STREAM, res);
									collector.fail(res.getTuple());
									break;
								}
								case FAILED_MAPPING_ERROR: {
									emitResult(Utils.FAILED_MAPPING_STREAM, res);
									collector.ack(res.getTuple());
									break;
								}
								default: {
									emitResult(Utils.FAILED_UNEXPECTED_STREAM, res);
									collector.ack(res.getTuple());
								}
								}
							}
						}
					} catch (Exception e) {
						logger.error(e);
					}

				}
				closeable = true;
			}
		});
		executor.start();
		
	}
	
	public BatchESLoadBolt(Map<String, Object> userConfig){
		this.userConfig = userConfig;
	}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		//p = new KafkaProduce(userConfig); 
		try {
			init(userConfig);
		} catch (IOException e) {
			logger.error("Init elasticsearch failed.", e);
		}
	}

	public void execute(Tuple input) {
         
         try {
				boolean success = this.tupleQueue.offer(input, queueTimeout, TimeUnit.SECONDS);
				if (!success) {
					logger.info("+++ enqueue time out." + timeOutCount++);
					Result res = new Result(Result.STATUS.FAILED_RECOVERABLE, "enqueue timeout.", input.getString(0));
					res.setTuple(input);
					emitResult(Utils.QUEUE_TIMEOUT_STREAM,res);
					collector.fail(input);
				}

			} catch (InterruptedException e) {
				logger.info("+++interupted when put tuple in queue:" + e);
				Result res = new Result(Result.STATUS.FAILED_RECOVERABLE, "enqueue timeout.", input.getString(0));
				res.setTuple(input);
				emitResult(Utils.QUEUE_TIMEOUT_STREAM,res);
				collector.fail(input);
			}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declare) {

		declare.declareStream(Utils.SUCCESS_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.RAWDATA_FORMAT_ERROR_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.ES_RESULT_ERROR_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.ES_EXECUTE_EXCEPTION_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.FAILED_RECOVERABLE_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.FAILED_UNEXPECTED_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.QUEUE_TIMEOUT_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.FAILED_REJECT_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
		declare.declareStream(Utils.FAILED_MAPPING_STREAM, new Fields(Utils.OUTPUT_STREAM_FIELDS_NAME));
	}
	
	private void emitResult(String stream, Result res) {
		List<Object> outputTuple = new ArrayList<Object>(1);
		String rawDoc = res.getTuple().getString(0);
		Result r = res.asNoTupleResult();
		r.setRawJSONDocument(rawDoc);
		outputTuple.add(r);
		collector.emit(stream, outputTuple);
	};
	
	public void cleanup() {
		closeFlag = true;
		while (!closeable) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.info(e);
			}
		}

		this.loader.close();
	}

}
