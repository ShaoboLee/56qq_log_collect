package com.wlqq.bigdata.es.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public interface ESLoader {
	/**
	 * 
	 * @param configure
	 *            初始化
	 * @throws IOException
	 */
	void init(Map configure) throws IOException;

	/**
	 * 批量写入docment到elasticsearch
	 * 
	 * @param docs
	 * @return
	 */
	List<Result> bulkLoadDocuments(List<Tuple> tuples);

	void close();
}
