package com.wlqq.bigdata.es.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

public interface ESLoader {
	/**
	 * 
	 * @param configure
	 *            ��ʼ��
	 * @throws IOException
	 */
	void init(Map configure) throws IOException;

	/**
	 * ����д��docment��elasticsearch
	 * 
	 * @param docs
	 * @return
	 */
	List<Result> bulkLoadDocuments(List<Tuple> tuples);

	void close();
}
