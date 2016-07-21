package com.wlqq.bigdata.monitor.statistic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wlqq.bigdata.utils.Utils;

import backtype.storm.tuple.Tuple;

public class HTTPClientESLoader implements ESLoader {
	private static final Logger logger = Logger.getLogger(HTTPClientESLoader.class);
	private HttpClient httpclient = null;

	private String indexHead = null;
	private String[] nodes = null;
	
	// ����StringBuffer�������Ż���ȷ��StringBuffer�ڵ�һ�η����ڴ�ʱ��������Ҫ���ֽ�����𲻴�
	private int bulkSize = -1;
	private int sizePerDocument = 500;
	
	// RR��ʽ����es�ķ�������ͨ����ֵȷ��RR�Ĺ�ƽ��
	private int lastWorkLoadNodeIndex = 1;
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private String type;

	// index������ȷʱ��״̬��
	private Set<Integer> STATUS_OK_SET = new HashSet<Integer>();

	{
		STATUS_OK_SET.add(200);
		STATUS_OK_SET.add(201);
	}
	
	// index����ʧ��ʱ,��Ҫ���Ե�ʧ����
	private Set<Integer> STATUS_RETRY_SET = new HashSet<Integer>();

	{
		STATUS_OK_SET.add(429);
		STATUS_OK_SET.add(503);
	}
	
	public void init(Map configure) throws IOException {
		logger.debug("+++ enter es loader init() method.");
		this.bulkSize = Utils.getValue(configure, Utils.THREAD_QUEUE_SIZE, 10);
		nodes = configure.get(Utils.ES_NODES).toString().split(",");
		type = configure.get(Utils.ES_TYPE).toString();
		indexHead = configure.get(Utils.ES_INDEX).toString();
		lastWorkLoadNodeIndex = 1;
		PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
		cm.setMaxTotal(nodes.length);
		cm.setDefaultMaxPerRoute(1);
		httpclient = new DefaultHttpClient(cm);
	}

	private String getServer() {
		int nextNodeIndex = lastWorkLoadNodeIndex;
		lastWorkLoadNodeIndex++;
		return nodes[nextNodeIndex % nodes.length];
	}

	public List<Result> bulkLoadDocuments(List<Tuple> tuples) {
		
		String index = indexHead+"-"+df.format(new Date());
		StringBuffer sb = new StringBuffer(bulkSize * sizePerDocument);
		List<Result> results = new ArrayList<Result>(tuples.size());
		List<Tuple> rigthFormateTuples = new ArrayList<Tuple>(tuples.size());
		for (Tuple tuple : tuples) {
			String esDoc = tuple.getString(0);
			Object id = null;
			Result res = null;
			try {
				JSONObject rjo = JSONObject.parseObject(esDoc);
				id = rjo.get("id");
				if (esDoc == null || esDoc.length() == 0) {
					res = new Result(Result.STATUS.FAILED_RAWDATA_FORMAT_ERROR, "JSON is empty.", esDoc);
					res.setTuple(tuple);
					results.add(res);
					continue;
				}
				if (id == null || id.toString().length() == 0) {
					res = new Result(Result.STATUS.FAILED_RAWDATA_FORMAT_ERROR, "ID is empty.", esDoc);
					res.setTuple(tuple);
					results.add(res);
					continue;
				}
			} catch (Exception e) {
				res = new Result(Result.STATUS.FAILED_RAWDATA_FORMAT_ERROR, e, "Parse raw doc exception.", esDoc);
				res.setTuple(tuple);
				results.add(res);
				continue;
			}
			
			sb.append("{\"index\":{\"_id\":\"").append(id).append("\"}}\n").append(esDoc).append("\n");
			rigthFormateTuples.add(tuple);
		}
		
		if (sb.length() == 0) {
			return results;
		}
		
		HttpPost httppost = new HttpPost("http://" + getServer() + "/" + index + "/" + type + "/_bulk");
		Exception ex = null;
		try {
			HttpEntity entity = new StringEntity(sb.toString(), "UTF-8");
			httppost.setEntity(entity);
			logger.info("++++++start execute:");
			long start = System.currentTimeMillis();
			HttpResponse response = httpclient.execute(httppost);
			logger.info("++++++end execute:" + (System.currentTimeMillis() - start));

			entity = response.getEntity();
			String esResult = convertStreamToString(entity.getContent());
			httppost.abort();
			JSONObject jo = JSONObject.parseObject(esResult);

			Boolean errors = jo.getBoolean("errors");
			if (errors != null && !errors) {//correct
				JSONArray items = jo.getJSONArray("items");
				Result res = null;
				if (items != null && items.size() == rigthFormateTuples.size()) {
					for (int i = 0; i < items.size(); i++) {
						res = new Result(Result.STATUS.OK);
						res.setTuple(rigthFormateTuples.get(i));
						results.add(res);
					}
				} else {
					fillResults(results, rigthFormateTuples, Result.STATUS.FAILED_BY_ES_RESULT,
							"+++Result size dose not match bulk load documents size.Result doc:" + esResult, null);
				}
			} else {//�ҳ����ؽ����ʧ�ܵļ�¼
				
				analyzeErrorResponse(jo,results, rigthFormateTuples, esResult);
			}
		} catch (UnsupportedEncodingException e) {
			ex = e;
		} catch (ClientProtocolException e) {
			ex = e;
		} catch (IllegalStateException e) {
			ex = e;
		} catch (IOException e) {
			ex = e;
		} catch (Exception e) {
			ex = e;
		}
		if (ex != null) {
			logger.info(ex);
			fillResults(results, rigthFormateTuples, Result.STATUS.FAILED_BY_ES_EXECUTE_EXCEPTION,
					"+++load to es failed by exception.", ex);
		}
		
		return results;
	}
	
	private void analyzeErrorResponse(JSONObject response,List<Result> results, List<Tuple> rigthFormateTuples,String esResult){
		
		JSONArray items = response.getJSONArray("items");
		Result res = null;
		if (items != null && items.size() == rigthFormateTuples.size()) {
			for (int i = 0; i < items.size(); i++) {
				JSONObject indexResult = items.getJSONObject(i).getJSONObject("index");
				if (indexResult != null && STATUS_OK_SET.contains(indexResult.getInteger("status"))) {
					res = new Result(Result.STATUS.OK);
				} else if(indexResult != null && STATUS_RETRY_SET.contains(indexResult.getInteger("status"))){
					res = new Result(Result.STATUS.FAILED_REJECT_ERROR);
				}else{
					res = new Result(Result.STATUS.FAILED_MAPPING_ERROR, null,
							"mapping error" + indexResult, null);
				}
				res.setTuple(rigthFormateTuples.get(i));
				results.add(res);
			}
		} else {
			fillResults(results, rigthFormateTuples, Result.STATUS.FAILED_BY_ES_RESULT,
					"+++Result size dose not match bulk load documents size.Result doc:" + esResult, null);
		}
		
	}

	private void fillResults(List<Result> results, List<Tuple> rigthFormateTuples, Result.STATUS status,
			String failedReason, Exception e) {
		Result r = null;
		for (Tuple t : rigthFormateTuples) {
			r = new Result(status, e, failedReason, t.getString(0));
			r.setTuple(t);
			results.add(r);
		}
	}

	public void close() {
		this.httpclient.getConnectionManager().shutdown();
		logger.info("closed.");
	}

	public static String convertStreamToString(InputStream is) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();

		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

	public static void main(String[] args) throws IOException {
		
	}
}
