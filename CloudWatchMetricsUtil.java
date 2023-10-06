package com.amazonaws.awslabs.utils;


import org.apache.log4j.Logger;

import scala.io.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

import java.io.IOException;
//import java.util.ArrayList;
import java.util.*;
//import java.util.HashMap;
//import java.util.List;

public class CloudWatchMetricsUtil {
  private AmazonCloudWatch cw;
  private String appName;
  private String jobFlowId;
  private Logger log;

  public CloudWatchMetricsUtil(String appName) {
    this.cw = AmazonCloudWatchClientBuilder.defaultClient();
    this.appName = appName;
    this.jobFlowId = getJobFlowId();
    this.log = Logger.getLogger(getClass().getName());
  }

  private String jobFlowInfoFile = "/mnt/var/lib/info/job-flow.json";
  private String strCodec = "UTF-8";

  private String getJobFlowId() {
    try {
      System.out.println("jobFlowInfoFile: " + jobFlowInfoFile);
      Map<String, Object> jsonMap = parseJsonWithJackson(Source.fromFile(jobFlowInfoFile, strCodec));
      return jsonMap.get("jobFlowId").toString();
    } catch (Exception e) {
      log.error("Exception caught while getting jobFlowId: " + e.getMessage());
      System.out.println("Exception caught while getting jobFlowId: " + e.getMessage());
      return "unknownJobFlowId";
    }
  }

    private Map<String, Object> parseJsonWithJackson(BufferedSource json) throws IOException 
    {
        ObjectMapper attrMapper = new ObjectMapper().registerModule(new DefaultScalaModule());
        return attrMapper.readValue(json.reader(), new TypeReference<Map<String,Object>>(){});
    }

    public void sendHeartBeat(Map<String, String> dimentionItems) {
        sendHeartBeat(dimentionItems, "heartBeat");
    }

    public void sendHeartBeat(Map<String,String> dimentionItems, String metricName) {
	    pushMetric(dimentionItems,"heartBeat",1.0, StandardUnit.Count);
    }

    private void pushMetric(Map<String, String> dimentionItems, String metricName, double value,
            StandardUnit unit) {
              System.out.println("pushMetric: " + metricName);
        ArrayList<Dimension> dimentions = new ArrayList<Dimension>();

        for (Map.Entry<String, String> entry : dimentionItems.entrySet()) {
            Dimension dimension = new Dimension()
                    .withName(entry.getKey())
                    .withValue(entry.getValue());
            dimentions.add(dimension);
        }

        Dimension dimensionAppName = new Dimension()
                .withName("ApplicationName")
                .withValue(appName);

        dimentions.add(dimensionAppName);

        Dimension dimentionsJobFlowId = new Dimension()
                .withName("JobFlowId")
                .withValue(jobFlowId);

        dimentions.add(dimentionsJobFlowId);

        MetricDatum datum = new MetricDatum()
					.withMetricName(metricName)
					.withUnit(unit)
					.withValue(value)
					.withDimensions(dimentions);

		    PutMetricDataRequest request = new PutMetricDataRequest()
					.withNamespace("AWS/ElasticMapReduce")
					.withMetricData(datum);

        PutMetricDataResult response = cw.putMetricData(request);
        if (response.getSdkHttpMetadata().getHttpStatusCode() != 200) {
			    log.warn("Failed pushing CloudWatch Metric with RequestId: " + response.getSdkResponseMetadata().getRequestId());
			    log.debug("Response Status code: " + response.getSdkHttpMetadata().getHttpStatusCode());
		    }

    }

    public void pushCountMetric(Map<String, String> dimentionItems, String metricName, Double value) {
		pushMetric(dimentionItems, metricName, value.doubleValue(), StandardUnit.Count);
	}
	
	public void pushMillisecondsMetric(Map<String, String> dimentionItems, String metricName, Long value) {
		pushMetric(dimentionItems, metricName, value.longValue(), StandardUnit.Milliseconds);
	}
	
	public void pushSecondsMetric(Map<String, String> dimentionItems, String metricName, Long value) {
		pushMetric(dimentionItems, metricName, value.longValue(), StandardUnit.Seconds);
	}
	
	public void pushCountSecondMetric(Map<String, String> dimentionItems, String metricName, Double value) {
		pushMetric(dimentionItems, metricName, value.doubleValue(), StandardUnit.CountSecond);
	}
}