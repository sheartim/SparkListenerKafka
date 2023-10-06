package com.amazonaws.awslabs.sparklistener.listener;

import org.apache.spark.streaming.scheduler.*;
import org.apache.log4j.Logger;
//import org.joda.time.DateTime;
//import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
//import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import java.util.Map;
import java.util.HashMap;

import com.amazonaws.awslabs.utils.CloudWatchMetricsUtil;

public class CloudWatchSparkListener implements StreamingListener {
  
  private static final Logger log = Logger.getLogger(CloudWatchSparkListener.class.getName());
  private CloudWatchMetricsUtil metricUtil;
  private Map<String,String> dimentionsMap;
  private String appName;
 
  /**
    * Constructor
    *
    * @param appName Application Name
    */
  public CloudWatchSparkListener(String appName) {
    this.appName = appName;
    this.metricUtil = new CloudWatchMetricsUtil(appName);
    this.dimentionsMap = new HashMap<String,String>();
  }

  /**
    * This method executes when a Spark Streaming batch completes.
    *
    * @param batchCompleted Class having information on the completed batch
    */
  @Override
  public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
    log.info("CloudWatch Streaming Listener, onBatchCompleted:" + appName);
    System.out.println("CloudWatch Streaming Listener, onBatchCompleted:" + appName);

    // write performance metrics to CloutWatch Metrics
    writeBatchStatsToCloudWatch(batchCompleted);
  }
  /**
  * This method executes when a Spark Streaming batch completes.
  *
  * @param receiverError Class having information on the reciever Errors
  */
  @Override
  public void onReceiverError(StreamingListenerReceiverError receiverError) { 
    log.warn("CloudWatch Streaming Listener, onReceiverError:" + appName);
System.out.println("CloudWatch Streaming Listener, onReceiverError:" + appName);
    writeRecieverStatsToCloudWatch(receiverError);
  }

    /**
    * This method will just send one, whenever there is a recieverError
  */
  public void writeRecieverStatsToCloudWatch(StreamingListenerReceiverError receiverError) {
    metricUtil.sendHeartBeat(dimentionsMap,"receiverError");
  }

  void writeBatchStatsToCloudWatch(StreamingListenerBatchCompleted batchCompleted) {

    long processingTime = batchCompleted.batchInfo().processingDelay().isDefined() ? 
        (long)batchCompleted.batchInfo().processingDelay().get() : 0;

    long schedulingDelay = batchCompleted.batchInfo().schedulingDelay().isDefined() && (long)batchCompleted.batchInfo().schedulingDelay().get() > 0 ? 
         (long)batchCompleted.batchInfo().schedulingDelay().get() : 0;

    long numRecords = batchCompleted.batchInfo().numRecords();

    metricUtil.sendHeartBeat(dimentionsMap,"batchCompleted");
    System.out.println("CloudWatch Streaming Listener, writeBatchStatsToCloudWatch:" + appName);
    metricUtil.pushMillisecondsMetric(dimentionsMap, "schedulingDelay", schedulingDelay );
    metricUtil.pushMillisecondsMetric(dimentionsMap, "processingDelay", (long)batchCompleted.batchInfo().processingDelay().get() );
    metricUtil.pushCountMetric(dimentionsMap, "numRecords", (double)numRecords);
    metricUtil.pushMillisecondsMetric(dimentionsMap, "totalDelay", (long)batchCompleted.batchInfo().totalDelay().get());

    log.info("Batch completed at: " + batchCompleted.batchInfo().processingEndTime().get() +
             " was started at: " + batchCompleted.batchInfo().processingStartTime().get() + 
             " submission time: " + batchCompleted.batchInfo().submissionTime() +
             " batch time: " + batchCompleted.batchInfo().batchTime() + 
             " batch processing delay: " + batchCompleted.batchInfo().processingDelay().get() + 
             " records : " + numRecords +
             " total batch delay:" + batchCompleted.batchInfo().totalDelay().get() + 
             " product prefix:" + batchCompleted.batchInfo().productPrefix() +
             " schedulingDelay:" + schedulingDelay +
             " processingTime:" + processingTime 
             );
  }
  
}
