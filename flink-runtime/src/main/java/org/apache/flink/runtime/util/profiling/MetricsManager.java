/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util.profiling;

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * The MetricsManager is responsible for logging activity profiling information (except for messages).
 * It gathers start and end events for deserialization, processing, serialization, blocking on read and write buffers
 * and records activity durations. There is one MetricsManager instance per Task (operator instance).
 * The MetricsManager aggregates metrics in a {@link ProcessingStatus} object and outputs processing and output rates
 * periodically to a designated rates file.
 */
public class MetricsManager implements Serializable {

	private final String taskId; // Flink's task description
	private final String workerName; // The task description string logged in the rates file
	private final int instanceId; // The operator instance id
	private final int numInstances; // The total number of instances for this operator

	private long recordsIn = 0;	// Total number of records ingested since the last flush
	private long recordsOut = 0;	// Total number of records produced since the last flush
	private long usefulTime = 0;	// Total period of useful time since last flush
	private long waitingTime = 0;	// Total waiting time for input/output buffers since last flush

	private long currentWindowStart;

	private final ProcessingStatus status;

	private final long windowSize;	// The aggregation interval
	private  final String ratesPath;	// The file path where to output aggregated rates

	private long epoch = 0;	// The current aggregation interval. The MetricsManager outputs one rates file per epoch.

	/**
	 * @param taskDescription the String describing the owner operator instance
	 * @param jobConfiguration this job's configuration
	 */
	public MetricsManager(String taskDescription, Configuration jobConfiguration) {
		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		workerName = workerId.substring(0, workerId.indexOf("(")-1);
		instanceId = Integer.parseInt(workerId.substring(workerId.indexOf("(")+1, workerId.indexOf("/")));
		numInstances = Integer.parseInt(workerId.substring(workerId.indexOf("/")+1, workerId.indexOf(")")));
		status = new ProcessingStatus();
		windowSize = jobConfiguration.getLong("policy.windowSize",  10_000_000_000L);
		ratesPath = jobConfiguration.getString("policy.rates.path", "rates/");
		currentWindowStart = status.getProcessingStart();
	}

	/**
	 * Once the current input buffer has been consumed, calculate and log useful and waiting durations
	 * for this buffer.
	 * @param timestamp the end buffer timestamp
	 * @param deserialization total duration of deserialization for this buffer
	 * @param processing total duration of processing for this buffer
	 * @param numRecords total number of records processed
	 */
	public void inputBufferConsumed(long timestamp, long deserialization, long processing, long numRecords) {

		synchronized (status) {
			if (currentWindowStart == 0) {
				currentWindowStart = timestamp;
			}

			status.setProcessingEnd(timestamp);

			// aggregate the metrics
			recordsIn += numRecords;
			recordsOut += status.getNumRecordsOut();
			usefulTime += processing + status.getSerializationDuration() + deserialization
				- status.getWaitingForWriteBufferDuration();

			// clear status counters
			status.clearCounters();

			// if window size is reached => output
			if (timestamp - currentWindowStart > windowSize) {

				// compute rates
				long duration = timestamp - currentWindowStart;
				double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000000;
				double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000000;
				double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000000;
				double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000000;

				// log the rates: one file per epoch
				String ratesLine = workerName + ","
					+ instanceId  + ","
					+ numInstances  + ","
					+ currentWindowStart + ","
					+ trueProcessingRate + ","
					+ trueOutputRate + ","
					+ observedProcessingRate + ","
					+ observedOutputRate;
				List<String> rates = Arrays.asList(ratesLine);

				Path ratesFile = Paths.get(ratesPath + workerName.trim() + "-" + instanceId + "-" + epoch + ".log").toAbsolutePath();
				try {
					Files.write(ratesFile, rates, Charset.forName("UTF-8"));
				} catch (IOException e) {
					System.err.println("Error while writing rates file for epoch " + epoch
						+ " on task " + taskId + ".");
					e.printStackTrace();
				}

				// clear counters
				recordsIn = 0;
				recordsOut = 0;
				usefulTime = 0;
				currentWindowStart = 0;
				epoch++;
			}
		}
	}

	/**
	 * A new input buffer has been retrieved with the given timestamp.
	 */
	public void newInputBuffer(long timestamp) {
		status.setProcessingStart(timestamp);
		// the time between the end of the previous buffer processing and timestamp is "waiting for input" time
		status.setWaitingForReadBufferDuration(timestamp - status.getProcessingEnd());
	}

	public void addSerialization(long serializationDuration) {
		status.addSerialization(serializationDuration);
	}

	public void incRecordsOut() {
		status.incRecordsOut();
	}

	public void addWaitingForWriteBufferDuration(long duration) {
		status.addWaitingForWriteBuffer(duration);

	}

	/**
	 * The source consumes no input, thus it must log metrics whenever it writes an output buffer.
	 * @param timestamp the timestamp when the current output buffer got full.
	 */
	public void outputBufferFull(long timestamp) {
		if (taskId.contains("Source")) {

			synchronized (status) {

				if (currentWindowStart == 0) {
					currentWindowStart = timestamp;
				}

				setOutBufferStart(timestamp);

				// aggregate the metrics
				recordsOut += status.getNumRecordsOut();
				if (status.getWaitingForWriteBufferDuration() > 0) {
					waitingTime += status.getWaitingForWriteBufferDuration();
				}

				// clear status counters
				status.clearCounters();

				// if window size is reached => output
				if (timestamp - currentWindowStart > windowSize) {

					// compute rates
					long duration = timestamp - currentWindowStart;
					usefulTime = duration - waitingTime;
					double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000000;
					double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000000;


					// log the rates: one file per epoch
					String ratesLine = workerName + ","
						+ instanceId  + ","
						+ numInstances  + ","
						+ currentWindowStart + ","
						+ 0 + ","
						+ trueOutputRate + ","
						+ 0 + ","
						+ observedOutputRate;
					List<String> rates = Arrays.asList(ratesLine);

					Path ratesFile = Paths.get(ratesPath + workerName.trim() + "-" + instanceId + "-" + epoch + ".log").toAbsolutePath();
					try {
						Files.write(ratesFile, rates, Charset.forName("UTF-8"));
					} catch (IOException e) {
						System.err.println("Error while writing rates file for epoch " + epoch
							+ " on task " + taskId + ".");
						e.printStackTrace();
					}

					// clear counters
					recordsOut = 0;
					usefulTime = 0;
					waitingTime = 0;
					currentWindowStart = 0;
					epoch++;
				}
			}
		}
	}

	private void setOutBufferStart(long start) {
		status.setOutBufferStart(start);
	}
}
