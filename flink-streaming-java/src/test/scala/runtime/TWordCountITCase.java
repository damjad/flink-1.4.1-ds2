package runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.junit.Test;
import runtime.sinks.DummySink;
import runtime.sources.RateControlledSourceFunction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.fail;

public class TWordCountITCase extends TBaseTest {


	@Test
	public void testSimpleWordCount() throws Exception {
		FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		JobID jobID = null;
		ActorGateway jobManager = null;

		try {
			jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createWordCountJobGraph();

			jobID = jobGraph.getJobID();

			cluster.submitJobAndWait(jobGraph, true, timeout);


		} finally {
			// clear the CollectionSink set for the restarted job

			// clear any left overs from a possibly failed job
			if (jobID != null && jobManager != null) {
				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);

				try {
					Await.ready(jobRemovedFuture, timeout);
				} catch (TimeoutException | InterruptedException ie) {
					fail("Failed while cleaning up the cluster.");
				}
			}
		}
	}


	private static JobGraph createWordCountJobGraph() {
// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(new String[]{"--policy.rates.path","/home/danish/FastWorkspace/BDMA/TUB/ds2/ds2/controller/running-examples/wordcount/metrics_repo/"});

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		final DataStream<String> text = env.addSource(
			new RateControlledSourceFunction(
				params.getInt("source-rate", 80000),
				params.getInt("sentence-size", 100)))
			.uid("sentence-source")
			.setParallelism(params.getInt("p1", 1));

		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<Tuple2<String, Long>> counts = text.rebalance()
			.flatMap(new Tokenizer())
			.name("Splitter FlatMap")
			.uid("flatmap")
			.setParallelism(params.getInt("p2", 1))
			.keyBy(0)
			.flatMap(new CountWords())
			.name("Count")
			.uid("count")
			.setParallelism(params.getInt("p3", 1));

		GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
		// write to dummy sink
		counts.transform("Latency Sink", objectTypeInfo,
			new DummySink<>())
			.uid("dummy-sink")
			.setParallelism(params.getInt("p3", 1));

		// execute program
		return env.getStreamGraph().getJobGraph();
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1L));
				}
			}
		}
	}

	public static final class CountWords extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

		private transient ReducingState<Long> count;

		@Override
		public void open(Configuration parameters) throws Exception {

			ReducingStateDescriptor<Long> descriptor =
				new ReducingStateDescriptor<Long>(
					"count", // the state name
					new Count(),
					BasicTypeInfo.LONG_TYPE_INFO);

			count = getRuntimeContext().getReducingState(descriptor);
		}

		@Override
		public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
			count.add(value.f1);
			out.collect(new Tuple2<>(value.f0, count.get()));
		}

		public static final class Count implements ReduceFunction<Long> {

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
		}
	}
}
