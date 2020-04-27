package runtime;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.testingUtils.TestingCluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TBaseTest {

	private static final int numTaskManagers = 1;
	private static final int slotsPerTaskManager = 16;
	private static final int numSlots = numTaskManagers * slotsPerTaskManager;

	protected static TestingCluster cluster;

	@BeforeClass
	public static void setup() throws Exception {
		// detect parameter change

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slotsPerTaskManager);
		config.setInteger(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), 2048);
//		config.setString(NettyConfig.TRANSPORT_TYPE, "auto");
//		config.setString("metrics.reporters", "slf4j");
//		config.setString("metrics.reporter.slf4j.class", "org.apache.flink.metrics.slf4j.Slf4jReporter");
//		config.setInteger(NettyConfig.REPLICATION_NUM_THREADS_SERVER, 2);
//		config.setInteger(NettyConfig.REPLICATION_NUM_THREADS_CLIENT, 2);
//		config.setLong(ReplicationOptions.NETWORK_REPLICA_BUFFERS_MEMORY_MIN, 16 * 1024 * 1024);
//		config.setLong(ReplicationOptions.NETWORK_REPLICA_BUFFERS_MEMORY_MAX, 32 * 1024 * 1024);
//		config.setFloat(ReplicationOptions.NETWORK_REPLICA_BUFFERS_MEMORY_FRACTION, 0.1f);
//		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 16 * 1024 * 1024);
//		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 32 * 1024 * 1024);
//		config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.2f);
//		config.setBoolean("state.backend.rocksdb.metrics.compaction-pending", true);

		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setInteger(WebOptions.PORT, 8081);


//		config.setString(CheckpointingOptions.ROCKSDB_LOCAL_DIRECTORIES,
//			System.getProperty("java.io.tmpdir") + "/r1," + System.getProperty("java.io.tmpdir") + "/r2");
//
//		if (!NativeIO.hasPosixFadvise()) {
//			config.setString(ReplicationOptions.TASK_MANAGER_CHECKPOINT_READER, "zero-copy");
//		} else {
//			config.setString(ReplicationOptions.TASK_MANAGER_CHECKPOINT_READER, "zero-copy-fadvise-lazy");
//		}

//		config.setString(ReplicationOptions.REPLICATION_REGISTRY_DIRECTORY, System.getProperty("java.io.tmpdir"));
//		config.setString(CheckpointingOptions.STATE_BACKEND, "custom");
//		config.setBoolean(NettyConfig.REPLICATION_SHARE_NETTY_THREADS_POOL, false);
//		config.setInteger(NettyConfig.REPLICATION_NUM_OF_ARENAS, 16);
//		config.setInteger(ReplicationOptions.NETWORK_BUFFERS_SIZE, 32768);
//		config.setInteger(NettyConfig.STATE_REPLICATION_LOW_WATERMARK_FACTOR, 1);
//		config.setInteger(NettyConfig.STATE_REPLICATION_HIGH_WATERMARK_FACTOR, 2);
//		config.setInteger(ReplicationOptions.STATE_REPLICATION_NUM_CONNECTIONS_POOL, 1);
//		config.setInteger(ReplicationOptions.NETWORK_BUFFERS_PER_REPLICA, 4);
//		config.setInteger(ReplicationOptions.NETWORK_BUFFERS_PER_REPLICA_MAX, 8);
//		config.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 4);
//		config.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 16);
//		config.setBoolean(ReplicationOptions.TASK_MANAGER_STATE_ROCKSDB_LOGGING, false);
//		config.setString(ReplicationOptions.TASK_MANAGER_STATE_ROCKSDB_PREDEF, "ssd-highmem");
//		config.setInteger(ReplicationOptions.STATE_REPLICATION_REPLICA_SLOTS, 20);
//		config.setInteger("state.replication.ack.timeout", 120000);
//		config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, false);
//		config.setInteger(ReplicationOptions.STATE_REPLICATION_FACTOR, 1);
//		config.setInteger("state.replication.placement.bin-packing-cache", 3);
//		config.setInteger(ReplicationOptions.STATE_REPLICATION_WRITER_THREADS, 4);
//		config.setString(ReplicationOptions.JM_REPLICATION_DIRECTORY, System.getProperty("java.io.tmpdir") + "/jm");
//		config.setString(ReplicationOptions.REPLICATION_DIRECTORY, System.getProperty("java.io.tmpdir") + "/t1:" + System.getProperty("java.io.tmpdir") + "/t2");

//		config.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, FailoverStrategyLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME);

		cluster = new TestingCluster(config);
		cluster.start();

	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.stop();
		}
	}

}
