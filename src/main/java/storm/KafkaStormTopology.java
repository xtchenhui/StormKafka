package storm;

import java.util.Properties;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

public class KafkaStormTopology extends BaseTopology {
	
    public KafkaStormTopology(String configFileLocation) throws Exception {
		super(configFileLocation);
		// TODO Auto-generated constructor stub
	}

	
	public static void main(String[] args) throws Exception
	  {
	    //Variable TOP_N number of words
	    int TOP_N = 1000;
	    // create the topology
	    TopologyBuilder builder = new TopologyBuilder();

	    // now create the tweet spout with the credentials
	    // credential
		String configFileLocation = "topology-conf.properties";
	    topologyConfig = new Properties();
	    topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
	    String zkConnString = topologyConfig.getProperty("zookeeper");
	    String topicName = topologyConfig.getProperty("topic");
		BrokerHosts hosts = new ZkHosts(zkConnString);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

	    // attach the tweet spout to the topology - parallelism of 1
	    builder.setSpout("kafka-tweet-spout", kafkaSpout, 1);
	    
	    builder.setBolt("tweet-original", new TweetKafkabolt(),1).shuffleGrouping("kafka-tweet-spout");
	    // attach the parse tweet bolt using shuffle grouping
	    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-original");
	    builder.setBolt("infoBolt", new InfoBolt(), 10).fieldsGrouping("parse-tweet-bolt", new Fields("county_id"));
	    builder.setBolt("top-words", new TopWords(), 10).fieldsGrouping("infoBolt", new Fields("county_id"));
	    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("top-words");

	    builder.setBolt("hbaseBolt", HBaseUpdateBolt.make(topologyConfig), 1).shuffleGrouping("parse-tweet-bolt");

	    // create the default config object
	    Config conf = new Config();

	    // set the config in debugging mode
	    conf.setDebug(true);

	    if (args != null && args.length > 0) {

	      // run it in a live cluster

	      // set the number of workers for running all spout and bolt tasks
	      conf.setNumWorkers(3);

	      // create the topology and submit with config
	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

	    } else {

	      // run it in a simulated local cluster

	      // set the number of threads to run - similar to setting number of workers in live cluster
	      conf.setMaxTaskParallelism(4);

	      // create the local cluster instance
	      LocalCluster cluster = new LocalCluster();

	      // submit the topology to the local cluster
	      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

	      // let the topology run for 300 seconds. note topologies never terminate!
	      Utils.sleep(300000);

	      // now kill the topology
	      cluster.killTopology("tweet-word-count");

	      // we are done, so shutdown the local cluster
	      cluster.shutdown();
	    }
	  }
}
