package storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.tools.SentimentAnalyzer;

public class TweetKafkabolt extends BaseRichBolt{
	OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	    SentimentAnalyzer.init();
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
	    String ret = input.getString(0);
	    String geoInfo;
	    String originalTweet;
	    // if no tweet is available, wait for 50 ms and return
	    if (ret==null)
	    {
	      Utils.sleep(50);
	      return;
	    }
	    else
	    {
	        geoInfo = ret.split("DELIMITER")[1];
	        originalTweet = ret.split("DELIMITER")[0];
	    }
	    
	    if(geoInfo != null && !geoInfo.equals("n/a"))
	    {
	        System.out.print("\t DEBUG SPOUT: BEFORE SENTIMENT \n");
	        int sentiment = SentimentAnalyzer.findSentiment(originalTweet)-2;
	        System.out.print("\t DEBUG SPOUT: AFTER SENTIMENT (" + String.valueOf(sentiment) + ") for \t" + originalTweet + "\n");
	        collector.emit(new Values(ret, sentiment));
	    }		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	    declarer.declare(new Fields("tweet", "sentiment"));		
	}

}
