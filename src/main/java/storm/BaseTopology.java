package storm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class BaseTopology {

	private static final Logger LOG = Logger.getLogger(BaseTopology.class);

	protected static  Properties topologyConfig;
	
	public BaseTopology(String configFileLocation) throws Exception {
		
		topologyConfig = new Properties();
		configFileLocation = "topology-conf.properties";
		try {
			topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
		} catch (FileNotFoundException e) {
			LOG.error("Encountered error while reading configuration properties: "
				+ e.getMessage());
			throw e;
		} catch (IOException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		}			
	}
}