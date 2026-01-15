package net.lavacro.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinTopic<R extends ConnectRecord<R>> implements Transformation<R> {
	private static final Logger log = LoggerFactory.getLogger(RoundRobinTopic.class);

	public static final String TARGET_TOPIC_BASE = "traffic-topic";
	public static final String NUM_TOPICS = "num.topics";

	public static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(TARGET_TOPIC_BASE, ConfigDef.Type.STRING, TARGET_TOPIC_BASE, ConfigDef.Importance.HIGH,
					"Base name for target topics")
			.define(NUM_TOPICS, ConfigDef.Type.INT, 4, ConfigDef.Importance.HIGH,
					"Number of subtopics to round-robin across");

	private String baseTopic;
	private int numTopics;
	private int counter = 0;

	@Override
	public void configure(Map<String, ?> configs) {
		AbstractConfig cfg = new AbstractConfig(CONFIG_DEF, configs);
		this.baseTopic = cfg.getString(TARGET_TOPIC_BASE);
		this.numTopics = cfg.getInt(NUM_TOPICS);

		if (numTopics <= 0) {
			throw new IllegalArgumentException("num.topics must be > 0");
		}

		log.info("RoundRobinRouter configured: baseTopic={}, numTopics={}", baseTopic, numTopics);
	}

	@Override
	public R apply(R rekord) {
		if(rekord == null) {
			log.warn("Null record");
			return null;
		}

		counter = ++counter % numTopics;
		String newTopic = String.format("%s-%02d", baseTopic, counter);

		if (log.isTraceEnabled()) {
			log.trace("Routing record from topic={} to {}", rekord.topic(), newTopic);
		}

		return rekord.newRecord(
				newTopic,					// topic
				null,						// partition (let broker decide)
				null,		//record.keySchema(),
				rekord.key(),
				null,		//record.valueSchema(),
				rekord.value(),
				rekord.timestamp()
		);
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void close() {
		// nothing to clean up
	}
}
