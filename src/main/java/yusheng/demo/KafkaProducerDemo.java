package yusheng.demo;

public class KafkaProducerDemo {
	public static final String TOPIC = "BlockChainCharts";

	public static void main(String[] args) {
		boolean isAsync = true;
		SampleProducer producerThread = new SampleProducer(TOPIC, isAsync, 2000);
		producerThread.start();
	}
}