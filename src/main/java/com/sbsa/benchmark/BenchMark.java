package com.sbsa.benchmark;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sbsa.benchmark.kafka.KafkaClientFactory;
import com.sbsa.benchmark.kafka.KafkaClientMonitor;
import com.sbsa.benchmark.natsio.NatsIOFactory;
import com.sbsa.benchmark.natsio.NatsIOMonitor;
import com.sbsa.benchmark.solace.SolFactory;
import com.sbsa.benchmark.solace.SolMonitor;
import com.sbsa.benchmark.solaceclient.SolClientFactory;
import com.sbsa.benchmark.solaceclient.SolClientMonitor;

public class BenchMark {
	private static final Log Trace = LogFactory.getLog(BenchMark.class);

	private ConcurrentHashMap<String, Object> runtimeProperties = null;

	public BenchMark(String[] paramArrayOfString) {
		this.runtimeProperties = generateRuntimeProperties(paramArrayOfString);
	}

	public static ConcurrentHashMap<String, Object> generateRuntimeProperties(String[] arg) {
		ConcurrentHashMap<String, Object> properties = new ConcurrentHashMap<String, Object>();

		properties.put("Implmentation", Implementation.valueOf(arg[0]));
		properties.put("NumProducers", Integer.parseInt(arg[1]));
		properties.put("NumConsumers", Integer.parseInt(arg[2]));
		properties.put("NumberOfEndpoints", Integer.parseInt(arg[3]));
		properties.put("EndpointType", EndPointType.valueOf(arg[4]));
		properties.put("DeliveryMode", DeliveryMode.valueOf(arg[5]));
		properties.put("PayLoadSize", Double.parseDouble(arg[6]));
		properties.put("Duration", Double.parseDouble(arg[7]));
		properties.put("NumOfMessages", Integer.parseInt(arg[8]));

		properties.put("BrokerIP", arg[9]);
		properties.put("BrokerPort", arg[10]);

		properties.put("SourceType", SourceType.valueOf(arg[11]));

		return properties;
	}

	public static void main(String... paramArrayOfString) throws Exception {
		if (paramArrayOfString.length < 1) {
			System.exit(1);
		}
		BenchMark localbenchMark = new BenchMark(paramArrayOfString);
		if (localbenchMark.run() == ReturnCode.FAILURE) {
			System.exit(1);
		} else {
			System.exit(0);
		}
	}

	public ReturnCode run() throws Exception {
		Factory factory;
		Monitor monitor;

		Implementation implementation = (Implementation) runtimeProperties.get("Implmentation");
		switch (implementation) {
		case Solace: {
			factory = new SolFactory(this.runtimeProperties);
			monitor = new SolMonitor(this.runtimeProperties, factory);
		}
			break;
		case SolaceClient: {
			factory = new SolClientFactory(this.runtimeProperties);
			monitor = new SolClientMonitor(this.runtimeProperties, factory);
		}
			break;
		case Kafka: {
			factory = new KafkaClientFactory(this.runtimeProperties);
			monitor = new KafkaClientMonitor(this.runtimeProperties, factory);
		}
			break;
		case NatsIO: {
			factory = new NatsIOFactory(this.runtimeProperties);
			monitor = new NatsIOMonitor(this.runtimeProperties, factory);
		}
			break;
		default:
			throw new RuntimeException("No Implmentation Set");
		}

		monitor.createEndPoints();
		monitor.createConsumers();
		monitor.createProducers();
		monitor.startConsumers();

		monitor.start();

		while (monitor.isBusy()) {
			try {
				String output = System.console().readLine();
				if (output != null && output.toUpperCase().trim().equals("EXPORT")) {
					monitor.export();
				} else if (output != null && output.toUpperCase().trim().equals("END")) {
					monitor.stop();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return ReturnCode.SUCCESS;
	}

	private static enum ReturnCode {
		SUCCESS, FAILURE;

		private ReturnCode() {
		}
	}

}
