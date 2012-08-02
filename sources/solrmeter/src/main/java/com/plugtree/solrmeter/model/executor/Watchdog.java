package com.plugtree.solrmeter.model.executor;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ThreadPoolExecutor;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.plugtree.solrmeter.model.QueryExecutor;

final class Watchdog implements Runnable {
	static private final Logger log = Logger.getLogger(Watchdog.class);

	private final MBeanServer mbean;
	private final ObjectName os;
	private final QueryExecutor exec;
	private final ThreadPoolExecutor liveThreads;
	
	public Watchdog(QueryExecutor fixedPoolRandomExecutor, ThreadPoolExecutor live) {
		this.exec = fixedPoolRandomExecutor;
		liveThreads = live;
		try {
			mbean = ManagementFactory.getPlatformMBeanServer();
				os = new ObjectName("java.lang:type=OperatingSystem");
				
			} catch (Exception e) {
				log.error("fail to access mbean", e);
				throw new RuntimeException(e);
			}
	}


	@Override
	public void run() {
		
		AttributeList attributes;
		try {
			attributes = mbean.getAttributes(os, 
					new String[]{"AvailableProcessors","SystemLoadAverage"});
		} catch (Exception e) {
			log.error("fail to access mbean: "+mbean+" "+os, e);
			throw new RuntimeException(e);
		}
		log.info(attributes + " live workers "+
				liveThreads.getActiveCount());
		double cores = ((Integer)
				((Attribute)attributes.get(0)).getValue()).doubleValue();
		double la = ((Double)((Attribute)attributes.get(1)).getValue()).doubleValue();
		if((int)cores==(int)la){
			log.warn("cores: "+cores+", load average:"+la);
		}else
			if(cores<la){
				log.error("haling! cores: "+cores+", load average:"+la);
				exec.stop();
			}
		
	}
}