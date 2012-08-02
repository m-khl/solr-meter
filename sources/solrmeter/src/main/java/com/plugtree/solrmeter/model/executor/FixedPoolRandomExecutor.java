package com.plugtree.solrmeter.model.executor;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.plugtree.solrmeter.model.QueryExecutor;
import com.plugtree.solrmeter.model.QueryStatistic;
import com.plugtree.solrmeter.model.SolrMeterConfiguration;
import com.plugtree.solrmeter.model.SolrServerRegistry;
import com.plugtree.solrmeter.model.exception.OperationException;
import com.plugtree.solrmeter.model.exception.QueryException;
import com.plugtree.solrmeter.model.generator.QueryGenerator;
import com.plugtree.solrmeter.model.operation.Operation;
import com.plugtree.solrmeter.model.operation.QueryOperation;
import com.plugtree.stressTestScope.StressTestScope;

@StressTestScope
public class FixedPoolRandomExecutor implements QueryExecutor {

	static private final Logger log = Logger.getLogger(FixedPoolRandomExecutor.class);
	// it's really could be fixed
	private final static ThreadPoolExecutor live = new ThreadPoolExecutor(1, 
			Integer.valueOf(SolrMeterConfiguration.getProperty("solr.maxWorkers","100")),
	  60L, TimeUnit.SECONDS,
	  new SynchronousQueue<Runnable>());
	
	protected final static ScheduledExecutorService timer = Executors.newScheduledThreadPool(10);
	
	/**
	 * Solr Server for strings
	 * TODO implement provider
	 */
	private CommonsHttpSolrServer server;
	
	/**
	 * List of Statistics observing this Executor.
	 */
	private List<QueryStatistic> statistics;
	
    /**
     * The generator that creates a query depending on the query mode selected
     */
    private QueryGenerator queryGenerator;
	private volatile int operationsPerMinute;
	private FixedRateLimitPool fixedRateLimitPool;
	private ScheduledFuture<?> loadAverageWatchDog;

    @Inject
	public FixedPoolRandomExecutor(@Named("queryGenerator") final QueryGenerator queryGenerator) {
		super();
        this.queryGenerator = queryGenerator;
		statistics = new LinkedList<QueryStatistic>();
		this.operationsPerMinute = Integer.valueOf(
				SolrMeterConfiguration.getProperty(SolrMeterConfiguration.QUERIES_PER_MINUTE)
				).intValue();
		fixedRateLimitPool = new FixedRateLimitPool(timer, live) {
			
			@Override
			protected Runnable createTask() {
				final Operation op = 
						new QueryOperation(FixedPoolRandomExecutor.this, queryGenerator);
				return new Runnable() {
					@Override
					public void run() {
						try {
							op.execute();
						} catch (Exception e) {
							log.info("on  invoking "+op, e);
						}
					}
				};
			}
		};
	}

	@Override
	public CommonsHttpSolrServer getSolrServer() {
		if(server == null) {
			server = SolrServerRegistry.getSolrServer(SolrMeterConfiguration.getProperty(SolrMeterConfiguration.SOLR_SEARCH_URL));
		}
		return server;
	}

	@Override
	public void notifyQueryExecuted(QueryResponse response, long clientTime) {
		for(QueryStatistic statistic:statistics) {
			statistic.onExecutedQuery(response, clientTime);
		}
	}

	@Override
	public void notifyError(QueryException exception) {
		for(QueryStatistic statistic:statistics) {
			statistic.onQueryError(exception);
		}
	}

	@Override
	public void addStatistic(QueryStatistic statistic) {
		this.statistics.add(statistic);
	}

	@Override
	public int getQueriesPerMinute() {
		return operationsPerMinute;
	}

	@Override
	public void prepare() {
	}

	@Override
	public void start() {
		loadAverageWatchDog = timer.scheduleAtFixedRate(new Watchdog(this, live), 0, 20, TimeUnit.SECONDS);
		fixedRateLimitPool.startWithRate(operationsPerMinute);
	}


	@Override
	public void stop() {
		try{
			fixedRateLimitPool.stop();
		}finally{
			if(loadAverageWatchDog!=null){
				loadAverageWatchDog.cancel(true);
				loadAverageWatchDog=null;
			}
		}
	}

	@Override
	public void incrementOperationsPerMinute() {
		operationsPerMinute+=1;
		SolrMeterConfiguration.setProperty(getOperationsPerMinuteConfigurationKey(), String.valueOf(operationsPerMinute));
		if(isRunning()){
			fixedRateLimitPool.startWithRate(operationsPerMinute);
		}
	}
	
	protected String getOperationsPerMinuteConfigurationKey() {
		return "solr.load.queriesperminute";
	}

	@Override
	public void decrementOperationsPerMinute() {
		operationsPerMinute-=1;
		operationsPerMinute = operationsPerMinute<1?1:operationsPerMinute;
		SolrMeterConfiguration.setProperty(getOperationsPerMinuteConfigurationKey(), String.valueOf(operationsPerMinute));
		if(isRunning()){
			log.info("restarting with "+operationsPerMinute+" operations for minute.");
			fixedRateLimitPool.startWithRate(operationsPerMinute);
		}
	}

	@Override
	public boolean isRunning() {
		return fixedRateLimitPool.isRunning();
	}
}
