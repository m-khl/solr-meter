package com.plugtree.solrmeter.model.executor;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.plugtree.solrmeter.model.operation.Operation;
import com.plugtree.solrmeter.model.operation.QueryOperation;

public abstract class FixedRateLimitPool  {

	final class MinuteSentinel implements Runnable {
		private final int minuteRate;

		MinuteSentinel(int minuteRate) {
			this.minuteRate = minuteRate;
		}

		@Override
		public void run() {
			final long startAt = System.currentTimeMillis();
			// every minute generate necessary randomly distributed ops.
			ArrayList<Future<?>> thisMinuteTasks = new ArrayList<Future<?>>();
			try{
				for(int i=0; i<minuteRate && !Thread.currentThread().isInterrupted(); i++){
					final int step = random.nextInt(60000);
					final long expectedAt = startAt+step;
					final Runnable purpose = createTask();
					thisMinuteTasks.add(
							timer.schedule(
								new AsyncCallExactlyAt(purpose, expectedAt, startAt,
										FixedRateLimitPool.this.executor),
								expectedAt-System.currentTimeMillis(), 
								TimeUnit.MILLISECONDS) 		);
				}
			}finally{
				cancelMinuteTasks();
				if(!Thread.currentThread().isInterrupted()){
					minuteTasks.addAll(thisMinuteTasks);
				}
			}
		}
	}

	static final class AsyncCallExactlyAt implements Runnable {
		private final Runnable purpose;
		private final long expectedAt;
		private final long validMinute;
		private final ExecutorService excutor;

		AsyncCallExactlyAt(Runnable purpose, long expectedAt, long startAt, ExecutorService exec) {
			this.purpose = purpose;
			this.expectedAt = expectedAt;
			this.validMinute = startAt;
			this.excutor = exec;
		}

		@Override
		public void run() {
			long now = System.currentTimeMillis();
			if(now-validMinute-60000>1 ) {
				log.warn("timer pool overflow: skipping task with "+(now-expectedAt)+ " ms delay");
			}
			else{
				Future<?> future = excutor.submit(purpose);
				long schedulingDelay = System.currentTimeMillis()-now;
				if(schedulingDelay>1){
					log.warn("executor pool overflow: lazyly cancelling task, which took "+ schedulingDelay
							+" ms for scheduling");
					future.cancel(false);
				}
			}
		}
	}

	private static final Logger log = Logger.getLogger(FixedRateLimitPool.class);
	
	final protected ScheduledExecutorService timer;
	final protected ExecutorService executor;
	private final ConcurrentSkipListSet<Future<?>> minuteTasks = new ConcurrentSkipListSet<Future<?>>();
	private ScheduledFuture<?> watchDog;
	protected final static Random random = new Random();
	
	public FixedRateLimitPool(ScheduledExecutorService timer,
			ExecutorService excutor) {
		this.timer = timer;
		this.executor = excutor;
	}

	public synchronized void startWithRate(final int minuteRate){
		stop();
		watchDog = timer.scheduleAtFixedRate(
				new MinuteSentinel(minuteRate),
				0, 60000, TimeUnit.MILLISECONDS);
	}

	public synchronized void stop() {
		if(isRunning()){
			watchDog.cancel(true);
			watchDog = null;
		}
		cancelMinuteTasks();
	}

	protected void cancelMinuteTasks() {
		for(Future<?> f:minuteTasks){
			f.cancel(true);
		}
		minuteTasks.clear();
	}

	protected abstract Runnable createTask();

	public boolean isRunning() {
		return watchDog!=null && !watchDog.isCancelled() && !watchDog.isDone();
	}

}
