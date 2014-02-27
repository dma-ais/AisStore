package dk.dma.ais.store.materialize.util;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class StatisticsWriter {

	private AtomicInteger count;
	private long startTime;
	private long endTime;
	private Object parent;
	private PrintWriter pw;
	
	private final String[] header = {"getParentClass","getApplicationName","getStartTime","getEndTime","getDuration","getCountValue","getPacketsPerSecond"};

	public long getStartTime() {
		return startTime;
	}

	public long getDuration() {
		return endTime - startTime;
	}

	public long getPacketsPerSecond() {
		try {
			return getCountValue() / (getDuration() / 1000);
		} catch (ArithmeticException e) {
			return -1L;
		}
	}

	private long getCountValue() {
		return count.get();
	}

	public StatisticsWriter(AtomicInteger count, Object parent, PrintWriter pw) {
		this.count = count;
		this.parent = parent;
		
		this.setStartTime(System.currentTimeMillis());
		
		pw.println(header);
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public String toCSV() {
		StringBuilder sb = new StringBuilder();
		

		for (String method : header) {
			try {
				sb.append(this.getClass().getMethod(method).invoke(this));
			} catch (IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException
					| SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sb.append(",");
		}
		sb.append("\n");

		return sb.toString();
	}
	
	public void print() {
		pw.print(toCSV());
	}

	public Class<? extends Object> getParentClass() {
		return parent.getClass();
	}

	public String getApplicationName() {
		return "";
	}

}
