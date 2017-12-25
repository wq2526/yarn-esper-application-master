package com.yarn.conf;

import org.apache.hadoop.util.Shell;

public class MonitorConfiguration {
	
	public static final String MONITOR_APPLICATION_CLASSPATH = "monitor.app.classpath";
	public static final String DEFAULT_MONITOR_APPLICATION_CLASSPATH = 
			Environment.MONITOR_LIB.toString() + "/*";
	
	private enum Environment {
		MONITOR_LIB("/usr/hadoop-yarn/monitor/lib");
		
		private final String variable;
	    private Environment(String variable) {
	      this.variable = variable;
	    }
	    
	    public String key() {
		      return variable;
		    }
		    
		    public String toString() {
		      return variable;
		    }

		    public String $() {
		      if (Shell.WINDOWS) {
		        return "%" + variable + "%";
		      } else {
		        return "$" + variable;
		      }
		    }

		    public String $$() {
		      return "{{" + variable + "}}";
		    }
	}

}
