package com.yarn.esper.conf;

import org.apache.hadoop.util.Shell;

public class EsperConfiguration {
	
	public static final String ESPER_APPLICATION_CLASSPARH = "esper.application.classpath";
	public static final String[] DEFAULT_ESPER_APPLICATION_CLASSPARH = {
			Environment.ESPER_HOME.toString() + "/*",
			Environment.ESPER_HOME.toString() + "/esper/lib/*",
			Environment.ESPER_ENGINE_LIB.toString() + "/*"
	};
	
	private enum Environment {
		ESPER_HOME("/usr/esper/esper-6.0.1"),
		ESPER_ENGINE_LIB("/usr/esper/lib");
		
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
