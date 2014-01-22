package de.tuberlin.dima.ml.pact;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.ErrorInPlanAssemblerException;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.client.nephele.api.ProgramInvocationException;
import eu.stratosphere.pact.common.plan.Plan;

/**
 * Runs a Stratosphere job (jar), either locally (using LocalExecutor) or using
 * the PACT/Nephele client api. Measures the runtime.
 * 
 * Allows the job to be executed on a cluster without using the shell/cli.
 */
public class JobRunner {
  
  private long lastWallClockRuntime = 0;    // runtime as seen from the client
  private long lastNetRuntime = 0;  // net runtime of the JobEvent
  
  private Logger logger = LoggerFactory.getLogger(this.getClass());
  
  /**
   * Run a Stratosphere job on a local instance of Nephele (a bit like Hadoop
   * local mode)
   * 
   * Note that only the local filesystem is supported, so data sources and sinks
   * must use the "file://" schema.
   * 
   * @return the total runtime in milliseconds 
   * 
   * @throws Exception
   */
  public void runLocal(Plan plan) throws Exception {
    LocalExecutor executor = new LocalExecutor();
    executor.start();

    final Stopwatch stop = new Stopwatch();
    long netRuntime = 0;
    try {
      stop.start();
      netRuntime = executor.executePlan(plan);
      stop.stop();
    } catch (Exception e) {
      executor.stop();
      throw e;
    }
    
    executor.stop();
    
    lastWallClockRuntime = stop.elapsed(TimeUnit.MILLISECONDS);
    lastNetRuntime = netRuntime; 
  }

   /**
   * Runs a Stratosphere job from a jar file using the pact client api. Measures
   * the total runtime.
   * 
   * @param jarPath
   *          Path to the jar to execute
   * @param jobArgs
   *          Arguments for the job
   * @param assemblerClassName
   *          Optional: Name of the Assembler class (e.g.
   *          de.tu-berlin.dima.MyJob) If empty, it will execute the default job
   *          (as defined in jar manifest)
   * @param configPath
   *          Optional: The path (or file) containing the cluster-configuration
   *          (e.g. jobmanager). If empty, defaults will be applied (localhost)
   * @param jobManagerAddress
   *          If not empty, it will overwrite the default values and the values
   *          that may be read from a conf file
   * @param jobManagerPort
   *          If not empty, it will overwrite the default values and the values
   *          that may be read from a conf file
   * @param waitForCompletion
   *          If true, the function will return when the job finished, otherwise
   *          immediately
   */
  public void run(String jarPath, String assemblerClassName, String[] jobArgs, String configPath, String jobManagerAddress, String jobManagerPort, boolean waitForCompletion) {

    File jar = new File(jarPath);

    try {

      // PactProgram wraps plan related function
      // Pass (classname and) arguments here
      // PactProgram(File jarFile, String... args)
      // PactProgram(File jarFile, String className, String... args)
      PactProgram prog = null;
      if (assemblerClassName.equals("")) {
        prog = new PactProgram(jar, jobArgs);
      } else {
        prog = new PactProgram(jar, assemblerClassName, jobArgs);
      }

      // Client compiles and submits pact programs to nephele cluster
      // Configuration is the nephele configuration
      // - Only Jobmanager address and port are read from configuration file
      // - Reads all files in specified directory
      // - results in warning, because nephele-plugins.xml does not start with
      // configuration
      if (!configPath.equals("")) {
        System.out.println("JobRunner: Load configuration from " + configPath);
        GlobalConfiguration.loadConfiguration(configPath);
      }
      Configuration config = GlobalConfiguration.getConfiguration();
      if (configPath.equals("")) {
        // Apply defaults (is there another way to get a default configuration?)
        System.out.println("JobRunner: No config path defined, load defaults (127.0.0.1:" + ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT + ")");
        config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "127.0.0.1");
        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
      }
      // Overwrite the values in the conf dir and in the 
      if (!"".equals(jobManagerAddress)) {
        System.out.println("JobRunner: Overwrite JobManager address: " + jobManagerAddress);
        config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAddress);
      }
      if (!"".equals(jobManagerPort)) {
        System.out.println("JobRunner: Overwrite JobManager port: " + jobManagerPort);
        config.setString(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort);
      }
      
      Client client = new Client(config);
      
      printPlanJson(prog, client);

      // Client.run(...)
      //  - compiles the PactProgram to a OptimizedPlan using
      // PactCompiler.compile()
      //  - compiles OptimizedPlan to JogGraph using
      // JogGraphGenerator.compileJobGraph(plan)
      //  - Uses ...nephele.client.JobClient.submitJob()/submitJobAndWait()
      //    to submit Job to nephele
      // unfortunately jobDuration returned from submitJobAndWait() is not
      // returned
      // probably because it is only returned if we wait for completion?!
      
      // Here we get: cannot access OptimizedPlan class file for OptimizedPlan not found
      // PROBLEM: Looks for short name, not for full qualified name!
      // This cannot be compiled from maven / javac. Works only if Eclipse built the class file for this class before
      //  http://stackoverflow.com/questions/9693889/maven-class-file-for-not-found-compilation-error
      //  http://maven.40175.n5.nabble.com/Maven-project-can-t-access-class-file-td4599976.html
      System.out.println("JobRunner: Run Job");
      final Stopwatch stop = new Stopwatch();
      stop.start();
      client.run(prog, waitForCompletion);
      stop.stop();

      lastNetRuntime = 0;   // not available
      lastWallClockRuntime = stop.elapsed(TimeUnit.MILLISECONDS);
      System.out.println("Elapsed time (ms): " + lastWallClockRuntime);

    } catch (ProgramInvocationException e) {
      System.out.println(e.toString());
    } catch (ErrorInPlanAssemblerException e) {
      System.out.println(e.toString());
    }
  }
  
  private void printPlanJson(PactProgram program, Client client) {
	
	String jsonPlan = null;
	try {
		jsonPlan = client.getOptimizerPlanAsJSON(program);
	} catch (ProgramInvocationException e) {
		e.printStackTrace();
	} catch (ErrorInPlanAssemblerException e) {
	  e.printStackTrace();
	}
	
	if(jsonPlan != null) {
	  logger.info("-------------------- PACT Execution Plan ---------------------");
	  logger.info(jsonPlan);
	  logger.info("--------------------------------------------------------------");
	} else {
	  logger.error("JSON plan could not be compiled.");
	}
  }
  

  /**
   * Starts a Ozone job with the parameters defined in an java property file
   * Currently not used
   */
  public void runFromPropertyFile(String propertyFile, boolean waitForCompletion) {

    Properties prop = new Properties();
    try {
      prop.load(new FileInputStream(propertyFile));
    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.exit(0);
    }
    int numArgs = Integer.parseInt(prop.getProperty("job.numargs", ""));
    System.out.println("Number of parameters: " + numArgs);
    String[] params = new String[numArgs];
    for (int i = 0; i < numArgs; ++i) {
      params[i] = prop.getProperty("job.arg" + (i + 1), "");
      System.out.println("param" + i + ": " + params[i]);
    }

    String jobJar = prop.getProperty("job.jar", "");

    String configPath = prop.getProperty("system.configpath", "");

    run(jobJar, "", params, configPath, "", "", waitForCompletion);
  }
  
  /**
   * @return The runtime for the last job as seen from the client
   */
  public long getLastWallClockRuntime() {
    return lastWallClockRuntime;
  }
  
  /**
   * Attention: Not supported if we run on a cluster!
   * 
   * @return The runtime for the internal JobEvent of the last job.
   */
  public long getLastNetRuntime() {
    return lastNetRuntime;
  }

}
