/*
 * author Neeraj Goswamy
 * Desc hadoop-2.0.2-alpha Yarn Client
 */

import java.util.*;
import java.net.URI;
import java.net.InetSocketAddress;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.api.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class YarnClient {
   public static void main(String args[]){
     ClientRMProtocol applicationsManager; 
    Configuration conf = new Configuration();
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = 
        NetUtils.createSocketAddr(yarnConf.get(
            YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS));             
    System.out.println("Connecting to ResourceManager at " + rmAddress);
    Configuration appsManagerServerConf = new Configuration(conf);

try{
    YarnRPC rpc = YarnRPC.create(appsManagerServerConf);
    applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, appsManagerServerConf));   
 
    GetNewApplicationRequest request = 
        Records.newRecord(GetNewApplicationRequest.class);              
    GetNewApplicationResponse response = 
        applicationsManager.getNewApplication(request);
    System.out.println("Got new ApplicationId=" + response.getApplicationId());
  // Create a new ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = 
        Records.newRecord(ApplicationSubmissionContext.class);
    // set the ApplicationId 
    appContext.setApplicationId(response.getApplicationId());
    // set the application name
    appContext.setApplicationName("NeerajApp");
    
    // Create a new container launch context for the AM's container
    ContainerLaunchContext amContainer = 
        Records.newRecord(ContainerLaunchContext.class);
    // Define the local resources required 
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
   Path jarPath= new Path("hdfs://master:9000/user/neeraj/tmp.jar"); // <- known path to jar file  
 
    Configuration nconf = new Configuration();
    FileSystem fs = FileSystem.get(nconf);
    FileStatus jarStatus = fs.getFileStatus(jarPath);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
   
   amJarRsrc.setType(LocalResourceType.FILE);
   amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);          
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath)); 
 
    amJarRsrc.setTimestamp(jarStatus.getModificationTime());
    amJarRsrc.setSize(jarStatus.getLen());
    localResources.put("tmp.jar",  amJarRsrc);    
    // Set the local resources into the launch context    
    amContainer.setLocalResources(localResources);

    // Set up the environment needed for the launch context
    Map<String, String> env = new HashMap<String, String>();    
    String classPathEnv = "$CLASSPATH:./*:";    
    env.put("CLASSPATH", classPathEnv);
    amContainer.setEnvironment(env);
    // Construct the command to be executed on the launched container 
    String command =  "${JAVA_HOME}" + "/bin/java" + " MyApp" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";                     
    //    " arg1 arg2 arg3" + 
    List<String> commands = new ArrayList<String>();
    commands.add(command);
    amContainer.setCommands(commands);
    
   Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(1);
    amContainer.setResource(capability);
    
    // Set the container launch content into the ApplicationSubmissionContext
    appContext.setAMContainerSpec(amContainer);

// Create the request to send to the ApplicationsManager 
    SubmitApplicationRequest appRequest = 
        Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);

     applicationsManager.submitApplication(appRequest);

GetApplicationReportRequest reportRequest = 
          Records.newRecord(GetApplicationReportRequest.class);
      reportRequest.setApplicationId(response.getApplicationId());
      GetApplicationReportResponse reportResponse = 
          applicationsManager.getApplicationReport(reportRequest);
      ApplicationReport report = reportResponse.getApplicationReport();
 System.out.println("Tracking Url " + report.getTrackingUrl());
} catch(Exception ioe)
{ ioe.printStackTrace();
}
 }
}
