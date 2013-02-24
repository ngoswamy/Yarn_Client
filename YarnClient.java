import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.net.URI;
import java.net.InetSocketAddress;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
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
/*    appsManagerServerConf.setClass(
        YarnConfiguration.YARN_SECURITY_INFO,
        ClientRMSecurityInfo.class, SecurityInfo.class); 
*/
try{
    YarnRPC rpc = YarnRPC.create(appsManagerServerConf);
    applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, appsManagerServerConf));   
 
 //Next step
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
    // Lets assume the jar we need for our ApplicationMaster is available in 
    // HDFS at a certain known path to us and we want to make it available to
    // the ApplicationMaster in the launched container 
    Path jarPath= new Path("hdfs://master:9000/user/neeraj/tmp.jar"); // <- known path to jar file  
    //FileStatus jarStatus = (new org.apache.hadoop.fs.LocalFileSystem()).getFileStatus(jarPath);
    Configuration nconf = new Configuration();
    FileSystem fs = FileSystem.get(nconf);
    FileStatus jarStatus = fs.getFileStatus(jarPath);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    // Set the type of resource - file or archive
    // archives are untarred at the destination by the framework
    amJarRsrc.setType(LocalResourceType.FILE);
    // Set visibility of the resource 
    // Setting to most private option i.e. this file will only 
    // be visible to this instance of the running application
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);          
    // Set the location of resource to be copied over into the 
    // working directory
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath)); 
    // Set timestamp and length of file so that the framework 
    // can do basic sanity checks for the local resource 
    // after it has been copied over to ensure it is the same 
    // resource the client intended to use with the application
    amJarRsrc.setTimestamp(jarStatus.getModificationTime());
    amJarRsrc.setSize(jarStatus.getLen());
    // The framework will create a symlink called AppMaster.jar in the 
    // working directory that will be linked back to the actual file. 
    // The ApplicationMaster, if needs to reference the jar file, would 
    // need to use the symlink filename.  
    localResources.put("tmp.jar",  amJarRsrc);    
    // Set the local resources into the launch context    
    amContainer.setLocalResources(localResources);

    // Set up the environment needed for the launch context
    Map<String, String> env = new HashMap<String, String>();    
    // For example, we could setup the classpath needed.
    // Assuming our classes or jars are available as local resources in the
    // working directory from which the command will be run, we need to append
    // "." to the path. 
    // By default, all the hadoop specific classpaths will already be available 
    // in $CLASSPATH, so we should be careful not to overwrite it.   
    String classPathEnv = "$CLASSPATH:./*:";    
    env.put("CLASSPATH", classPathEnv);
    amContainer.setEnvironment(env);
    // Construct the command to be executed on the launched container 
    String command =  "${JAVA_HOME}" + "/bin/java" + " MyApp" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";                     
    //    " arg1 arg2 arg3" + 
    List<String> commands = new ArrayList<String>();
    commands.add(command);
    // add additional commands if needed                

    // Set the command array into the container spec
    amContainer.setCommands(commands);
    
    // Define the resource requirements for the container
    // For now, YARN only supports memory so we set the memory 
    // requirements. 
    // If the process takes more than its allocated memory, it will 
    // be killed by the framework. 
    // Memory being requested for should be less than max capability 
    // of the cluster and all asks should be a multiple of the min capability. 
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(1);
    amContainer.setResource(capability);
    
    // Set the container launch content into the ApplicationSubmissionContext
    appContext.setAMContainerSpec(amContainer);

// Create the request to send to the ApplicationsManager 
    SubmitApplicationRequest appRequest = 
        Records.newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);

    // Submit the application to the ApplicationsManager
    // Ignore the response as either a valid response object is returned on 
    // success or an exception thrown to denote the failure
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
//-------------------
 }
}
