using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Npgsql;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Rest;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Rest.Azure.Authentication;

namespace AzureDatalakeStorereader
{
    class Program
    {
        private static string[] argList;
        public static AdlsClient client;
        public static NpgsqlConnection conn = null;
        public static string[] iterations = new string[500];
        public static int IterationsCount = 0;

        static void Main(string[] args)
        {
            var applicationId = ConfigurationManager.AppSettings["ApplicationId"];// "b93a619a-d985-49e4-a3f4-b9e97f958bb5";
            var secretKey = ConfigurationManager.AppSettings["Password"];//"cAxb4+mSLXfMZhulLbm5+t9Jx65z1TkR3MH8yv1pEMU=";
            var tenantId = ConfigurationManager.AppSettings["ActiveDirectoryTenantId"];//"90fa383b-7959-47d2-8c3a-ece9d79acd2b";
            var adlsAccountName = ConfigurationManager.AppSettings["adlsName"] + ".azuredatalakestore.net";
            var creds = ApplicationTokenProvider.LoginSilentAsync(tenantId, applicationId, secretKey).Result;

            client = AdlsClient.CreateClient(adlsAccountName, creds);

            try
            {
                argList = args;

                if (argList != null && argList.Length > 0 && argList.First().Contains("1"))
                {
                    GetDatalakedata(client);
                }
                else if (argList != null && argList.Length > 0 && argList.First().Contains("2"))
                {
                    RerunPipeline(creds);
                }
                else if (argList != null && argList.Length > 0 && argList.First().Contains("3"))
                {
                    for (int i = 0; i <= 10000; i++)
                    {
                        ADLSGen1FileOperations.AppendToFile("/Samples/Output/Ex", i);
                    }
                }
                else
                {
                    GetDatalakedata(client);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to read any input from command line. Executing statements after that now:" + ex.Message);

            }
            finally
            {
                client = null;
                creds = null;
            }
        }

        private static void RerunPipeline(ServiceClientCredentials cred)
        {
            DateTime lastupdatedbf = DateTime.Now.AddHours(-12);
            DateTime lastupdatedaf = DateTime.Now.AddHours(12);
            
            RunFilterParameters runFilter = new RunFilterParameters(lastupdatedaf,lastupdatedaf);
            var client = new DataFactoryManagementClient(cred) { SubscriptionId = ConfigurationManager.AppSettings["subscriptionId"] };
            CreateRunResponse runResponse = null;
            Console.WriteLine("Pipeline run ID: " + runResponse.RunId);
            var activityRun = client.ActivityRuns.QueryByPipelineRun
            (ConfigurationManager.AppSettings["resourceGroupName"], "CGS-Data-Factory1", runResponse.RunId, runFilter);
            
            //var context = new AuthenticationContext(ConfigurationManager.AppSettings["adlsName"] + ".azuredatalakestore.net"  +ConfigurationManager.AppSettings["ActiveDirectoryTenantId"]);
            //ClientCredential cc = new ClientCredential(ConfigurationManager.AppSettings["ApplicationId"], ConfigurationManager.AppSettings["Password"]);
            //AuthenticationResult result = context.AcquireTokenAsync("https://management.azure.com/", cc).Result;
            //   ServiceClientCredentials cred = new TokenCredentials(result.AccessToken);
            // string parameters = @"/subscriptions/1a73e65a-48a0-4f70-8460-ad8131ec7f1d/resourcegroups/CGS-TechFinium/providers/Microsoft.DataFactory/datafactories/CGS-Data-Factory1 /datapipelines/";

            // List<Microsoft.Azure.Management.DataFactory.Models.ActivityRun> activityRuns =


            // client.Pipelines.CreateRunWithHttpMessagesAsync(ConfigurationManager.AppSettings["resourceGroupName"], "CGS-Data-Factory1", "SC_PL_D_PreProd_1Day_LastIteration_DSAstaCloudProjectdboConsumableAllocation", parameters);
        }

        public static void GetDatalakedata(AdlsClient client)
        {
            Logging.WriteToLog(LoggerEnum.INFO, "Get Datalakedata Entry: ", Constants.LogFileInputPrefix);

            try
            {
                
                JObject jOpAllSourcesData = (JObject.Parse(DataSourceDetails.ExecuteDataSourceDetails()));
                
                DataSourceDetails.LoadDataSourceObjects(jOpAllSourcesData);

                Console.WriteLine("Number of data sources: " + DataSourceDetails.Sources.Count);

                foreach (JObject JSourceData in DataSourceDetails.Sources)
                {
                    IEnumerable<DirectoryEntry> v = null;
                    
                    LoadDataSourceDataFromJson(JSourceData);

                    string filename = Constants.LogFileOutputPrefix + DataSource.DataSourceName + "_" + DataSource.DataSourceID;
                    string sqlcount = string.Empty;
                   
                    JObject jsonOpattr = new JObject();
                    JObject joutputmain = new JObject();
                    
                    Logging.WriteToLog(LoggerEnum.INFO, "sourcename  : " + DataSource.DataSourceName, filename);
                    Logging.WriteToLog(LoggerEnum.INFO, "sourceid  : " + DataSource.DataSourceID, filename);

                    Console.WriteLine("DATASOURCE: sourceid: " + DataSource.DataSourceID + " sourcename: " + DataSource.DataSourceName + " started");
                                       
                    try
                    {
                        sqlcount = GetDatamartCount(DataSource.DataSourceName, DataSource.DSLastIteratedDate, filename);
                    }
                    catch (Exception exdb)
                    {
                        Logging.WriteToLog(LoggerEnum.ERROR, "Exception occured for fetching the source records count in DM: " + exdb.Message, filename);
                    }

                    jsonOpattr["TransType"] = "updatedatalakestore";
                    jsonOpattr["sqlcount"] = sqlcount;
                    jsonOpattr["SOURCEID"] = DataSource.DataSourceID;
                    jsonOpattr["id"] = DataSource.EngineInstanceId;
                    jsonOpattr["adlaprovid"] = DataSource.ADLAProvId;
                    jsonOpattr["isfileupdated"] = 1;

                    foreach (string path in FormatAdlsFilePaths())
                    {
                        bool IsPathFound = false;
                        try
                        {
                            v = client.EnumerateDirectory(path);

                            Logging.WriteToLog(LoggerEnum.INFO, "ADLS path: " + path, filename);
                           
                            Console.WriteLine(path);

                            if (v != null)
                            {                               
                                var v1 = (from cust in v
                                            //where cust.LastModifiedTime >= DataSource.DSLastIteratedDate
                                            orderby cust.LastModifiedTime descending
                                            select cust).Take(1).ElementAt(0);

                                Logging.WriteToLog(LoggerEnum.INFO, "lambda query", filename);

                                if (v1 != null && v1.Length > 1)
                                {
                                    string rep = v1.Name.Replace("Data_", "");

                                    Console.WriteLine("Success");
                                    Console.WriteLine("LAST MODIFIED:{0},Length:{1},Name:{2}", v1.LastModifiedTime, v1.Length, v1.Name);
                                    Logging.WriteToLog(LoggerEnum.INFO, "v1.LastModifiedTime" + v1.LastModifiedTime, filename);
                                    Logging.WriteToLog(LoggerEnum.INFO, message: "v1.Length" + v1.Length, fileName: filename);
                                    Logging.WriteToLog(LoggerEnum.INFO, message: "v1.Name" + v1.Name, fileName: filename);

                                    if (v1.LastModifiedTime < DataSource.DSLastIteratedDate)
                                    {   
                                        jsonOpattr["isfileupdated"] = 0;
                                    }
                                    
                                    jsonOpattr["path"] = rep;
                                    jsonOpattr["size"] = v1.Length;
                                    jsonOpattr["filename"] = v1.FullName;
                                    jsonOpattr["lastmodifieddate"] = v1.LastModifiedTime;

                                    if (jsonOpattr["lastmodifieddate"] != null)
                                    {
                                        joutputmain["INPUT"] = jsonOpattr;
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("file size is less");

                                    joutputmain["INPUT"] = jsonOpattr;
                                }

                                Console.WriteLine("joutput :" + joutputmain);

                                UpdateStatusInMasterDB(joutputmain.ToString(), filename);

                                iterations[IterationsCount] = "file path found and processed";
                                IterationsCount++;

                                Console.WriteLine("File Path found and updated: ");
                                
                            }
                        }
                        catch (AdlsException adlsEx)
                        {
                            Logging.WriteToLog(LoggerEnum.INFO, "Exception occured in finding the path: " + path, filename);
                        }
                        catch (IndexOutOfRangeException indexEx)
                        {
                            Logging.WriteToLog(LoggerEnum.ERROR, message: " File not updated: ", fileName: filename);
                        }
                    }
                }

                Console.WriteLine("Total counts updated:" + IterationsCount);

                Console.WriteLine("DATASOURCE ITERATIONS COMPLETED...");
                
                Console.Read();
            }
            catch (Exception ex1)
            {
                Console.WriteLine("Exception in Main Method :" + ex1.Message);

                Logging.WriteToLog(LoggerEnum.ERROR, ex1.Message, "MainException");

                Console.Read();
            }
        }

        private static void LoadDataSourceDataFromJson(JObject jSourceData)
        {
            try
            {
                DataSource.DataSourceID = (string)jSourceData[Constants.JsonDSDetailsDataSourceID];
                DataSource.DataSourceName = (string)jSourceData[Constants.JsonDSDetailsDataSourceName];
                DataSource.ADLAType = (string)jSourceData[Constants.JsonDSDetailsADLAType];
                DataSource.ADLAProvisioningName = (string)jSourceData[Constants.JsonDSDetailsADLAName];
                DataSource.FolderPath = (string)jSourceData[Constants.JsonDSDetailsFolderPath];
                DataSource.DSCreatedDate = (DateTime)jSourceData[Constants.JsonDSDetailsDSCreatedDate];

                if(jSourceData[Constants.JsonDSDetailsDSLastIteratedDate] != null)
                {
                    DataSource.DSLastIteratedDate = (DateTime)jSourceData[Constants.JsonDSDetailsDSLastIteratedDate];
                }

                if (jSourceData[Constants.JsonDSDetailsADLACreatedDate] != null)
                {
                    DataSource.ADLAProvCreatedDate = (DateTime)jSourceData[Constants.JsonDSDetailsADLACreatedDate];
                }

                try
                {
                    DataSource.ADLAProvModifiedDate = (DateTime)jSourceData[Constants.JsonDSDetailsADLAModifiedDate];
                }
                catch(Exception ex)
                {
                    DataSource.ADLAProvModifiedDate = DateTime.Now.AddYears(18);
                }
                DataSource.ContainerPath = (string)jSourceData[Constants.JsonDSDetailsContainerPath];
                DataSource.Frequency = (string)jSourceData[Constants.JsonDSDetailsFrequency];
                DataSource.FrequencyUOM = (string)jSourceData[Constants.JsonDSDetailsFrequencyUOM];
                DataSource.EngineInstanceId = (string)jSourceData[Constants.JsonDSDetailsEngineInstanceID];
                DataSource.ADLAProvId = (string)jSourceData[Constants.JsonDSDetailsADLAProvID];
            }
            catch(Exception ex)
            {
                Console.WriteLine("Exception occured in loading the data source attributes:" + ex);
            }
        }

        private static string[] FormatAdlsFilePaths()
        {
            string[] paths = new string[3];
            try
            {
                DataSource.ContainerPath = GetdataSourceType(DataSource.DataSourceName);

                paths[0] = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + DataSource.ContainerPath + "/" + DataSource.DataSourceName + "/" + DataSource.DataSourceName + "_" + DataSource.ADLAType;
                paths[1] = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + DataSource.ContainerPath + "/" + DataSource.DataSourceName + "/" + DataSource.Frequency + DataSource.FrequencyUOM + "_" + DataSource.ADLAType + "_" + DataSource.DataSourceName;
                paths[2] = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + DataSource.ContainerPath + "/Fact_" + DataSource.DataSourceName + "/" + DataSource.Frequency + DataSource.FrequencyUOM + "_" + DataSource.ADLAType + "_" + DataSource.DataSourceName;

                Console.WriteLine(paths[0]);
                Console.WriteLine(paths[1]);
                Console.WriteLine(paths[2]);
                
            }
            catch(Exception ex)
            {
                Logging.WriteToLog(LoggerEnum.ERROR, String.Format("Exception occured in building the paths: {0} , {1}", DataSource.DataSourceName, DataSource.ContainerPath), "inputfile");
            }
            return paths;
        }
       

        public static void UpdateStatusInMasterDB(string input,string filename)
        {
            try
            {
                Logging.WriteToLog(LoggerEnum.INFO, "UpdateADLSFileMoficationDateStatus Start time : " + DateTime.Now, filename);
                Logging.WriteToLog(LoggerEnum.INFO, input, filename);
                string comm = "select usp_adla('" + input.ToString() + "')";
                Logging.WriteToLog(LoggerEnum.INFO, comm, filename);

                MasterDBUtils.MasterDBConnect();
                object objResult1 = MasterDBUtils.GenerateSQLQueryCommand(comm).ExecuteScalar();
                string strresult2 = objResult1.ToString();

              
                
                Logging.WriteToLog(LoggerEnum.INFO, "OUTPUT" + strresult2, filename);
                Logging.WriteToLog(LoggerEnum.INFO, input, filename);
                Logging.WriteToLog(LoggerEnum.INFO, "UpdateADLSFileMoficationDateStatus End time : " + DateTime.Now, filename);
            }
            catch (Exception exup)
            {
                MasterDBUtils.CloseConnections();
            }
            finally
            {
                MasterDBUtils.CloseConnections();
            }
        }

        public static void FetchandUpdateDatamartCounts()
        {
            Logging.WriteToLog(LoggerEnum.INFO, "Get GetDatamartCounts Entry: ", Constants.LogFileInputPrefix);

            try
            {
                JObject jOpAllSourcesData = (JObject.Parse(DataSourceDetails.ExecuteDataSourceDetails()));

                DataSourceDetails.LoadDataSourceObjects(jOpAllSourcesData);

                Console.WriteLine("Number of data sources: " + DataSourceDetails.Sources.Count);


                foreach (JObject JSourceData in DataSourceDetails.Sources.GroupBy( i => i.ElementAt(1),(key,group) => group.First()).ToArray())
                {
                    LoadDataSourceDataFromJson(JSourceData);

                    string filename = Constants.LogFileOutputPrefix + DataSource.DataSourceName + "_" + DataSource.DataSourceID;
                    string sqlcount = string.Empty;

                    JObject jsonOpattr = new JObject();
                    JObject joutputmain = new JObject();

                    Logging.WriteToLog(LoggerEnum.INFO, "sourcename  : " + DataSource.DataSourceName, filename);
                    Logging.WriteToLog(LoggerEnum.INFO, "sourceid  : " + DataSource.DataSourceID, filename);

                    Console.WriteLine("DATASOURCE: sourceid: " + DataSource.DataSourceID + " sourcename: " + DataSource.DataSourceName + " started");

                    try
                    {
                        sqlcount = GetDatamartCount(DataSource.DataSourceName, DataSource.DSLastIteratedDate, filename);
                    }
                    catch (Exception exdb)
                    {
                        Logging.WriteToLog(LoggerEnum.ERROR, "Exception occured for fetching the source records count in DM: " + exdb.Message, filename);
                    }

                    jsonOpattr["TransType"] = "updatedmcounts";
                    jsonOpattr["sqlcount"] = sqlcount;
                    jsonOpattr["SOURCEID"] = DataSource.DataSourceID;
                    jsonOpattr["id"] = DataSource.EngineInstanceId;
                    jsonOpattr["adlaprovid"] = DataSource.ADLAProvId;
                    joutputmain["INPUT"] = jsonOpattr;

                    UpdateStatusInMasterDB(joutputmain.ToString(), filename);                    
                }

                Console.WriteLine("DATASOURCE ITERATIONS COMPLETED");

                Console.Read();
            }

            catch (Exception ex)
            {
                Console.WriteLine("Exception occured in UpdateDatamartCounts() Main procedure: " + ex.Message);

                Logging.WriteToLog(LoggerEnum.ERROR, ex.Message, "MainException");
                Console.Read();
            }

        }

        public static string GetDatamartCount(string tableName, DateTime date,string filename)
        {
            
            Logging.WriteToLog(LoggerEnum.INFO, "GetDatamartCount Start time : " + DateTime.Now, filename);

            string result = string.Empty;
            
            string query = "select count(1) from Fact_" + tableName + "metricdetails where recorddateutc>='" + date.ToString("yyyy-MM-dd") + "' and recorddateutc<='" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'";

            Logging.WriteToLog(LoggerEnum.INFO, query, filename);
            
            try
            {
                Console.WriteLine("SQL DATAMART CONN is CONNECTED");

                SqlConnection connect = SQLUtils.SQLConnect();

                SqlCommand cmd = SQLUtils.GenerateSQLQueryCommand(query);

                if (connect.State == ConnectionState.Closed)
                {
                    connect.Open();
                }
                object obj2 = cmd.ExecuteScalar();

                result = obj2.ToString();

                
                Logging.WriteToLog(LoggerEnum.INFO, result, filename);
            } catch (Exception ex)
            {
                result = "-1";
                Logging.WriteToLog(LoggerEnum.INFO, result, filename);

                Console.WriteLine("SQL DATAMART CONN is FAILED");

                Logging.WriteToLog(LoggerEnum.ERROR, ex.Message, filename);
            }
            
            Logging.WriteToLog(LoggerEnum.INFO, "GetDatamartCount End time : " + DateTime.Now, filename);
            return result;
        }

        public static string GetdataSourceType(string tableName)
        {
            string dsType = string.Empty;
            if (tableName.ToLower().Contains("donseed") || tableName.ToLower().Contains("seed"))
            {
                dsType = "DonSeed";
            }
            else if (tableName.ToLower().Contains("seneca") || tableName.ToLower().Contains("gatehousesuite"))
            {
                dsType = "GateHouseSeneca";
            }
            else if (tableName.ToLower().Contains("snowdroplive"))
            {
                dsType = "SnowdropLive";
            }
            else if (tableName.ToLower().Contains("snowdroptest"))
            {
                dsType = "SnowdropTest";
            }
            else if (tableName.ToLower().Contains("snowdroptrain"))
            {
                dsType = "SnowdropTrain";
            }
            else if (tableName.ToLower().Contains("sharepoint"))
            {
                dsType = "SharePointOnline";
            }
            //else if (tableName.ToLower().Contains("sharepointonlinedoc") || tableName.ToLower().Contains("sharepointdoc"))
            //{
            //    dsType = "SharePointOnlineDocuments";
            //}
            else if (tableName.ToLower().Contains("servicenow") || tableName.ToLower().Contains("service"))
            {
                dsType = "ServiceNow";
            }
            else if (tableName.ToLower().Contains("ldap"))
            {
                dsType = "ActiveDirectory";
            }
            else if ((tableName.ToLower().Contains("rivohse")) || (tableName.ToLower().Contains("rivo")))
            {
                dsType = "RivoHSE";
            }
            else if (tableName.ToLower().Contains("excel") && (tableName.ToLower().Contains("coins")))
            {
                dsType = "CoinsEXCEL";
            }
            else if (tableName.ToLower().Contains("coins"))
            {
                dsType = "CoinsSQL";
            }
            else if (tableName.ToLower().Contains("pocbdl") || tableName.ToLower().Contains("bdl") ||
                tableName.ToLower().Contains("cmt") || tableName.ToLower().Contains("bdl-cmt"))
            {
                dsType = "BDL-CMT";
            }
            else if (tableName.ToLower().Contains("assettagz") || tableName.ToLower().Contains("assettag"))
            {
                dsType = "AssetTagz";
            }
            else if (tableName.ToLower().Contains("asta") || tableName.ToLower().Contains("astapowerproject"))
            {
                dsType = "AstaPowerProject";
            }
            else if (tableName.ToLower().Contains("dynamics") || tableName.ToLower().Contains("microsoftdynamic") || tableName.ToLower().Contains("microsoftdynamics") || tableName.ToLower().Contains("dynamic") || tableName.ToLower().Contains("kpid"))
            {
                dsType = "MicrosoftDyanmics";
            }

            else
            {
                dsType = tableName;
            }
            return dsType;
        }
    }
}
