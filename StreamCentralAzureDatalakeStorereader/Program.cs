using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Rest.Azure.Authentication;
using Npgsql;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Rest;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.Management.DataFactory.Models;

namespace AzureDatalakeStorereader
{
    class Program
    {
        private static string[] argList;
        public static AdlsClient client;
        public static NpgsqlConnection conn = null;

        public static string filenamemain = "LogFileInput";

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
            Logging.WriteToLog(LoggerEnum.INFO, "Get Datalakedata Entry: ", filenamemain);

            try
            {
                JObject jOpAllSourcesData = (JObject.Parse(DataSourceDetails.ExecuteDataSourceDetails()));

                DataSourceDetails.Sources = DataSourceDetails.LoadDataSourceObjects(jOpAllSourcesData);

                Console.WriteLine("Number of data sources: " + DataSourceDetails.Sources.Count);

                foreach (JObject JSourceData in DataSourceDetails.Sources)
                {
                    IEnumerable<DirectoryEntry> v = null;
                    JObject joutputmain = new JObject();
                    JObject engines = (JObject)JSourceData["engines"];
                    JArray pull = null;
                    string id = "0";
                    string sourcename = JSourceData["SPOKENAME"].ToString();
                    string sourceid = JSourceData["SOURCEID"].ToString();
                    string filename = "logfileoutput" + sourcename + "_" + sourceid;
                    string sqlcount = string.Empty;
                    string createddate = string.Empty;
                    JObject jsonOpattr = new JObject();
                    
                    Logging.WriteToLog(LoggerEnum.INFO, "sourcename  : " + sourcename, filename);
                    Logging.WriteToLog(LoggerEnum.INFO, "sourceid  : " + sourceid, filename);
                    Console.WriteLine("DATASOURCE: sourceid: " + sourceid + " sourcename: " + sourcename + " started");

                    try
                    {
                        pull = (JArray)engines["f1"];
                        createddate = pull[0]["createddate"].ToString();
                        createddate = createddate.Substring(0, 10);
                        id = pull[0]["id"].ToString();
                    }
                    catch (Exception ex)
                    {
                        createddate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                    }

                    Logging.WriteToLog(LoggerEnum.INFO, "createddate  : " + createddate, filename);
                    Logging.WriteToLog(LoggerEnum.INFO, "id  : " + id, filename);

                    DateTime dtcreateddate = Convert.ToDateTime(createddate);

                    try
                    {
                        sqlcount = GetDatamartCount(sourcename, dtcreateddate, filename);
                    }
                    catch (Exception exdb)
                    {
                        Logging.WriteToLog(LoggerEnum.ERROR, "Exception occured for fetching the source records count in DM: " + exdb.Message, filename);
                    }

                    jsonOpattr["TransType"] = "updatedatalakestore";
                    jsonOpattr["sqlcount"] = sqlcount;
                    jsonOpattr["SOURCEID"] = sourceid;
                    jsonOpattr["id"] = id;
                    
                    string containerpath = GetdataSourceType(sourcename);
                    
                    string[] adlsFilePaths = FormatAdlsFilePaths(containerpath,sourcename);

                    foreach (string path in adlsFilePaths)
                    {
                        try
                        {
                            v = client.EnumerateDirectory(path);

                            Logging.WriteToLog(LoggerEnum.INFO, "ADLS path: " + path, filename);

                            Console.WriteLine(path);

                            if (v != null)
                            {
                                var v1 = (from cust in v
                                         where cust.LastModifiedTime >= dtcreateddate
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

                                UpdateADLSFileMoficationDateStatus(joutputmain.ToString(), filename);

                            }
                        }
                        catch (AdlsException ex)
                        {
                            Logging.WriteToLog(LoggerEnum.INFO, "Exception occured in finding the path: " + path, filename);
                        }
                    }   
                    

                }                          
                              
                Console.WriteLine("DATASOURCE ITERATIONS COMPLETED");
               
                Console.Read();
            }

            catch (Exception ex1)
            {
                Console.WriteLine("MAIN :" + ex1.Message);
                Logging.WriteToLog(LoggerEnum.ERROR, ex1.Message, "MainException");
                Console.Read();
            }
        }

        private static string[] FormatAdlsFilePaths(string containerpath, string sourcename)
        {
            if (sourcename.Contains("DSAstaCloudProjectdboProject")) { }
            string[] paths = new string[3];
            paths[0] = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + containerpath + "/" + sourcename + "/" + sourcename + "_Transactional";
            paths[1] = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + containerpath + "/" + sourcename + "/1Day_Transactional_" + sourcename;
            paths[2] = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + containerpath + "/Fact_" + sourcename + "/1Day_Transactional_Fact_" + sourcename;

            return paths;
        }
       

        public static void UpdateADLSFileMoficationDateStatus(string input,string filename)
        {
            try
            {
                Logging.WriteToLog(LoggerEnum.INFO, "UpdateADLSFileMoficationDateStatus Start time : " + DateTime.Now, filename);
                Logging.WriteToLog(LoggerEnum.INFO, input, filename);
                string comm = "select usp_dbservgetsourcecommonurl('" + input.ToString() + "')";
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
