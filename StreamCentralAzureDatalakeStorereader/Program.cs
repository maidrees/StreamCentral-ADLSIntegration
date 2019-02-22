using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.Rest.ClientRuntime;
using Microsoft.Rest.ClientRuntime.Azure.Authentication;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Npgsql;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;
using Microsoft.IdentityModel.Clients.ActiveDirectory;


namespace AzureDatalakeStorereader
{
    class Program
    {
        private static string[] argList;
        public static AdlsClient client;
        public static string connString = ConfigurationManager.AppSettings["MasterConnString"];// "User ID=postgres;Password=pass@word1;Host=127.0.0.1;Port=5432;Database=prod10thdec;";
        public static NpgsqlConnection conn = null;
        public static string datamartconnString = ConfigurationManager.AppSettings["SCDMConn"];//@"Persist Security Info=False;Integrated Security=false;Initial Catalog = datamartCareysLive; Server=172.20.0.6\MSSQLSERVERDEV;User Id=sa;Password=Unl0ckm3";
        public static SqlConnection datamartconn = new SqlConnection(datamartconnString);
        
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
                datamartconn.Close();
                datamartconn = null;
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


            JObject json = JObject.Parse("{'INPUT':{'TransType':'dashboard','VID':34,'LoginId':1619,'filter':'all','pagesize':10,'pagenumber':0,'lastid':0}}");
            string connString = ConfigurationManager.AppSettings["MasterConnString"];// "User ID=postgres;Password=pass@word1;Host=127.0.0.1;Port=5432;Database=prod10thdec;";
            string filenamemain = "LogFileInput";
            Logging.WriteToLog(LoggerEnum.INFO, connString, filenamemain);
            NpgsqlConnection conn;

            try
            {
                Logging.WriteToLog(LoggerEnum.INFO, "connection string message" , filenamemain);
              
                    conn = new NpgsqlConnection(connString);

                    conn.Open();
             
                 
                    NpgsqlCommand command = new NpgsqlCommand("select usp_dbservgetsourcecommonurl('" + json.ToString() + "')", conn);
                    command.CommandTimeout = 0;
                   
                  
           
               
                object objResult = command.ExecuteScalar();
                string strresult = objResult.ToString();
                Logging.WriteToLog(LoggerEnum.INFO, "INPUTquery" + strresult, filenamemain);
                JObject js = JObject.Parse(strresult);
                JArray jarr = (JArray)js["SPOKE"];

                Console.WriteLine("No. datasources" + jarr.Count);

                foreach (JObject jojbj in jarr)
                {
                    string id = "0";
                    string sourcename = jojbj["SPOKENAME"].ToString();
                    string sourceid = jojbj["SOURCEID"].ToString();
                    string filename = "logfileoutput" + sourcename + "_" + sourceid;
                    Console.WriteLine("DATASOURCE:" + sourceid );
                    Console.WriteLine("DATASOURCE:" + sourcename + " started");
                    bool isdbstatusupdated = false;
                    string sqlcount = string.Empty;
                    bool sqlcall = false;


                    Logging.WriteToLog(LoggerEnum.INFO, "sourcename  : " + sourcename, filename);
                    Logging.WriteToLog(LoggerEnum.INFO, "sourceid  : " + sourceid, filename);

                    JObject engines = (JObject)jojbj["engines"];
                    JArray pull = null;
                    string createddate = string.Empty;
                    try
                    {
                        pull = (JArray)engines["f1"];
                        createddate = pull[0]["createddate"].ToString();
                        createddate=createddate.Substring(0,10);
                        id = pull[0]["id"].ToString();
                    }
                    catch (Exception ex)
                    {
                        createddate = DateTime.Now.ToString("MM/dd/yyyy");
                
                    }

                    Logging.WriteToLog(LoggerEnum.INFO, "createddate  : " + createddate, filename);
                    Logging.WriteToLog(LoggerEnum.INFO, "id  : " + id, filename);

                    DateTime dtcreateddate = Convert.ToDateTime(createddate);
                    
                    try
                    {
                        if (!sqlcall)
                        {
                            sqlcount = GetDatamartCount(sourcename, dtcreateddate, filename);
                            sqlcall = true;
                        }
                    }
                    catch(Exception exdb)
                    {
                        Logging.WriteToLog(LoggerEnum.INFO, "EXCEPTION FOR SQL  : " + exdb.Message, filename);
                    }


                    string containerpath = GetdataSourceType(sourcename);
                    bool patherror = false;

                    string path1 = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + containerpath + "/" + sourcename + "/" + sourcename + "_Transactional";
                    string path2 = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + containerpath + "/" + sourcename + "/1Day_Transactional_" + sourcename;
                    string path3 = "/" + ConfigurationManager.AppSettings["folderPath"] + "/DL-" + containerpath + "/Fact_" + sourcename + "/1Day_Transactional_Fact_" + sourcename;



                    ///Careys-Provisioned-Data-Live/DL-ServiceNow/DSServiceNowUsers/1Day_Transactional_DSServiceNowUsers
                    /// /Careys-Provisioned-Data-Live/DL-ServiceNow/DSServiceNowUsers/DSServiceNowUsers_Transactional

                    //Careys-Provisioned-Data-Live/DL-ServiceNow/DSServiceNowUsers/1Day_Transactional_DSServiceNowUsers
                    //Careys-Provisioned-Data-Live/DL-MicrosoftDynamics/DSDynamicCareysProject/1Day_Transactional_DSDynamicCareysProject


                    int iterator = 0;
                    JObject joutput =  new JObject();
                    joutput["TransType"] = "updatedatalakestore";
                    joutput["sqlcount"] = sqlcount;
                    joutput["SOURCEID"] = sourceid;
                    joutput["id"] = id;
                    do
                    {
                        iterator++;
                       
                        try
                        {

                           

                            if (patherror)
                            {
                                path2 = path1;
                                patherror = false;
                            }

                            if (iterator == 3 && sourcename.Contains("dynamics") && patherror)

                            {
                                path2 = path3;
                                patherror = false;
                            }

                            

                            IEnumerable<DirectoryEntry> v = client.EnumerateDirectory(path2);

                            Logging.WriteToLog(LoggerEnum.INFO, "ADLA path : " + path2, filename);

                            if (v == null)
                            {
                                // resetpath:
                                Logging.WriteToLog(LoggerEnum.INFO, "v is null : " + path2, filename);
                                v = client.EnumerateDirectory(path1);

                            }

                            var v1 = from cust in v
                                     where cust.LastModifiedTime >= dtcreateddate
                                     orderby cust.LastModifiedTime descending
                                     select cust;
                            Logging.WriteToLog(LoggerEnum.INFO, "lambda query", filename);


                            foreach (var v2 in v1)
                            {
                                Logging.WriteToLog(LoggerEnum.INFO, "for loop" , filename);

                                if (v2 != null && v2.Length > 1)
                                {
                                    string rep = v2.Name.Replace("Data_", "");
                                    Console.WriteLine("Success");
                                    Console.WriteLine("LAST MODIFIED:{0},Length:{1},Name:{2}", v2.LastModifiedTime, v2.Length, v2.Name);
                                    Logging.WriteToLog(LoggerEnum.INFO, "v2.LastModifiedTime" + v2.LastModifiedTime, filename);
                                    Logging.WriteToLog(LoggerEnum.INFO, "v2.Length" + v2.Length, filename);
                                    Logging.WriteToLog(LoggerEnum.INFO, "v2.Name" + v2.Name, filename);
                                    // JObject joutput = JObject.Parse(" {'INPUT':{'TransType':'updatedatalakestore', 'path':'" + rep + "','size':" + v2.Length + ", 'filename':'" + v2.Name + "',  'lastmodifieddate':'" + v2.LastModifiedTime + "', 'sqlcount':" + sqlcount + ",'SOURCEID':" + sourceid + "} } ')");
                                    //JObject joutput = new JObject();

                                    joutput["path"] = rep;
                                    joutput["size"] = v2.Length;
                                    joutput["filename"] = v2.Name;
                                    joutput["lastmodifieddate"] = v2.LastModifiedTime;




                                    JObject joutputmain = new JObject();
                                    joutputmain["INPUT"] = joutput;
                                    Console.WriteLine("joutput :" + joutputmain);
                                    isdbstatusupdated = true;
                                    UpdateStatus(joutputmain.ToString(), filename);

                                    break;
                                }
                                else
                                {
                                    Console.WriteLine("file size is less");
                                    JObject joutputmain = new JObject();
                                    joutputmain["INPUT"] = joutput;
                                    Console.WriteLine("joutput :" + joutputmain);
                                    isdbstatusupdated = true;
                                    UpdateStatus(joutputmain.ToString(), filename);
                                }

                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Exception :" + ex.Message);
                            Logging.WriteToLog(LoggerEnum.INFO, "Exception : " + ex.Message, filename);

                            if (ex.Message.ToLower().Contains("for path"))
                                patherror = true;

                            if (iterator == 3 && sourcename.Contains("dynamics"))
                            {
                                if (!isdbstatusupdated)
                                {
                                    JObject joutputmain = new JObject();
                                    joutputmain["INPUT"] = joutput;
                                    UpdateStatus(joutputmain.ToString(), filename);
                                    isdbstatusupdated = true;

                                }
                                break;
                            }
                            else if (iterator == 2 && !sourcename.Contains("dynamics"))
                            {
                                if (!isdbstatusupdated)
                                {
                                    JObject joutputmain = new JObject();
                                    joutputmain["INPUT"] = joutput;
                                    UpdateStatus(joutputmain.ToString(), filename);
                                    isdbstatusupdated = true;

                                }
                                break;

                            }
                        }
                        finally
                        {

                            if (iterator == 3 && sourcename.Contains("dynamics"))
                            {
                                if (!isdbstatusupdated)
                                {
                                    JObject joutputmain = new JObject();
                                    joutputmain["INPUT"] = joutput;
                                    UpdateStatus(joutputmain.ToString(), filename);
                                    isdbstatusupdated = true;
                                    
                                }
                               
                            }
                            else if (iterator == 2 && !sourcename.Contains("dynamics"))
                            {
                                if (!isdbstatusupdated)
                                {
                                    JObject joutputmain = new JObject();
                                    joutputmain["INPUT"] = joutput;
                                    UpdateStatus(joutputmain.ToString(), filename);
                                    isdbstatusupdated = true;

                                }
                               

                            }
                        }


                    }
                    while (patherror);


                    Console.WriteLine("DATASOURCE:" + sourcename + " ENDED");
                    joutput = null;
                    engines = null;
                    pull = null;
                }
                Console.WriteLine("DATASOURCE ITERATIONS COMPLETED");
                conn.Close();
                Console.Read();
            }

            catch (Exception ex1)
            {


                Console.WriteLine("MAIN :" + ex1.Message);
                Logging.WriteToLog(LoggerEnum.ERROR, ex1.Message, "MainException");
                Console.Read();

            }


        }
        public static void UpdateStatus(string input,string filename)

        {
            try
            {
                Logging.WriteToLog(LoggerEnum.INFO, "UpdateStatus Start time : " + DateTime.Now, filename);
                Logging.WriteToLog(LoggerEnum.INFO, input, filename);
                NpgsqlConnection connus = new NpgsqlConnection(connString);
                connus.Open();
                string comm = "select usp_dbservgetsourcecommonurl('" + input.ToString() + "')";
                Logging.WriteToLog(LoggerEnum.INFO, comm, filename);
                NpgsqlCommand commandus = new NpgsqlCommand(comm, connus);
                object objResult1 = commandus.ExecuteScalar();
                string strresult2 = objResult1.ToString();
                Logging.WriteToLog(LoggerEnum.INFO, "OUTPUT" + strresult2, filename);
                connus.Close();
                connus = null;
                commandus.Dispose();
                commandus = null;
                Logging.WriteToLog(LoggerEnum.INFO, input, filename);
                Logging.WriteToLog(LoggerEnum.INFO, "UpdateStatus End time : " + DateTime.Now, filename);
            }
            catch (Exception exup)
            {
            }
        }

        public static string GetDatamartCount(string tableName, DateTime date,string filename)
        {
            
            Logging.WriteToLog(LoggerEnum.INFO, "GetDatamartCount Start time : " + DateTime.Now, filename);
           
            string query = "select count(1) from Fact_" + tableName + "metricdetails where recorddateutc>='" + date.ToString("yyyy-MM-dd") + "' and recorddateutc<='" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'";
            Logging.WriteToLog(LoggerEnum.INFO, query, filename);
            string result = string.Empty;
            try
            {
                if (datamartconn.State == ConnectionState.Closed)
                    datamartconn.Open();

                Console.WriteLine("SQL DATAMART CONN is CONNECTED");
                SqlCommand sqlCommand = new SqlCommand(query, datamartconn);
                object obj2 = sqlCommand.ExecuteScalar();
                result = obj2.ToString();
               // datamartconn.Close();
                sqlCommand.Dispose();
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
