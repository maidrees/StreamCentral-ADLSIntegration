using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.ObjectModel;
using System.Data.SqlClient;
using Microsoft.Azure.Management.DataLake.Store;
//using Microsoft.Azure.Management.DataFactories;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Rest.ClientRuntime;
using Microsoft.Rest.ClientRuntime.Azure.Authentication;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Npgsql;
using Newtonsoft.Json.Linq;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.Management.ResourceManager;

namespace StreamCentral.ADLSIntegration
{
    class ADFV2Operations
    {
        //// Set variables
        //public static string tenantID = "<tenant ID>";
        //public static string applicationId = "<Activity directory application ID>";
        //public static string authenticationKey = "<Activity directory application authentication key>";
        //public static string subscriptionId = "<subscription ID>";
        //public static string resourceGroup = "<resource group name>";

        //// Note that the data stores (Azure Storage, Azure SQL Database, etc.) and computes (HDInsight, etc.) used by data factory can be in other regions.
        //public static string region = "East US";
        //public static string dataFactoryName = "<name of the data factory>"; //must be globally unique

        //// Specify the source Azure Blob information
        //public static string storageAccount = "<name of Azure Storage account>";
        //public static string storageKey = "<key for your Azure Storage account>";
        public static string inputBlobPath = "adfv2tutorial/";
        public static string inputBlobName = "inputEmp.txt";

        //// Specify the sink Azure SQL Database information
        //public static string azureSqlConnString = "Server=tcp:<name of Azure SQL Server>.database.windows.net,1433;Database=spsqldb;User ID=spelluru;Password=Sowmya123;Trusted_Connection=False;Encrypt=True;Connection Timeout=30";
        public static string azureSqlTableName = "dbo.emp";

        public static string storageLinkedServiceName = "AzureStorageLinkedService";
        public static string sqlDbLinkedServiceName = "AzureSqlDbLinkedService";
        public static string blobDatasetName = "BlobDataset";
        public static string sqlDatasetName = "SqlDataset";
        public static string pipelineName = "Adfv2TutorialBlobToSqlCopy";


        public static DataFactoryManagementClient adfv2Client;

        static ADFV2Operations()
        {
            adfv2Client =  CreateManagementClientInstanceADFV2();
        }

        private static DataFactoryManagementClient CreateManagementClientInstanceADFV2()
        {
            //I // Authenticate and create a data factory management client
            var context = new AuthenticationContext("https://login.windows.net/" + AppSettingsManager.activeDirectoryTenantID);
            ClientCredential cc = new ClientCredential(AppSettingsManager.applicationId, AppSettingsManager.password);
            AuthenticationResult result = context.AcquireTokenAsync("https://management.azure.com/", cc).Result;
            ServiceClientCredentials cred = new TokenCredentials(result.AccessToken);
            adfv2Client = new DataFactoryManagementClient(cred) { SubscriptionId = AppSettingsManager.subscriptionId };

            return adfv2Client;
        }

        public static async Task<string> GetAuthorizationHeader()
        {
            AuthenticationContext context = new AuthenticationContext(AppSettingsManager.activeDirectoryEndPoint + AppSettingsManager.activeDirectoryTenantID);
            ClientCredential credential = new ClientCredential(
                AppSettingsManager.applicationId,
                AppSettingsManager.password);
            AuthenticationResult result = await context.AcquireTokenAsync(
                resource: AppSettingsManager.windowsManagementUri,
                clientCredential: credential);

            if (result != null)
                return result.AccessToken;

            throw new InvalidOperationException("Failed to acquire token");
        }

        static void DeployADFV2DataSetsAndPipelines()
        {
            
           
           

        }

        static void CreateOrUpdateADFV2InputDataSet()
        {
            // Create a Azure Blob dataset
            Console.WriteLine("Creating dataset " + blobDatasetName + "...");
            DatasetResource blobDataset = new DatasetResource(
                new AzureBlobDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = storageLinkedServiceName
                    },
                    FolderPath = inputBlobPath,
                    FileName = inputBlobName,
                    Format = new TextFormat { ColumnDelimiter = "|" },
                    Structure = new List<DatasetDataElement>
                    {
            new DatasetDataElement
            {
                Name = "FirstName",
                Type = "String"
            },
            new DatasetDataElement
            {
                Name = "LastName",
                Type = "String"
            }
                    }
                }
            );
            adfv2Client.Datasets.CreateOrUpdate(AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, blobDatasetName, blobDataset);
            Console.WriteLine(SafeJsonConvert.SerializeObject(blobDataset, adfv2Client.SerializationSettings));


        }

        static void CreateOrUpdateADFV2OutputDataSet()
        {
            // Create a Azure SQL Database dataset
            Console.WriteLine("Creating dataset " + sqlDatasetName + "...");
            DatasetResource sqlDataset = new DatasetResource(
                new AzureSqlTableDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = sqlDbLinkedServiceName
                    },
                    TableName = azureSqlTableName
                }
            );
            adfv2Client.Datasets.CreateOrUpdate(AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, sqlDatasetName, sqlDataset);
            Console.WriteLine(SafeJsonConvert.SerializeObject(sqlDataset, adfv2Client.SerializationSettings));


        }

        static void CreateOrUpdateADFv2Pipeline()
        {
            // Create a pipeline with copy activity
            Console.WriteLine("Creating pipeline " + pipelineName + "...");
            PipelineResource pipeline = new PipelineResource
            {
                Activities = new List<Activity>
                {
                    new CopyActivity
                    {
                        Name = "CopyFromBlobToSQL",
                        Inputs = new List<DatasetReference>
                        {
                            new DatasetReference()
                            {
                                ReferenceName = blobDatasetName
                            }
                        },
                        Outputs = new List<DatasetReference>
                        {
                            new DatasetReference
                            {
                                ReferenceName = sqlDatasetName
                            }
                        },
                        Source = new BlobSource { },
                        Sink = new SqlSink { }
                    }
                }
            };
            adfv2Client.Pipelines.CreateOrUpdate(AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, pipelineName, pipeline);
            Console.WriteLine(SafeJsonConvert.SerializeObject(pipeline, adfv2Client.SerializationSettings));
        }

        //Create Data Sets and Pipelines - Deploy these in Azure Data Factory for a single Structure.
        public static void DeployADFDataSetsAndPipelines(CopyOnPremSQLToADLAType cpType)
        {
            //check if there is any data in the source structure.
            DateTime firstDateTimeRecordInTable;
            string tableName = InitialParams.TableName;

            if (System.String.IsNullOrEmpty(InitialParams.TableName))
            {
                Console.WriteLine("source structure(SC table name) cannot be empty. Execution does not proceed further");
                return;
            }

            //List the attribute, data type of each column
            List<DatasetDataElement> lstElements = GenerateStructure(InitialParams.TableName);

            //if (System.String.IsNullOrEmpty(InitialParams.DataSourcePathInADLS))
            //{
            //    InitialParams.DataSourcePathInADLS = Utils.GetdataSourceType(tableName);
            //}


            string dateTimeField = (String.IsNullOrEmpty(InitialParams.FilterDateTimeField) ? "recorddateutc" : InitialParams.FilterDateTimeField);

            AppSettingsManager.folderPath = (String.IsNullOrEmpty(AppSettingsManager.folderPath) ? ConfigurationManager.AppSettings["folderPath"] : AppSettingsManager.folderPath);


            firstDateTimeRecordInTable = ADFV1Operations.FetchFirstRowRecordDate(InitialParams.TableName, InitialParams.FilterDateTimeField);



            if (firstDateTimeRecordInTable <= DateTime.Now.Subtract(TimeSpan.FromMinutes(5)))
            {
                //re: INPUT DATASET - Prepare the SQL query required for pipeline to execute on Source System

                firstDateTimeRecordInTable = firstDateTimeRecordInTable.AddHours(3);

                string sqlQuery = GenerateADFPipelineSQLQuery(lstElements, dateTimeField, false, cpType);

                string InOutDataSetNameRef = Utils.GetCustomizedInputOutRefName();

                Console.WriteLine("Deploying data sets and pipelines for Headers");

                string inDataSetName = Utils.GetCustomizedInputDataSetName(true);
                string outDataSetName = Utils.GetCustomizedOutputDataSetName(true);
                string pipelineName = Utils.GetCustomizedPipelineName(true);
                string activityName = Utils.GetCustomizedActivityName(true);
                string fileName = Utils.GetCustomizedFileName(true);
                string folderpath = Utils.GetCustomizedFolderPath();

                DeployDatasetAndPipelines(pipelineName, activityName, inDataSetName, outDataSetName, tableName,
                    lstElements, fileName, folderpath, sqlQuery, firstDateTimeRecordInTable, false, cpType);

                Console.WriteLine("Deployed data sets and pipelines for headers");

                //re: OUTPUT DATASET - Prepare the SQL query required for pipeline to execute on Source System
                sqlQuery = GenerateADFPipelineSQLQuery(lstElements, dateTimeField, true, cpType);

                Console.WriteLine("Deploying data sets and pipelines for data");

                inDataSetName = Utils.GetCustomizedInputDataSetName(false);
                outDataSetName = Utils.GetCustomizedOutputDataSetName(false);
                pipelineName = Utils.GetCustomizedPipelineName(false);
                activityName = Utils.GetCustomizedActivityName(false);
                fileName = Utils.GetCustomizedFileName(false);

                DeployDatasetAndPipelines(pipelineName, activityName, inDataSetName, outDataSetName, tableName,
                    lstElements, fileName, folderpath, sqlQuery, firstDateTimeRecordInTable, true, cpType);

                Console.WriteLine("Deployed data sets and pipelines for data");
            }
            else
            {
                Console.WriteLine("There are no records in a table OR data received in a table in less than 5 Minutes. Please try again after some time. table provisioning failed for : " + tableName);
            }
        }

        public static void DeployDatasetAndPipelines(string pipelineName, string activityName, string inDataSetName, string outDataSetName,
            string tableName, List<DatasetDataElement> lstElements, string fileName, string folderpath,
             string sqlQuery, DateTime recorddateUTC, bool IsDataDeploy, CopyOnPremSQLToADLAType cpType)
        {
            DatasetResource dsInput = null, dsOutput = null;

            try
            {
                DatasetResource respGetInDatasets = adfv2Client.Datasets.Get(AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, inDataSetName);
                dsInput = respGetInDatasets;
            }
            catch (Exception ex) { Console.WriteLine("Unable to find the Input dataset: Will look to create new : " + ex.Message); }

            if (dsInput == null && InitialParams.SourceStructureType == EnumSourceStructureType.Table)
            {
                //Create Input DataSet
                CreateOrUpdateInputDataSet(adfv2Client, AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, AppSettingsManager.linkedServiceNameSource,
                    inDataSetName, tableName, lstElements, IsDataDeploy);
            }

            try
            {
                //DatasetGetResponse respGetOutDatasets = adfv2Client.Datasets.Get(AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, outDataSetName);
                //dsOutput = respGetOutDatasets.Dataset;
            }
            catch (Exception ex) { Console.WriteLine("Unable to find the Output dataset: Will look to create new : " + ex.Message); }

            if (dsOutput == null)
            {
                //Create Output DataSet
                if (InitialParams.SourceStructureType.Equals(EnumSourceStructureType.Table))
                {
                    CreateOrUpdateOutputDataSet(adfv2Client, AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, AppSettingsManager.linkedServiceNameDestination,
                        outDataSetName, fileName, folderpath, lstElements, IsDataDeploy);
                }               
               
            }

            //Create Or Update Pipeline
            if (InitialParams.SourceStructureType.Equals(EnumSourceStructureType.Table))
            {
                CreateOrUpdatePipeline(adfv2Client, AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, AppSettingsManager.linkedServiceNameDestination, pipelineName,
                    inDataSetName, outDataSetName, sqlQuery, recorddateUTC, activityName, IsDataDeploy, cpType);
            }          
        }

        public static void CreateOrUpdateInputDataSet(DataFactoryManagementClient client, string resourceGroupName,
            string dataFactoryName, string linkedServiceName, string datasetSource, string sourceStructureName,
            List<DatasetDataElement> InputParams, bool isDataDeploy)
        {
            // create input datasets
            Console.WriteLine("Creating input dataset - " + datasetSource);


            try
            {
                DatasetResource objInputDataset = new DatasetResource(
                    new AzureSqlTableDataset
                    {
                        LinkedServiceName = new LinkedServiceReference
                        {
                            ReferenceName = linkedServiceName
                        },
                        TableName = sourceStructureName,
                        Structure = InputParams                       
                    }
                );

                adfv2Client.Datasets.CreateOrUpdate(AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, datasetSource, objInputDataset);
                Console.WriteLine(SafeJsonConvert.SerializeObject(objInputDataset, adfv2Client.SerializationSettings));  

                Console.WriteLine("Created input dataset - " + datasetSource);
            }
            catch (InvalidOperationException invalidToken)
            {
                if (invalidToken.Message.Contains("token") || invalidToken.Message.Contains("expir"))
                {
                    Console.WriteLine("Oops! Client Token Expired while creating Input Data set : " + invalidToken.Message);
                    client = CreateManagementClientInstanceADFV2();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Oops! Something went wrong in creating data set : " + ex.Message);
            }
        }


        public static void CreateOrUpdateOutputDataSet(DataFactoryManagementClient client, string resourceGroupName, string dataFactoryName,
           string linkedServiceName, string datasetDestination, string fileName, string folderPath, List<DatasetDataElement> InputParams, bool isDataDeploy)
        {
            // create output datasets
            Console.WriteLine(String.Format("Creating output dataset - {0} ", datasetDestination));

            bool isFirstRowHeader = false;

            if (!isDataDeploy)
            { isFirstRowHeader = true; }

            try
            {
                if (client == null)
                {
                    client = CreateManagementClientInstanceADFV2();
                }

                TextFormat objTextFormat = new TextFormat()
                {
                    RowDelimiter = "\n",
                    ColumnDelimiter = "~",
                    NullValue = String.Empty,
                    EncodingName = "utf-8",
                    SkipLineCount = 0,
                    FirstRowAsHeader = isFirstRowHeader,
                    TreatEmptyAsNull = true
                };


                DatasetResource objOutputDataset = new DatasetResource(
                   new AzureDataLakeStoreDataset
                   {
                       LinkedServiceName = new LinkedServiceReference
                       {
                           ReferenceName = linkedServiceName
                       },
                       Structure = InputParams,
                       Format = objTextFormat,
                       FolderPath = folderPath                       
                   }
                );
                
                adfv2Client.Datasets.CreateOrUpdate(AppSettingsManager.resourceGroupName, AppSettingsManager.dataFactoryName, datasetDestination, objOutputDataset );
                Console.WriteLine(SafeJsonConvert.SerializeObject(objOutputDataset, adfv2Client.SerializationSettings));


                // created output datasets
                Console.WriteLine(String.Format("Created output dataset - {0} ", datasetDestination));
            }
            catch (InvalidOperationException invalidToken)
            {
                if (invalidToken.Message.Contains("token") || invalidToken.Message.Contains("expir"))
                {
                    Console.WriteLine("Oops! Client Token Expired while creating output data set : ", invalidToken.Message);
                    adfv2Client = CreateManagementClientInstanceADFV2();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Generic Exception occured and couldnt handled: ", ex.Message);
            }
        }


        public static void CreateOrUpdatePipeline(DataFactoryManagementClient client, string resourceGroupName, string dataFactoryName,
            string linkedServiceName, string pipelineName, string dsInput, string dsOutput, string sqlQuery,
            DateTime recordDateUTC, string activityName, bool isDataDeploy, CopyOnPremSQLToADLAType cpType)
        {
            if (String.IsNullOrEmpty(sqlQuery))
            {
                Console.WriteLine("Cannot create pipeline. Empty SQL Query");
                return;
            }

            try
            {
                DateTime PipelineActivePeriodStartTime = DateTime.Now.Subtract(TimeSpan.FromDays(1000.00));
                DateTime PipelineActivePeriodEndTime = PipelineActivePeriodStartTime;
                TimeSpan objActivityDelayPolicyAttr = TimeSpan.FromMinutes(Convert.ToDouble(InitialParams.DelayIntervalOfActivity));
                

                if ((isDataDeploy))// && (!cpType.ToString().Equals(CopyOnPremSQLToADLAType.All.ToString())))
                {
                    recordDateUTC.AddHours(3);
                    PipelineActivePeriodStartTime = recordDateUTC;
                    PipelineActivePeriodEndTime = recordDateUTC.AddYears(100);
                }

                //Scheduler objActivityScheduler = new Scheduler()
                //{
                //    Interval = Convert.ToUInt16(InitialParams.ActivityFrequencyInterval)
                //};


                //if (isDataDeploy)
                //{
                //    objActivityScheduler.Offset = TimeSpan.FromMinutes(Convert.ToDouble(InitialParams.OffsetIntervalOfDataSlice));
                //}

                //if (InitialParams.SliceType == SliceType.Start && isDataDeploy)
                //{
                //    objActivityScheduler.Style = SchedulerStyle.StartOfInterval;
                //}

                //switch (InitialParams.ActivityFrequencyType)
                //{
                //    case Frequency.Month:
                //        {
                //            objActivityScheduler.Frequency = SchedulePeriod.Month;
                //            break;
                //        }
                //    case Frequency.Day:
                //        {
                //            objActivityScheduler.Frequency = SchedulePeriod.Day;
                //            break;
                //        }
                //    case Frequency.Hour:
                //        {
                //            objActivityScheduler.Frequency = SchedulePeriod.Hour;
                //            break;
                //        }
                //    case Frequency.Minute:
                //        {
                //            objActivityScheduler.Frequency = SchedulePeriod.Minute;
                //            break;
                //        }
                //    default:
                //        {
                //            objActivityScheduler.Frequency = SchedulePeriod.Day;
                //            break;
                //        }

                //}

                //if (client == null)
                //{
                //    client = CreateManagementClientInstance();
                //}

                //Activity activityInPipeline = new Activity()
                //{
                //    Name = activityName,

                //    Inputs = new List<ActivityInput>() { new ActivityInput() { Name = dsInput } },

                //    Outputs = new List<ActivityOutput>() { new ActivityOutput() { Name = dsOutput } },

                //    TypeProperties = new CopyActivity()
                //    {
                //        Source = new SqlSource() { SqlReaderQuery = sqlQuery },
                //        Sink = new AzureDataLakeStoreSink()
                //        {
                //            WriteBatchSize = 0,
                //            WriteBatchTimeout = TimeSpan.FromMinutes(0)
                //        }
                //    },

                    //Policy = new ActivityPolicy()
                    //{
                    //    Timeout = TimeSpan.FromMinutes(3.0),
                    //    Delay = objActivityDelayPolicyAttr,
                    //    Concurrency = 1,
                    //    ExecutionPriorityOrder = ExecutionPriorityOrder.NewestFirst,
                    //    LongRetry = 0,
                    //    LongRetryInterval = TimeSpan.FromMinutes(0.0),
                    //    Retry = 3

                    //},

               // };


            }
            catch (Exception ex)
            {
                Console.WriteLine("pipeline " + ex.Message);
            }
        }      



        //public static Availability GetFormattedAvailabilityInstance(bool isDataDeploy)
        //{

        //    TimeSpan ObjOffsetTimeSpan = TimeSpan.FromMinutes(0);

        //    if (isDataDeploy)
        //    {
        //        ObjOffsetTimeSpan = TimeSpan.FromMinutes(Convert.ToDouble(InitialParams.OffsetIntervalOfDataSlice));
        //    }

        //    Availability objAvailability = new Availability();

        //    if (InitialParams.SliceType == SliceType.Start && isDataDeploy)
        //    {
        //        objAvailability.Style = SchedulerStyle.StartOfInterval;
        //    }

        //    objAvailability.Interval = Convert.ToUInt16(InitialParams.ActivityFrequencyInterval);

        //    objAvailability.Offset = ObjOffsetTimeSpan;

        //    try
        //    {
        //        switch (InitialParams.ActivityFrequencyType)
        //        {
        //            case Frequency.Month:
        //                {
        //                    objAvailability.Frequency = Frequency.Month.ToString();
        //                    break;
        //                }
        //            case Frequency.Day:
        //                {
        //                    objAvailability.Frequency = Frequency.Day.ToString();
        //                    break;
        //                }
        //            case Frequency.Hour:
        //                {
        //                    objAvailability.Frequency = Frequency.Hour.ToString();
        //                    break;
        //                }
        //            case Frequency.Minute:
        //                {
        //                    objAvailability.Frequency = Frequency.Minute.ToString();
        //                    break;
        //                }
        //            default:
        //                {
        //                    objAvailability.Frequency = Frequency.Day.ToString();
        //                    break;
        //                }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //    return objAvailability;
        //}

        //public static Collection<Partition> GetFormattedPartitionCollectionInstance()
        //{
        //    return new Collection<Partition>()
        //        {
        //        new Partition()
        //        {
        //            Name = "year",
        //            Value = new DateTimePartitionValue()
        //            {
        //                Date = "SliceEnd",
        //                Format  = "yyyy"
        //            }
        //        },
        //        new Partition()
        //        {
        //            Name = "month",
        //            Value = new DateTimePartitionValue()
        //            {
        //                Date = "SliceEnd",
        //                Format  = "MM"
        //            }
        //        },
        //        new Partition()
        //        {
        //            Name = "day",
        //            Value = new DateTimePartitionValue()
        //            {
        //                Date = "SliceEnd",
        //                Format  = "dd"
        //            }
        //        },
        //        new Partition()
        //        {
        //            Name = "hour",
        //            Value = new DateTimePartitionValue()
        //            {
        //                Date="SliceEnd",
        //                Format = "HH"
        //            }

        //        },
        //        new Partition()
        //        {
        //            Name = "minute",
        //            Value = new DateTimePartitionValue()
        //            {
        //                Date = "SliceEnd",
        //                Format = "mm"
        //            }
        //        }
        //    };


        //}

        public static string GenerateADFPipelineSQLQuery(List<DatasetDataElement> inOutParams, string dateField,
            bool isDataQuery, CopyOnPremSQLToADLAType copyDataType)
        {
            string sqlQuery = "$$Text.Format('select ";
            string tableName = InitialParams.TableName;
            string paramsQuery = String.Empty;

            int itemIteration = 0;
            foreach (var columnName in inOutParams)
            {
                if (itemIteration < inOutParams.Count - 1)
                {
                    paramsQuery = paramsQuery + "[" + columnName.Name + "],";
                    itemIteration = itemIteration + 1;
                }
                else
                {
                    paramsQuery = paramsQuery + "[" + columnName.Name + "]";
                }

            }

            if (!System.String.IsNullOrEmpty(paramsQuery))
            {
                sqlQuery = sqlQuery + paramsQuery;
            }

            //if (!(isDataQuery && copyDataType.ToString() == CopyOnPremSQLToADLAType.All.ToString()))
            if (!(isDataQuery))
            {
                sqlQuery = sqlQuery + " from " + tableName + " where [" + dateField + "] >= \\'{0:yyyy-MM-dd HH:mm}\\' AND  " +
              " [" + dateField + "] < \\'{1:yyyy-MM-dd HH:mm}\\'', " +
              "WindowStart, WindowEnd)";

                return sqlQuery;
            }

            switch (copyDataType)
            {
                case CopyOnPremSQLToADLAType.LastIteration:
                    {
                        sqlQuery = sqlQuery + " from " + tableName + " where " +
                            " CONVERT(varchar, RECORDDATEUTC, 101) = (select max(CONVERT(varchar, RECORDDATEUTC, 101)) from " +
                            tableName + ")')";
                        break;
                    }
                case CopyOnPremSQLToADLAType.All:
                    {
                        sqlQuery = sqlQuery + " from " + tableName + "')";
                        break;
                    }
                case CopyOnPremSQLToADLAType.Distinct:
                    {
                        if (!String.IsNullOrEmpty(InitialParams.PrimaryKey))
                        {
                            sqlQuery = sqlQuery + " from (Select " + paramsQuery + ",ROW_NUMBER() " +
                            " over(partition by " + InitialParams.PrimaryKey + " order by " +
                            dateField + " desc) pk from " + tableName + ") dat where pk = 1')";
                        }

                        break;
                    }
                case CopyOnPremSQLToADLAType.Transactional:
                    {
                        sqlQuery = sqlQuery + " from " + tableName + " where [" + dateField + "] >= \\'{0:yyyy-MM-dd HH:mm}\\' AND  " +
               " [" + dateField + "] < \\'{1:yyyy-MM-dd HH:mm}\\'', " +
               "WindowStart, WindowEnd)";
                        break;
                    }
            }

            return sqlQuery;
        }

        public static List<string> ListFilteredTableNames()
        {
            String[] searchTexts = new String[2];
            return ListFilteredTableNames(searchTexts);
        }

        public static List<string> ListFilteredTableNames(String[] searchTexts)
        {
            List<string> tableNames = new List<string>();

            string searchText01 = String.Empty; string searchText02 = String.Empty; string searchText03 = String.Empty;

            SqlConnection connect = SQLUtils.SQLConnect();

            try
            {
                searchText01 = searchTexts[0];
                searchText02 = searchTexts[1];
                searchText03 = searchTexts[2];
            }
            catch (Exception ex)
            {
                Console.WriteLine("Seems like not all search parameters are passed: {0}", ex.Message);
            }

            SqlCommand cmd = SQLUtils.GenerateStoredProcCommand("SCDMListTablesProc", searchText01, searchText02, searchText03);

            if (connect.State == ConnectionState.Closed)
            {
                connect.Open();
            }

            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        tableNames.Add(reader.GetString(0));
                    }
                    reader.Close();
                }
            }

            return tableNames;
        }

        public static DateTime FetchFirstRowRecordDate(string tableName, string dateField)
        {
            string sqlQuery = String.Empty;
            DateTime firstDateTime = DateTime.Now;

            if (!System.String.IsNullOrEmpty(dateField))
            {
                sqlQuery = String.Format("SELECT TOP 1 {0} FROM {1} WHERE {2} IS NOT NULL ORDER BY {3} ASC", dateField, tableName, dateField, dateField);
            }
            else
            {
                sqlQuery = String.Format("SELECT TOP 1 RECORDDATEUTC FROM {0} WHERE RECORDDATEUTC IS NOT NULL ORDER BY RECORDDATEUTC ASC", tableName);
            }

            SqlConnection connect = SQLUtils.SQLConnect();

            SqlCommand cmd = SQLUtils.GenerateSQLQueryCommand(sqlQuery);

            if (connect.State == ConnectionState.Closed)
            {
                connect.Open();
            }

            try
            {
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            firstDateTime = reader.GetDateTime(0).Date.Subtract(TimeSpan.FromDays(1));
                        }
                        reader.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to execute the sql command: {0}, Exception occured: {1}", cmd.CommandText, ex.Message);
            }

            return firstDateTime;
        }

        public static List<DatasetDataElement> GenerateStructure(string tableName)
        {
            try
            {
                List<DatasetDataElement> InOutParams = new List<DatasetDataElement>();

                //// Look for the name in the connectionStrings section.
                SqlConnection connect = SQLUtils.SQLConnect();

                // SqlCommand cmd = SQLUtils.GenerateStoredProcCommand("SCDMTableSchemaProc", tableName);
                SqlCommand cmd = new SqlCommand(ConfigurationManager.AppSettings["SCDMTableSchemaProc"], connect)
                {
                    CommandType = CommandType.StoredProcedure
                };

                cmd.Parameters.Add(new SqlParameter("@TableName", tableName));

                if (connect.State == ConnectionState.Closed)
                {
                    connect.Open();
                }

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var name = reader.GetString(0);
                            var type = reader.GetString(1);

                            if (!(name == "RecordId" || name == "Seq" || name == "Call_UID" || name == "CreatedBy" ||
                                name == "CreatedDate" || name == "ModifiedBy" || name == "ModifiedDate" ||
                                name == "LOCATIONDIMENSIONID" || name == "EntityId" || name == "EntityLocationId" ||
                                name == "operating_id" || name == "source_id" || (type.Contains("image"))))
                            {

                                //converting SQL Server datatypes to ADF data types
                                switch (type)
                                {
                                    case "varchar":
                                        type = "String";
                                        break;
                                    case "nvarchar":
                                        type = "String";
                                        break;
                                    case "int":
                                        type = "Int32";
                                        break;
                                    case "bigint":
                                        type = "Int32";
                                        break;
                                    case "datetime":
                                        type = "DateTime";
                                        break;
                                    case "time":
                                        type = "DateTime";
                                        break;
                                    case "nchar":
                                        type = "String";
                                        break;
                                }

                                InOutParams.Add(new DatasetDataElement
                                {
                                    Name = name,
                                    Type = type
                                });
                            }
                        }
                        reader.Close();
                        connect.Close();
                    }
                }
                return InOutParams;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

    }
}
