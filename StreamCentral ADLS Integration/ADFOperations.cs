using System;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.ObjectModel;
using System.Data.SqlClient;

using Microsoft.Azure;
using Microsoft.Azure.Management.DataFactories;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Common.Models;

using Microsoft.IdentityModel.Clients.ActiveDirectory;


namespace StreamCentral.ADLSIntegration
{
    public class ADFOperations
    {

        //IMPORTANT: specify the name of Azure resource group here
        public static string resourceGroupName = ConfigurationSettings.AppSettings["resourceGroupName"];

        //IMPORTANT: the name of the data factory must be globally unique.
        public static string dataFactoryName = ConfigurationSettings.AppSettings["dataFactoryName"];

        //IMPORTANT: specify the name of the source linked Service
        public static string linkedServiceNameSource = ConfigurationSettings.AppSettings["linkedServiceNameSource"];

        //IMPORTANT: specify the name of the destination linked Service. These linked services have already been created in our SC - scenario.
        public static string linkedServiceNameDestination = ConfigurationSettings.AppSettings["linkedServiceNameDestination"];

        //IMPORTANT: specify the CGS ADF Hub Name
        public static string cgsHubName = ConfigurationSettings.AppSettings["cgsHubName"];

        //IMPORTANT: specify the name of the main container of the Destination Lake Store.
        public static string folderPath = ConfigurationSettings.AppSettings["folderPath"];

        public static string dataSourceType = string.Empty;
        
        public static DataFactoryManagementClient client;

        static ADFOperations()
        {
            //IMPORTANT: generate security token for the subsciption and AAD App
            TokenCloudCredentials aadTokenCredentials = new TokenCloudCredentials(ConfigurationSettings.AppSettings["SubscriptionId"],
                    GetAuthorizationHeader().Result);

            Uri resourceManagerUri = new Uri(ConfigurationSettings.AppSettings["ResourceManagerEndpoint"]);

            // create data factory management client
            client = new DataFactoryManagementClient(aadTokenCredentials, resourceManagerUri);

        }

        public static string getClientToken()
        {
            string token = string.Empty;

            //IMPORTANT: generate security token for the subsciption and AAD App
            TokenCloudCredentials aadTokenCredentials = new TokenCloudCredentials(ConfigurationSettings.AppSettings["SubscriptionId"],
                    GetAuthorizationHeader().Result);

            return aadTokenCredentials.Token;
        }

        //Create Data Sets and Pipelines - Deploy these in Azure Data Factory for a all structures in the source system.
        public static void DeployADFDataSetsAndPipelines()
        {

            List<string> tableNames = ADFOperations.ListFilteredTableNames();

            foreach (string tableName in tableNames)
            {
                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty,CopyDataType.All);

                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty, CopyDataType.Transactional);
            }
        }


        //Create Data Sets and Pipelines - Deploy these in Azure Data Factory for a all structures in the source system.
        public static void DeployADFDataSetsAndPipelines(string [] searchTexts)
        {
            List<string> tableNames = ADFOperations.ListFilteredTableNames(searchTexts);

            foreach (string tableName in tableNames)
            {
                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty, CopyDataType.All);

                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty, CopyDataType.Transactional);
            }
        }

        //Create Data Sets and Pipelines - Deploy these in Azure Data Factory for a single Structure.
        public static void DeployADFDataSetsAndPipelines(string dataSourceName, string tableName, 
            string folderPath, string dateTimeField, string interval, CopyDataType cpType)
        {
            //check if there is any data in the source structure.
            DateTime firstDateTimeRecordInTable;

            if(System.String.IsNullOrEmpty(tableName))
            {
                Console.WriteLine("source structure(SC table name) cannot be empty. Execution does not proceed further");
                return;
            }

            //List the attribute, data type of each column
            List<DataElement> lstElements = ADFOperations.GenerateStructure(tableName);

            //if (System.String.IsNullOrEmpty(dataSourceName))
            //{
                dataSourceType = Utils.GetdataSourceType(tableName);
            //}

            dateTimeField = (String.IsNullOrEmpty(dateTimeField) ? "recorddateutc" : dateTimeField);
            folderPath = (String.IsNullOrEmpty(folderPath) ? ConfigurationSettings.AppSettings["folderPath"] : folderPath);
            

            firstDateTimeRecordInTable = ADFOperations.FetchFirstRowRecordDate(tableName, dateTimeField);

            if (firstDateTimeRecordInTable <= DateTime.Now.Subtract(TimeSpan.FromHours(1)))
            {
                //re: INPUT DATASET - Prepare the SQL query required for pipeline to execute on Source System

                string sqlQuery = ADFOperations.GenerateADFPipelineSQLQuery(tableName, lstElements, dateTimeField, false, cpType);

                string InOutDataSetNameRef = tableName.Replace("[", String.Empty).Replace("]", String.Empty).
                    Replace("FACT_", String.Empty).Replace("metricdetails", String.Empty) + "_" + cpType.ToString();
                  //  "_" + DateTime.Now.Year.ToString() + DateTime.Now.Month.ToString() + DateTime.Now.Day.ToString(); ;


                Console.WriteLine("Deploying data sets and pipelines for Headers");

                string inDataSetName = "SC_DSI_H_" + InOutDataSetNameRef;                    
                string outDataSetName = "SC_DSO_H_" + InOutDataSetNameRef; 
                string pipelineName = "SC_PL01_Staging_H_" + dataSourceType + "_" + cpType.ToString();
                string fileName = "Header_" + InOutDataSetNameRef;
                string folderpath = folderPath + "/DL-" + dataSourceType + "/" + InOutDataSetNameRef;

                DeployDatasetAndPipelines(pipelineName, inDataSetName, outDataSetName, tableName,
                    lstElements, fileName, folderpath, sqlQuery, firstDateTimeRecordInTable, false,cpType);

                Console.WriteLine("Deployed data sets and pipelines for  headers");
                
                //re: OUTPUT DATASET - Prepare the SQL query required for pipeline to execute on Source System

                sqlQuery = ADFOperations.GenerateADFPipelineSQLQuery(tableName, lstElements, dateTimeField, true,cpType);

                Console.WriteLine("Deploying data sets and pipelines for data");

                inDataSetName = "SC_DSI_D_" + InOutDataSetNameRef;
                outDataSetName = "SC_DSO_D_" + InOutDataSetNameRef;
                pipelineName = "SC_PL01_Staging_D_" + dataSourceType + "_" + cpType.ToString();
                fileName = "Data_" + InOutDataSetNameRef + "-{year}-{month}-{day}";

                
                DeployDatasetAndPipelines(pipelineName, inDataSetName, outDataSetName, tableName, 
                    lstElements, fileName, folderpath, sqlQuery, firstDateTimeRecordInTable, true,cpType);

                Console.WriteLine("Deployed data sets and pipelines for data");
            }
            else
            {
                Console.WriteLine(" Empty record date UTC for table : " + tableName);
            }
        }

        public static void DeployDatasetAndPipelines(string pipelineName, string inDataSetName, string outDataSetName, 
            string tableName, List<DataElement> lstElements, string fileName, string folderpath,
             string sqlQuery, DateTime recorddateUTC, bool IsDataDeploy,CopyDataType cpType)
        {
            Dataset dsInput = null, dsOutput = null;

            try
            {

                DatasetGetResponse respGetInDatasets = client.Datasets.Get(resourceGroupName, dataFactoryName, inDataSetName);
                dsInput = respGetInDatasets.Dataset;
            }
            catch (Exception ex) { }

            if (dsInput == null)
            {
                //Create Input DataSet
                CreateOrUpdateInputDataSet(client, resourceGroupName, dataFactoryName, linkedServiceNameSource,
                    inDataSetName, tableName, lstElements);
            }

            try
            {
                DatasetGetResponse respGetOutDatasets = client.Datasets.Get(resourceGroupName, dataFactoryName, outDataSetName);
                dsOutput = respGetOutDatasets.Dataset;
            }
            catch (Exception ex) { }

            if (dsOutput == null)
            {
                //Create Output DataSet
                CreateOrUpdateOutputDataSet(client, resourceGroupName, dataFactoryName, linkedServiceNameDestination,
                    outDataSetName, fileName, folderpath, lstElements, IsDataDeploy);
            }

            //Create Or Update Pipeline
            CreateOrUpdatePipeline(client, resourceGroupName, dataFactoryName, linkedServiceNameDestination, pipelineName,
                inDataSetName, outDataSetName, sqlQuery, recorddateUTC, IsDataDeploy,cpType);

        }

        public static async Task<string> GetAuthorizationHeader()
        {
            AuthenticationContext context = new AuthenticationContext(ConfigurationSettings.AppSettings["ActiveDirectoryEndpoint"] + ConfigurationSettings.AppSettings["ActiveDirectoryTenantId"]);
            ClientCredential credential = new ClientCredential(
                ConfigurationSettings.AppSettings["ApplicationId"],
                ConfigurationSettings.AppSettings["Password"]);
            AuthenticationResult result = await context.AcquireTokenAsync(
                resource: ConfigurationSettings.AppSettings["WindowsManagementUri"],
                clientCredential: credential);

            if (result != null)
                return result.AccessToken;

            throw new InvalidOperationException("Failed to acquire token");
        }

        public static void CreateDataFactory(DataFactoryManagementClient clc, string dataFactoryName, string resourceGroupName)
        {
            // create a data factory
            Console.WriteLine("Creating a data factory");
            clc.DataFactories.CreateOrUpdate(resourceGroupName,
                new DataFactoryCreateOrUpdateParameters()
                {
                    DataFactory = new DataFactory()
                    {
                        Name = dataFactoryName,
                        Location = "eastus",
                        Properties = new DataFactoryProperties()
                    }
                }
            );
        }

        public static void CreateLinkedServices(DataFactoryManagementClient client, string
            resourceGroupName, string dataFactoryName)
        {
            // create a linked service for input data store: Azure Storage
            Console.WriteLine("Creating Azure Storage linked service");
            client.LinkedServices.CreateOrUpdate(resourceGroupName, dataFactoryName,
                new LinkedServiceCreateOrUpdateParameters()
                {
                    LinkedService = new LinkedService()
                    {
                        Name = "AzureStorageLinkedService",
                        Properties = new LinkedServiceProperties
                        (
                            new AzureStorageLinkedService("DefaultEndpointsProtocol=https;AccountName=<storageaccountname>;AccountKey=<accountkey>")
                        )
                    }
                }
            );
        }

        public static void CreateOrUpdateInputDataSet(DataFactoryManagementClient client, string resourceGroupName,
            string dataFactoryName, string linkedServiceName, string datasetSource, string sourceStructureName, List<DataElement> InputParams)
        {
            // create input datasets
            Console.WriteLine("Creating input dataset - " + datasetSource);

            try
            {
                client.Datasets.CreateOrUpdate(resourceGroupName, dataFactoryName,
                new DatasetCreateOrUpdateParameters()
                {
                    Dataset = new Dataset()
                    {
                        Name = datasetSource,

                        Properties = new DatasetProperties()
                        {
                            LinkedServiceName = linkedServiceName,

                            TypeProperties = new SqlServerTableDataset()
                            {
                                TableName = sourceStructureName
                            },

                            Structure = InputParams,
                            External = true,
                            Availability = new Availability()
                            {
                                Frequency = "Day",
                                Interval = 1,
                            },

                            Policy = new Policy()
                            {
                                Validation = new ValidationPolicy()
                                {
                                    MinimumRows = 2
                                }
                            },

                        },
                    }
                });

                // create input dataset
                Console.WriteLine("Created input dataset - " + datasetSource);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Oops! Something went wrong in creating data set" + ex.Message);
            }
        }


        public static void CreateOrUpdateOutputDataSet(DataFactoryManagementClient client, string resourceGroupName, string dataFactoryName,
           string linkedServiceName, string datasetDestination, string fileName, string folderPath, List<DataElement> InputParams, bool isDataDeploy)
        {
            bool isFirstRowHeader = false;

            if (!isDataDeploy)
            { isFirstRowHeader = true; }
                        
            client.Datasets.CreateOrUpdate(resourceGroupName, dataFactoryName,
            new DatasetCreateOrUpdateParameters()
            {
                Dataset = new Dataset()
                {
                      Name = datasetDestination,
                      Properties = new DatasetProperties()
                      {

                          LinkedServiceName = linkedServiceName,
                          Structure = InputParams,

                          TypeProperties = new AzureDataLakeStoreDataset()
                          {
                              FileName = fileName,

                              FolderPath = folderPath,

                              Format = new TextFormat()
                              {
                                  RowDelimiter = "\n",
                                  ColumnDelimiter = "~",
                                  NullValue = null,
                                  EncodingName = "utf-8",
                                  SkipLineCount = 0,
                                  FirstRowAsHeader = isFirstRowHeader,
                                  TreatEmptyAsNull = true
                              },

                              PartitionedBy = new Collection<Partition>()
                              {
                                    new Partition()
                                    {
                                        Name = "year",
                                        Value = new DateTimePartitionValue()
                                        {
                                            Date = "SliceStart",
                                            Format  = "yyyy"
                                        }
                                    },
                                    new Partition()
                                    {
                                        Name = "month",
                                        Value = new DateTimePartitionValue()
                                        {
                                            Date = "SliceStart",
                                            Format  = "MM"
                                        }
                                    },
                                    new Partition()
                                    {
                                        Name = "day",
                                        Value = new DateTimePartitionValue()
                                        {
                                            Date = "SliceStart",
                                            Format  = "dd"
                                        }
                                    }
                              },
                          },

                          Availability = new Availability()
                          {
                              Frequency = SchedulePeriod.Day,
                              Interval = 1,
                              Style = SchedulerStyle.StartOfInterval
                          },

                          External = false,

                          Policy = new Policy()
                          {
                              Validation = new ValidationPolicy()
                              {
                                  MinimumRows = 2
                              }
                          }
                      }
                  }
            });
        }

        public static bool DeleteDatasets(string startWithSearchText)
        {
            try
            {
                string nextLink = string.Empty;

                DatasetListResponse respListDatasets = (DatasetListResponse)client.Datasets.List(resourceGroupName, dataFactoryName);

                do
                {
                    nextLink = respListDatasets.NextLink;

                    foreach (var ds in respListDatasets.Datasets)
                    {
                        DeleteDataset(ds.Name, startWithSearchText);
                    }

                    respListDatasets = client.Datasets.ListNext(nextLink);

                } while (!String.IsNullOrEmpty(nextLink));


                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("unable to delete the data set: " + ex.Message);
            }
            return false;
        }

        public static void DeleteDataset(string datasetName, string startWithSearchText)
        {
            try
            {
                if (datasetName.StartsWith(startWithSearchText))
                {
                    Console.WriteLine("Deleting data set: " + datasetName);

                    client.Datasets.Delete(resourceGroupName, dataFactoryName, datasetName);

                    Console.WriteLine("Deleted data set: " + datasetName);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("unable to delete the dataset: " + ex.Message);
            }
        }

        public static void DeletePipelines(string startWithSearchText)
        {
            try
            {
                PipelineListResponse respListPipelines = (PipelineListResponse)client.Pipelines.List(resourceGroupName, dataFactoryName);

                foreach (var pl in respListPipelines.Pipelines)
                {
                    DeletePipeline(pl.Name, startWithSearchText);
                }
            }
            catch (Exception ex)
            {

            }
        }

        public static void DeletePipeline(string pipelineName, string startWithSearchText)
        {
            if (pipelineName.StartsWith(startWithSearchText))
            {
                Console.WriteLine("Deleting pipeline: " + pipelineName);

                client.Pipelines.Delete(resourceGroupName, dataFactoryName, pipelineName);

                Console.WriteLine("Deleted pipeline: " + pipelineName);
            }
        }

        public static void CreateOrUpdatePipeline(DataFactoryManagementClient client, string resourceGroupName, string dataFactoryName,
            string linkedServiceName, string pipelineName, string dsInput, string dsOutput, string sqlQuery,
            DateTime recordDateUTC, bool isDataDeploy,CopyDataType cpType)
        {
            DateTime PipelineActivePeriodStartTime = DateTime.Now.Subtract(TimeSpan.FromDays(1000.00));
            DateTime PipelineActivePeriodEndTime = PipelineActivePeriodStartTime;
            string mode = PipelineMode.OneTime;

            if ((isDataDeploy) && (!cpType.ToString().Equals(CopyDataType.All.ToString())))
            {
                PipelineActivePeriodStartTime = recordDateUTC;
                PipelineActivePeriodEndTime = recordDateUTC.AddYears(100);
                mode = PipelineMode.Scheduled;
            }
         

            Activity activityInPipeline = new Activity()
            {
                Name = "Act_" + dsInput,

                Inputs = new List<ActivityInput>() { new ActivityInput() { Name = dsInput } },

                Outputs = new List<ActivityOutput>() { new ActivityOutput() { Name = dsOutput } },

                TypeProperties = new CopyActivity()
                {
                    Source = new SqlSource() { SqlReaderQuery = sqlQuery },
                    Sink = new AzureDataLakeStoreSink()
                    {
                        WriteBatchSize = 0,
                        WriteBatchTimeout = TimeSpan.FromMinutes(0)
                    }
                },

                Policy = new ActivityPolicy()
                {
                    Timeout = TimeSpan.FromMinutes(1.0),
                    Concurrency = 1,
                    ExecutionPriorityOrder = ExecutionPriorityOrder.NewestFirst,
                    LongRetry = 0,
                    LongRetryInterval = TimeSpan.FromMinutes(0.0),
                    Retry = 3,
                    Delay = TimeSpan.FromMinutes(0.0)                   
                },
                Scheduler = new Scheduler()
                {
                    Frequency = "Day",
                    Interval = 1,
                    Style = SchedulerStyle.StartOfInterval

                }
                
            };

            Pipeline pl = null;

            try
            {
                PipelineGetResponse respPipelines = client.Pipelines.Get(resourceGroupName, dataFactoryName, pipelineName);
                pl = respPipelines.Pipeline;

                Console.WriteLine("updating existing pipeline " + pipelineName + " ... " + activityInPipeline.Name);
                
                pl.Properties.Activities.Add(activityInPipeline);

                PipelineCreateOrUpdateParameters plParameters = new PipelineCreateOrUpdateParameters() { Pipeline = pl };

                client.Pipelines.CreateOrUpdate(resourceGroupName, dataFactoryName, plParameters);

                Console.WriteLine("updated successfully existing pipeline: " + pipelineName + " ... " + activityInPipeline.Name);
            }
            catch (Exception ex)
            {
            }

            if (pl == null)
            {
                // Create a pipeline with a copy activity
                Console.WriteLine("Creating new pipeline " + pipelineName + " ... " + activityInPipeline.Name);

                client.Pipelines.CreateOrUpdate(resourceGroupName, dataFactoryName,
                    new PipelineCreateOrUpdateParameters()
                    {
                        Pipeline = new Pipeline()
                        {
                            Name = pipelineName,
                            Properties = new PipelineProperties()
                            {
                                Description = "Demo Pipeline for data transfer from on-premise SC - SQL Server Datamart to Lake Store",

                                Activities = new List<Activity>()
                                {
                                        activityInPipeline
                                },

                                // Initial value for pipeline's active period. With this, you won't need to set slice status
                                Start = PipelineActivePeriodStartTime,
                                End = PipelineActivePeriodEndTime,
                                IsPaused = false,
                                PipelineMode = mode,
                                HubName = cgsHubName,
                                ExpirationTime = TimeSpan.FromMinutes(0)
                            },
                        }
                    });

                // Create a pipeline with a copy activity
                Console.WriteLine("created new pipeline " + pipelineName + " ... " + activityInPipeline.Name);
            }
        }
        
        public static List<DataElement> GenerateStructure(string tableName)
        {
            List<DataElement> InOutParams = new List<DataElement>();

            //// Look for the name in the connectionStrings section.
            SqlConnection connect = SQLUtils.SQLConnect();

            // SqlCommand cmd = SQLUtils.GenerateStoredProcCommand("SCDMTableSchemaProc", tableName);
            SqlCommand cmd = new SqlCommand(ConfigurationSettings.AppSettings["SCDMTableSchemaProc"], connect);
            cmd.CommandType = CommandType.StoredProcedure;
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
                            name == "operating_id" || name == "source_id"))
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
                            }

                            InOutParams.Add(new DataElement
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

        public static string GenerateADFPipelineSQLQuery(string tableName, 
            List<DataElement> inOutParams, string dateField, bool isDataQuery, CopyDataType copyDataType)
        {
            string sqlQuery = "$$Text.Format('select ";

            int itemIteration = 0;
            foreach (var columnName in inOutParams)
            {
                if (itemIteration < inOutParams.Count - 1)
                {
                    sqlQuery = sqlQuery + "[" + columnName.Name + "],";
                    itemIteration = itemIteration + 1;
                }
                else
                {
                    sqlQuery = sqlQuery + "[" + columnName.Name + "]";
                }

            }

            if(!(isDataQuery && copyDataType.ToString() == CopyDataType.All.ToString()))
            {
                sqlQuery = sqlQuery + " from " + tableName + " where [" + dateField + "] >= \\'{0:yyyy-MM-dd HH:mm}\\' AND  " +
              " [" + dateField + "] < \\'{1:yyyy-MM-dd HH:mm}\\'', " +
              "WindowStart, WindowEnd)";

                return sqlQuery;
            }

            switch (copyDataType)
            {
                case CopyDataType.LastIteration:
                    {

                        break;
                    }
                case CopyDataType.All:
                    {
                        sqlQuery = sqlQuery + " from " + tableName + "')";
                        break;
                    }
                case CopyDataType.Distinct:
                    {
                        break;
                    }
                case CopyDataType.Transactional:
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

        public static List<string> ListFilteredTableNames(String [] searchTexts)
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
                sqlQuery = String.Format("SELECT TOP 1 {0} FROM {1} ORDER BY {2} ASC", dateField, tableName, dateField);
            }
            else
            {
                sqlQuery = String.Format("SELECT TOP 1 RECORDDATEUTC FROM {0} ORDER BY RECORDDATEUTC ASC", tableName);
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
    }
}
