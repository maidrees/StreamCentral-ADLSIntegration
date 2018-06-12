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
            CreateManagementClientInstance();
        }

        public static DataFactoryManagementClient  CreateManagementClientInstance()
        {

            //IMPORTANT: generate security token for the subsciption and AAD App
            TokenCloudCredentials aadTokenCredentials = new TokenCloudCredentials(ConfigurationSettings.AppSettings["SubscriptionId"],
                    GetAuthorizationHeader().Result);

            Uri resourceManagerUri = new Uri(ConfigurationSettings.AppSettings["ResourceManagerEndpoint"]);

            // create data factory management client
            client = new DataFactoryManagementClient(aadTokenCredentials, resourceManagerUri);

            return client;
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
                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty,CopyOnPremSQLToADLAType.All);

                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty, CopyOnPremSQLToADLAType.Transactional);
            }
        }


        //Create Data Sets and Pipelines - Deploy these in Azure Data Factory for a all structures in the source system.
        public static void DeployADFDataSetsAndPipelines(string [] searchTexts)
        {
            List<string> tableNames = ADFOperations.ListFilteredTableNames(searchTexts);

            //tableNames.Reverse();

            foreach (string tableName in tableNames)
            {
                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty, CopyOnPremSQLToADLAType.All);

                DeployADFDataSetsAndPipelines(String.Empty, tableName, String.Empty, string.Empty, string.Empty, CopyOnPremSQLToADLAType.Transactional);
            }
        }

        //Create Data Sets and Pipelines - Deploy these in Azure Data Factory for a single Structure.
        public static void DeployADFDataSetsAndPipelines(string dataSourceName, string tableName, 
            string folderPath, string dateTimeField, string interval, CopyOnPremSQLToADLAType  cpType)
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

            //if (System.String.IsNullOrEmpty(InitialParams.DataSourcePathInADLS))
            //{
            //    InitialParams.DataSourcePathInADLS = Utils.GetdataSourceType(tableName);
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

                string inDataSetName = String.Format("SC_DSI_H_{0}",InOutDataSetNameRef);                    
                string outDataSetName =String.Format("SC_DSO_H_{0}",InOutDataSetNameRef); 
                string pipelineName = "SC_PL01_Staging_H_" + dataSourceType + "_" + cpType.ToString();
                string fileName = "Header_" + InOutDataSetNameRef;
                string folderpath = String.Format("{0}/DL-{1}/{2}/{3}", InitialParams.FolderPath,
                    InitialParams.DataSourcePathInADLS, InitialParams.TablePathInADLS, InOutDataSetNameRef);

                DeployDatasetAndPipelines(pipelineName, inDataSetName, outDataSetName, tableName,
                    lstElements, fileName, folderpath, sqlQuery, firstDateTimeRecordInTable, false,cpType);

                Console.WriteLine("Deployed data sets and pipelines for  headers");
                
                //re: OUTPUT DATASET - Prepare the SQL query required for pipeline to execute on Source System

                sqlQuery = ADFOperations.GenerateADFPipelineSQLQuery(tableName, lstElements, dateTimeField, true,cpType);

                Console.WriteLine("Deploying data sets and pipelines for data");

                inDataSetName = String.Format("SC_DSI_D_{0}",InOutDataSetNameRef);
                outDataSetName = String.Format("SC_DSO_D_{0}", InOutDataSetNameRef);
                pipelineName = String.Format("SC_PL01_{0}_D_{1}_{2}",InitialParams.Environment, InitialParams.DataSourceName, cpType.ToString());

                if(cpType.Equals(CopyOnPremSQLToADLAType.Distinct) || cpType.Equals(CopyOnPremSQLToADLAType.All) || cpType.Equals(CopyOnPremSQLToADLAType.Flattened))
                {
                    fileName = "Data_" + InOutDataSetNameRef;
                }
                else
                {
                    fileName = "Data_" + InOutDataSetNameRef + "-{year}-{month}-{day}";
                }
                

                
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
             string sqlQuery, DateTime recorddateUTC, bool IsDataDeploy,CopyOnPremSQLToADLAType cpType)
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
            catch(InvalidOperationException invalidToken)
            {
                if (invalidToken.Message.Contains("token") || invalidToken.Message.Contains("expir"))
                {
                    Console.WriteLine("Oops! Client Token Expired while creating Input Data set:" + invalidToken.Message);
                    client = CreateManagementClientInstance();
                }
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

            try
            {
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
                                    NullValue = String.Empty,
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
            catch (InvalidOperationException invalidToken)
            {
                if (invalidToken.Message.Contains("token") || invalidToken.Message.Contains("expir"))
                {
                    Console.WriteLine("Oops! Client Token Expired while creating output data set : ",invalidToken.Message);
                    client = CreateManagementClientInstance();
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Generic Exception occured and couldnt handled: ", ex.Message);
            }
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
            DateTime recordDateUTC, bool isDataDeploy,CopyOnPremSQLToADLAType cpType)
        {
            DateTime PipelineActivePeriodStartTime = DateTime.Now.Subtract(TimeSpan.FromDays(1000.00));
            DateTime PipelineActivePeriodEndTime = PipelineActivePeriodStartTime;
            string mode = PipelineMode.OneTime;

            if ((isDataDeploy) && (!cpType.ToString().Equals(CopyOnPremSQLToADLAType.All.ToString())))
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

        
        //public static string GenerateADFPipelineSQLQuery(string tableName, 
        //    List<DataElement> inOutParams, string dateField, bool isDataQuery, CopyOnPremSQLToADLAType copyDataType)
        //{
        //    string sqlQuery = "$$Text.Format('select ";

        //    int itemIteration = 0;
        //    foreach (var columnName in inOutParams)
        //    {
        //        if (itemIteration < inOutParams.Count - 1)
        //        {
        //            sqlQuery = sqlQuery + "[" + columnName.Name + "],";
        //            itemIteration = itemIteration + 1;
        //        }
        //        else
        //        {
        //            sqlQuery = sqlQuery + "[" + columnName.Name + "]";
        //        }

        //    }

        //    if(!(isDataQuery && copyDataType.ToString() == CopyOnPremSQLToADLAType.All.ToString()))
        //    {
        //        sqlQuery = sqlQuery + " from " + tableName + " where [" + dateField + "] >= \\'{0:yyyy-MM-dd HH:mm}\\' AND  " +
        //      " [" + dateField + "] < \\'{1:yyyy-MM-dd HH:mm}\\'', " +
        //      "WindowStart, WindowEnd)";

        //        return sqlQuery;
        //    }

        //    switch (copyDataType)
        //    {
        //        case CopyOnPremSQLToADLAType.LastIteration:
        //            {

        //                break;
        //            }
        //        case CopyOnPremSQLToADLAType.All:
        //            {
        //                sqlQuery = sqlQuery + " from " + tableName + "')";
        //                break;
        //            }
        //        case CopyOnPremSQLToADLAType.Distinct:
        //            {
        //                break;
        //            }
        //        case CopyOnPremSQLToADLAType.Transactional:
        //            {
        //                sqlQuery = sqlQuery + " from " + tableName + " where [" + dateField + "] >= \\'{0:yyyy-MM-dd HH:mm}\\' AND  " +
        //       " [" + dateField + "] < \\'{1:yyyy-MM-dd HH:mm}\\'', " +
        //       "WindowStart, WindowEnd)";
        //                break;
        //            }
        //    }
                    

        //    return sqlQuery;
        //}

        public static string GenerateADFPipelineSQLQuery(string tableName,
            List<DataElement> inOutParams, string dateField, bool isDataQuery, CopyOnPremSQLToADLAType copyDataType)
        {
            string sqlQuery = "$$Text.Format('select ";

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

                        break;
                    }
                case CopyOnPremSQLToADLAType.All:
                    {
                        sqlQuery = sqlQuery + " from " + tableName + "')";
                        break;
                    }
                case CopyOnPremSQLToADLAType.Distinct:
                    {
                        sqlQuery = sqlQuery + " from (Select " + paramsQuery + ",ROW_NUMBER() " +
                            " over(partition by " + InitialParams.PrimaryKey + " order by " +
                            dateField + " desc) pk from " + tableName + ") dat where pk = 1')";
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

            if(tableName.Contains("DSServiceNowCMNLocations"))
            {
                sqlQuery = "$$Text.Format('SELECT [RECORDDATEUTC],[city]," +
                    "(select top 1 name from Fact_DSServiceNowCompaniesmetricdetails where sys_id = [company]) as [company]," +
                    "(select top 1 name from Fact_DSServiceNowUsersmetricdetails where sys_id = [contact] and name is not null) as [contact]," +
                    "[country],[fax_phone],[full_name],[lat_long_error],[latitude],[longitude],[name],[parent],[phone]," +
                    "[phone_territory],[state],[stock_room],[street],[sys_created_by],[sys_created_on],[sys_created_onId],[sys_id]," +
                    "[sys_mod_count],[sys_tags],[sys_updated_by],[sys_updated_on],[sys_updated_onId],[u_boolean_1]," +
                    "(select top 1 name from FACT_DSServiceNowUsersmetricdetails where sys_id = [u_chief_operating_officer]) as [u_chief_operating_officer]," +
                    "(select top 1 name from FACT_DSServiceNowUsersmetricdetails where sys_id = [u_contracts_manager]) as [u_contracts_manager], " +
                    "(select top 1 name from FACT_DSServiceNowUsersmetricdetails where sys_id = [u_head_of_delivery]) as [u_head_of_delivery], " +
                    "(select top 1 name from FACT_DSServiceNowUsersmetricdetails where sys_id = [u_project_manager]) as [u_project_manager], " +
                    "(select top 1 name from [FACT_DSServiceNowUsersmetricdetails] where sys_id = [u_reference_1]) as [u_reference_1]," +
                    "(select top 1 name from [FACT_DSServiceNowUsersmetricdetails] where sys_id = [u_reference_2]) as [u_reference_2]," +
                    "(select top 1 name from [FACT_DSServiceNowUsersmetricdetails] where sys_id = [u_reference_4]) as [u_reference_4], " +
                    "(select top 1 name from [FACT_DSServiceNowUsersmetricdetails] where sys_id = [u_reference_5]) as [u_reference_5],  " +
                    "(select top 1 name from [FACT_DSServiceNowUsersmetricdetails] where sys_id = [u_regionalmanager_office]) as [u_regionalmanager_office]," +
                    "[u_string_1],[zip] from " +

                    "(Select[RECORDDATEUTC], [city], [company], [contact], [country], [fax_phone], [full_name], [lat_long_error], [latitude],[longitude]," +
                    " [name], [parent], [phone], [phone_territory], [state], [stock_room], [street], [sys_created_by], [sys_created_on]," +
                    "[sys_created_onId], [sys_id], [sys_mod_count], [sys_tags], [sys_updated_by], [sys_updated_on], [sys_updated_onId]," +
                    " [u_boolean_1],[u_chief_operating_officer], [u_contracts_manager], [u_head_of_delivery], [u_project_manager], " +
                    "[u_reference_1],[u_reference_2],[u_reference_4],[u_reference_5], " +
                    "[u_regionalmanager_office], [u_string_1], " +
                    "[zip], ROW_NUMBER() over(partition by sys_id order by recorddateutc desc) pk from FACT_DSServiceNowCMNLocationsmetricdetails) " +
                    "dat where pk = 1')";

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
                sqlQuery = String.Format("SELECT TOP 1 {0} FROM {1} WHERE {2} IS NOT NULL ORDER BY {3} ASC", dateField, tableName, dateField,dateField);
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
    }
}
