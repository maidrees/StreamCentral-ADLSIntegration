using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.Runtime;

namespace StreamCentral.ADLSIntegration
{    
    /// <summary>
    /// Entry point of the application. Loads params and calls out the required function.
    /// </summary>
    class Program
    {

        private static int _currArgIndex = 0;

        private static string[] argList;

       static void Main(string[] args)
        {
            try
            {
                try
                {
                    argList = args;

                    if (argList.First().Contains(":"))
                    {
                        LoadCommandLineArgs();
                    }
                    else
                    {
                        SetCommandLinePropertyValues();
                    }
                    ////Call Method: Create Data Sets, Pipelines for all structures qualified for criteria.

                }
                catch(Exception ex)
                {
                    Console.WriteLine("Unable to read any input from command line. Executing statements after that now:" + ex.Message);

                    ADFOperations.DeleteDatasets("DSDONSEEDTimesheetAdjudicationsThisWeek",EnumDeleteSearchType.Contains);
                    //ADFOperations.DeleteDatasets("SC_DSI_H_live");
                }
                if (InitialParams.DeployCriteria.Equals("search"))
                {
                    string[] searchText = new string[3];

                    searchText[0] = InitialParams.SearchText01;
                    searchText[1] = InitialParams.SearchText02;
                    searchText[2] = InitialParams.SearchText03;

                    ADFOperations.DeployADFDataSetsAndPipelines(searchText);
                }

                if (InitialParams.DeployCriteria.Equals("exact"))
                {
                    ADFOperations.DeployADFDataSetsAndPipelines(InitialParams.OnPremiseADLAType);
                }

                if (InitialParams.DeployCriteria.Equals("delete"))
                {

                    Console.WriteLine("Deleting the Pipelines : Start ");

                    ADFOperations.DeletePipelines(Utils.GetCustomizedPipelineName(true));
                    ADFOperations.DeletePipelines(Utils.GetCustomizedPipelineName(false));

                    Console.WriteLine("Deleted Pipelines : End ");

                    Console.WriteLine("Deleting the Data Sets now : Start ");


                    ADFOperations.DeleteDatasets(Utils.GetCustomizedInputDataSetName(true),EnumDeleteSearchType.Exact);
                    ADFOperations.DeleteDatasets(Utils.GetCustomizedOutputDataSetName(true),EnumDeleteSearchType.Exact);


                    ADFOperations.DeleteDatasets(Utils.GetCustomizedInputDataSetName(false),EnumDeleteSearchType.Exact);
                    ADFOperations.DeleteDatasets(Utils.GetCustomizedOutputDataSetName(false),EnumDeleteSearchType.Exact);


                    Console.WriteLine("Deleted Data Seta : End ");

                }

                if(InitialParams.DeployCriteria.Equals("bulk-copy"))
                { 

                }
                
                Console.WriteLine("Completed the process in Microsoft Windows Azure");

               // Console.ReadLine();                
            }
            catch(Exception ex)
            {
                Console.WriteLine("Some exception while working with Azure Environment : {0}", ex.Message);
                Console.ReadLine();
            }
        }

       
        /// <summary>
        /// This Method loads the Command Line Params passed as arguments 
        /// during the execution of the program 
        /// in the Application Static Variables.
        /// </summary>
        /// <param name="args"></param>
        static void LoadCommandLineArgs()
        {
            try
            {
                int count = 0;
                do
                {
                    ReadNextArgumentValue();
                    count++;

                } while (argList.Count() > count);

                
                InitialParams.DataSourcePathInADLS = Utils.GetdataSourceType(InitialParams.DataSourceName);
                Console.WriteLine("Data Source root path entered: " + InitialParams.DataSourcePathInADLS);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to read the data source rooth path : " + ex.Message);
            }           
        }

        public static string ReadNextArgumentValue()
        {
            int nxtArgIndex = _currArgIndex;

            string arg = string.Empty;

            string[] arguments = new string[2];

            try
            {
                arg = argList[nxtArgIndex];

                try
                {
                    if (arg.Contains(":"))
                    {
                        arguments = arg.Split(':');

                       SetCommandLinePropertyValues(arguments[0], arguments[1]);                        
                    }                  
                }
                catch(Exception ex)
                {
                    Console.WriteLine("Unable to split input parameter: " + ex.Message);                    
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to read the argument:" + ex.Message);
            }

            _currArgIndex = nxtArgIndex+1;

            return arg;
        }

        public static void SetCommandLinePropertyValues(string propertyName, string propertyValue)
        {
            switch(propertyName)
            {
                case nameof(InputCommandLineArgs.criteria):
                    {
                        InitialParams.DeployCriteria = propertyValue;
                        Console.WriteLine("ADLS assembly invoked purpose: " + InitialParams.DeployCriteria);
                        break;
                    }

                case nameof(InputCommandLineArgs.datasource):
                    {
                        InitialParams.DataSourceName = propertyValue;
                        Console.WriteLine("Data Source Name: " + InitialParams.DataSourceName);
                        break;
                    }

                case nameof(InputCommandLineArgs.table):
                    {
                        InitialParams.TableName = propertyValue;
                        Console.WriteLine("Table Name entered: " + InitialParams.TableName);
                        break;
                    }

                case nameof(InputCommandLineArgs.folderpath):
                    {
                        InitialParams.FolderPath = propertyValue;
                        Console.WriteLine("Folder Path entered: " + InitialParams.FolderPath);
                        break;
                    }

                case nameof(InputCommandLineArgs.datefilter):
                    {
                        InitialParams.FilterDateTimeField = propertyValue;
                        Console.WriteLine("Filter Time Field entered: " + InitialParams.FilterDateTimeField);
                        break;
                    }

                case nameof(InputCommandLineArgs.dataslice):
                    {
                        InitialParams.ActivityFrequencyType = Utils.GetDSActivityFrequency(propertyValue);
                        Console.WriteLine("Frequency Type for scheduling Activity entered:  " + InitialParams.ActivityFrequencyType);
                        break;
                    }
                case nameof(InputCommandLineArgs.datasliceinterval):
                    {
                        InitialParams.ActivityFrequencyInterval = propertyValue;
                        Console.WriteLine("Frequency Interval for scheduling Activity entered:  " + InitialParams.ActivityFrequencyInterval.ToString());
                        break;
                    }
                case nameof(InputCommandLineArgs.primarykey):
                    {
                        InitialParams.PrimaryKey = propertyValue;
                        Console.WriteLine("Primary Key entered: " + InitialParams.PrimaryKey);
                        break;
                    }
                case nameof(InputCommandLineArgs.copydataType):
                    {
                        InitialParams.OnPremiseADLAType = Utils.GetOnPremADLAType(propertyValue);
                        Console.WriteLine("Copy On-Premise To ADLA Type entered: " + InitialParams.OnPremiseADLAType.ToString());
                        break;
                    }
                case nameof(InputCommandLineArgs.tablepathinadls):
                    {
                        InitialParams.TablePathInADLS = propertyValue;
                        Console.WriteLine("Table path in ADLS entered: " + InitialParams.TablePathInADLS);
                        break;
                    }
                case nameof(InputCommandLineArgs.environment):
                    {
                        InitialParams.Environment = propertyValue;
                        Console.WriteLine("Environment entered: " + InitialParams.Environment);
                        break;
                    }
                case nameof(InputCommandLineArgs.offsetinterval):
                    {
                        InitialParams.OffsetIntervalOfDataSlice = propertyValue;
                        Console.WriteLine("OffSet Interval entered: " + InitialParams.OffsetIntervalOfDataSlice);
                        break;
                    }
                case nameof(InputCommandLineArgs.delayinterval):
                    {
                        InitialParams.DelayIntervalOfActivity = propertyValue;
                        Console.WriteLine("Delay Interval for Activity entered: " + InitialParams.DelayIntervalOfActivity);
                        break;
                    }
                case nameof(InputCommandLineArgs.datasourcepathinadls):
                    {
                        InitialParams.DataSourcePathInADLS = Utils.GetdataSourceType(InitialParams.DataSourceName);
                        Console.WriteLine("Data Source root path in ADLS entered: " + InitialParams.DataSourcePathInADLS);
                        break;
                    }
                
                case nameof(InputCommandLineArgs.temppathdeviation):
                    {
                        InitialParams.TempPathDeviation = propertyValue;
                        Console.WriteLine("Temporary path deviation root entered: " + InitialParams.TempPathDeviation);
                        break;
                    }
                case nameof(InputCommandLineArgs.tempcompprefix):
                    {
                        InitialParams.TempCompPrefix = propertyValue;
                        Console.WriteLine("Temporary Component Name prefix entered: " + InitialParams.TempCompPrefix);
                        break;
                    }
                case nameof(InputCommandLineArgs.slicetype):
                    {
                        InitialParams.SliceType = Utils.GetSliceType(propertyValue); ;
                        Console.WriteLine("Temporary Component Name prefix entered: " + InitialParams.TempCompPrefix);
                        break;
                    }
                case nameof(InputCommandLineArgs.pipelinename):
                    {
                        InitialParams.DeletePipelineName = propertyValue;
                        Console.WriteLine("Pipeline Name to be deleted entered: " + InitialParams.DeletePipelineName);
                        break;
                    }
                case nameof(InputCommandLineArgs.inputds):
                    {
                        InitialParams.DeleteInputDataSetName = propertyValue;
                        Console.WriteLine("Input Data set to be deleted entered: " + InitialParams.DeleteInputDataSetName);
                        break;
                    }
                case nameof(InputCommandLineArgs.outputds):
                    {
                        InitialParams.DeleteOutputDataSetName = propertyValue;
                        Console.WriteLine("Output Data set to be deleted entered: " + InitialParams.DeleteOutputDataSetName);
                        break;
                    }
                case nameof(InputCommandLineArgs.sourcetype):
                    {
                        InitialParams.SourceType = Utils.GetSourceType(propertyValue);
                        Console.WriteLine("Source Type defined : " + InitialParams.SourceType);
                        break;
                    }

                case nameof(InputCommandLineArgs.sourceStructureType):
                    {
                        InitialParams.SourceStructureType = Utils.GetSourceStructureType(propertyValue);
                        Console.WriteLine("Source Structure Type defined : " + InitialParams.SourceStructureType);
                        break;
                    }

            }
        }

        public static void SetCommandLinePropertyValues()
        {
            try
            {
                try
                {
                    InitialParams.DeployCriteria = ReadNextArgumentValue();
                    Console.WriteLine("ADLS assembly invoked purpose: " + InitialParams.DeployCriteria);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: deployCriteria,dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " + ex.Message);
                    return;
                }

                try
                {
                    if (InitialParams.DeployCriteria.Equals("search"))
                    {
                        InitialParams.SearchText01 = ReadNextArgumentValue();
                        InitialParams.SearchText02 = ReadNextArgumentValue();
                        InitialParams.SearchText03 = ReadNextArgumentValue();
                        return;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Not all search parameters are passed as arguments: " + ex.Message);
                }

                if (InitialParams.DeployCriteria.Equals("exact"))
                {
                    //Specify the Data Source Name
                    try
                    {
                        InitialParams.DataSourceName = ReadNextArgumentValue();
                        Console.WriteLine("Data Source Name: " + InitialParams.DataSourceName);

                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " +ex.Message);
                    }

                    //specify the name of the table for which the ETL to be configured.
                    try
                    {
                        InitialParams.TableName = ReadNextArgumentValue();
                        Console.WriteLine("Table Name entered: " + InitialParams.TableName);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " + ex.Message);
                    }

                    //specify the root path of ADLS
                    try
                    {
                        InitialParams.FolderPath = ReadNextArgumentValue();
                        Console.WriteLine("Folder Path entered: " + InitialParams.FolderPath);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " + ex.Message);
                    }

                    //Used to specify the time interval field that shall be used to slice the data in ETL process. 
                    //RECORDDATEUTC will become otherwise the default time interval.
                    try
                    {
                        InitialParams.FilterDateTimeField = ReadNextArgumentValue();
                        Console.WriteLine("Filter Time Field entered: " + InitialParams.FilterDateTimeField);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " + ex.Message);
                    }

                    //Used to configure the schedule time interval for the Pipeline to run the activity
                    try
                    {
                        InitialParams.ActivityFrequencyType = Utils.GetDSActivityFrequency(ReadNextArgumentValue());
                        Console.WriteLine("Frequency Type for scheduling Activity entered:  " + InitialParams.ActivityFrequencyType);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval :" + ex.Message);
                    }

                    //Used to configure the schedule time interval for the Pipeline to run the activity
                    try
                    {
                        InitialParams.ActivityFrequencyInterval = ReadNextArgumentValue();
                        Console.WriteLine("Frequency Interval for scheduling Activity entered:  " + InitialParams.ActivityFrequencyInterval.ToString());
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " + ex.Message);
                    }

                    //used for ETL of distinct records.
                    try
                    {
                        InitialParams.PrimaryKey = ReadNextArgumentValue();
                        Console.WriteLine("Primary Key entered: " + InitialParams.PrimaryKey);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " + ex.Message);
                    }

                    try
                    {
                        InitialParams.OnPremiseADLAType = Utils.GetOnPremADLAType(ReadNextArgumentValue());

                        Console.WriteLine("Copy On-Premise To ADLA Type entered: " + InitialParams.OnPremiseADLAType.ToString());
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval :" + ex.Message);
                    }

                    try
                    {
                        InitialParams.TablePathInADLS = ReadNextArgumentValue();
                        Console.WriteLine("Table path entered: " + InitialParams.TablePathInADLS);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : " + ex.Message);
                    }

                    try
                    {
                        InitialParams.Environment = ReadNextArgumentValue();
                        Console.WriteLine("Environment entered: " + InitialParams.Environment);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval :" + ex.Message);
                    }

                    try
                    {
                        InitialParams.OffsetIntervalOfDataSlice = ReadNextArgumentValue();
                        Console.WriteLine("Pipeline scheduling offset entered: " + InitialParams.OffsetIntervalOfDataSlice);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval : "  + ex.Message);
                    }



                    try
                    {
                        InitialParams.DataSourcePathInADLS = Utils.GetdataSourceType(InitialParams.DataSourceName);
                        Console.WriteLine("Data Source root path entered: " + InitialParams.DataSourcePathInADLS);
                    }
                    catch (IndexOutOfRangeException ex)
                    {
                        Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval: " + ex.Message);
                    }


                }

                if (InitialParams.DeployCriteria.Equals("delete"))
                {

                }
                _currArgIndex = 0;
            }
            catch (IndexOutOfRangeException ex)
            {
                Console.WriteLine("Few arguments are not provided : {0}", ex.Message);
            }
        }
    }
}
