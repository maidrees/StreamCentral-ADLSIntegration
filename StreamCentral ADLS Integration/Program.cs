using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                argList = args;

                LoadCommandLineArgs();
                //Call Method: Create Data Sets, Pipelines for all structures qualified for criteria.

                if (InitialParams.DeployCriteria.Equals("search"))
                {
                    string[] searchText = new string[3];

                    searchText[0] = InitialParams.SearchText01;
                    searchText[1] = InitialParams.SearchText02;
                    searchText[2] = InitialParams.SearchText03;

                    ADFOperations.DeployADFDataSetsAndPipelines(searchText);
                }

                ADFOperations.DeployADFDataSetsAndPipelines(CopyOnPremSQLToADLAType.All);

                //ADFOperations.DeployADFDataSetsAndPipelines(CopyOnPremSQLToADLAType.Transactional);

                //ADFOperations.DeployADFDataSetsAndPipelines(CopyOnPremSQLToADLAType.Distinct);
                
                //ADFOperations.DeployADFDataSetsAndPipelines(_dataSourceName,
                //_tableName, _folderPath, _filterDateTimeField, _filterDateTimeInterval, CopyDataType.Distinct, "sys_id");

                //Console.WriteLine("Delete status of Input Data sets: Start ");

                //ADFOperations.DeleteDatasets("SC_DSO_H_POCSharePoint");

                //ADFOperations.DeleteDatasets("SC_DSO_D_POCSharePoint");

                ////// ADFOperations.DeleteDatasets("SC_DSI_H_DSSnowdropLive");

                ////// ADFOperations.DeleteDatasets("SC_DSI_D_DSSnowdropLive");

                //Console.WriteLine("Delete status of Input Data sets : Success");


                //Console.WriteLine("Delete status of Output Data sets: Start ");

                //ADFOperations.DeleteDatasets("SC_DSO");

                //Console.WriteLine("Delete status of Output Data sets : Success");


                Console.WriteLine("Completed the process of deploying ADF in azure");
                Console.ReadLine();
            }
            catch(Exception ex)
            {
                Console.WriteLine("Some exception occured: {0}", ex.Message);
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
                try
                {
                      InitialParams.DeployCriteria = ReadNextArgumentValue();
                      Console.WriteLine(InitialParams.DeployCriteria);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: deployCriteria,dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                    return;
                }

                try
                {
                    if(InitialParams.DeployCriteria.Equals("search"))
                    {
                        InitialParams.SearchText01 = ReadNextArgumentValue();
                        InitialParams.SearchText02 = ReadNextArgumentValue();
                        InitialParams.SearchText03 = ReadNextArgumentValue();
                        return;
                    }
                } catch(Exception ex)
                {
                    Console.WriteLine("Not all search parameters are passed as arguments: " + ex.Message);
                }

                //Specify the Data Source Name
                try
                {  
                        InitialParams.DataSourceName = ReadNextArgumentValue();
                        Console.WriteLine("Data Source Name: " + InitialParams.DataSourceName);
                    
                }catch(IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");                    
                }

                //specify the name of the table for which the ETL to be configured.
                try
                {
                    InitialParams.TableName = ReadNextArgumentValue();
                    Console.WriteLine("Table Name entered: " + InitialParams.TableName);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                //specify the root path of ADLS
                try
                {
                    InitialParams.FolderPath = ReadNextArgumentValue();
                    Console.WriteLine("Folder Path entered: " + InitialParams.FolderPath);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                //Used to specify the time interval field that shall be used to slice the data in ETL process. 
                //RECORDDATEUTC will become otherwise the default time interval.
                try
                {
                    InitialParams.FilterDateTimeField = ReadNextArgumentValue();
                    Console.WriteLine("Filter Time Field entered: "+ InitialParams.FilterDateTimeField);
                }catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                //Used to configure the schedule time interval for the Pipeline to run the activity
                try
                {
                    InitialParams.ActivityFrequencyType = ReadNextArgumentValue();
                    Console.WriteLine("Frequency Type for scheduling Activity entered:  " + InitialParams.ActivityFrequencyType);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                //Used to configure the schedule time interval for the Pipeline to run the activity
                try
                {
                    InitialParams.ActivityFrequencyInterval = ReadNextArgumentValue();
                    Console.WriteLine("Frequency Interval for scheduling Activity entered:  " + InitialParams.ActivityFrequencyInterval);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                //used for ETL of distinct records.
                try
                {
                    InitialParams.PrimaryKey = ReadNextArgumentValue();
                    Console.WriteLine("Primary Key entered: " + InitialParams.PrimaryKey);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                try
                {
                    InitialParams.DataSourcePathInADLS = ReadNextArgumentValue();
                    Console.WriteLine("Data Source root path entered: " + InitialParams.DataSourcePathInADLS);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                try
                {
                    InitialParams.TablePathInADLS = ReadNextArgumentValue();
                    Console.WriteLine("Table path entered: " + InitialParams.TablePathInADLS);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                try
                {
                    InitialParams. = ReadNextArgumentValue();
                    Console.WriteLine("Environment entered: " + InitialParams.Environment);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                try
                {
                    InitialParams.Environment = ReadNextArgumentValue();
                    Console.WriteLine("Environment entered: " + InitialParams.Environment);
                }
                catch (IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");
                }

                _currArgIndex = 0;
            }
            catch (IndexOutOfRangeException ex)
            {
                Console.WriteLine("Few arguments are not provided : {0}", ex.Message);                
            }
        }

        public static string ReadNextArgumentValue()
        {
            int nxtArgIndex = _currArgIndex;
            string arg = string.Empty;

            try
            {
                arg = argList[nxtArgIndex];
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unable to read the argument:" + ex.Message);
            }

            _currArgIndex = nxtArgIndex+1;
            return arg;
        }
    }
}
