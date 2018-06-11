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
       static void Main(string[] args)
        {
            try
            {
                LoadCommandLineArgs(args);
                //Call Method: Create Data Sets, Pipelines for all structures qualified for criteria.

                if (InitialParams.DeployCriteria.Equals("search"))
                {
                    string[] searchText = new string[3];

                    searchText[0] = InitialParams.SearchText01;
                    searchText[1] = InitialParams.SearchText02;
                    searchText[2] = InitialParams.SearchText03;

                    ADFOperations.DeployADFDataSetsAndPipelines(searchText);
                }

                //ADFOperations.DeployADFDataSetsAndPipelines(InitialParams.DataSourceName,
                //    InitialParams.TableName, InitialParams.FolderPath, InitialParams.FilterDateTimeField, InitialParams.FilterDateTimeInterval, CopyOnPremSQLToADLAType.All);

                //ADFOperations.DeployADFDataSetsAndPipelines(InitialParams.DataSourceName,
                //    InitialParams.TableName, InitialParams.FolderPath, InitialParams.FilterDateTimeField, InitialParams.FilterDateTimeInterval, CopyOnPremSQLToADLAType.Transactional);

                //ADFOperations.DeployADFDataSetsAndPipelines(InitialParams.DataSourceName,
                //    InitialParams.TableName, InitialParams.FolderPath, InitialParams.FilterDateTimeField, InitialParams.FilterDateTimeInterval, CopyOnPremSQLToADLAType.Distinct);

                ADFOperations.DeployADFDataSetsAndPipelines(InitialParams.DataSourceName,
                    InitialParams.TableName, InitialParams.FolderPath, InitialParams.FilterDateTimeField, InitialParams.FilterDateTimeInterval, CopyOnPremSQLToADLAType.Flattened);


                //ADFOperations.DeployADFDataSetsAndPipelines(_dataSourceName,
                //_tableName, _folderPath, _filterDateTimeField, _filterDateTimeInterval, CopyDataType.Distinct, "sys_id");

                //Console.WriteLine("Delete status of Input Data sets: Start ");

                //ADFOperations.DeleteDatasets("SC_DSO_H_DSCoinsSQLDataTest");

                //ADFOperations.DeleteDatasets("SC_DSO_D_DSCoinsSQLDataTest");

                //ADFOperations.DeleteDatasets("SC_DSI_H_DSCoinsSQLDataTest");

                //ADFOperations.DeleteDatasets("SC_DSI_D_DSCoinsSQLDataTest");

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
        /// This Method loads the Command Line Params passed as arguments during the execution of the program in the Static Variables.
        /// </summary>
        /// <param name="args"></param>
        static void LoadCommandLineArgs(String[] args)
        {
            try
            {
                String[] listArguments = args;

                try
                {
                    if (!System.String.IsNullOrEmpty(listArguments[0]))
                    {
                        InitialParams.DeployCriteria = listArguments[0].ToString();
                        Console.WriteLine(InitialParams.DeployCriteria);
                    }
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
                        InitialParams.SearchText01 = listArguments[1].ToString();
                        InitialParams.SearchText02 = listArguments[2].ToString();
                        InitialParams.SearchText03 = listArguments[3].ToString();
                        return;
                    }
                } catch(Exception ex)
                {

                }

                try
                {
                    if (!System.String.IsNullOrEmpty(listArguments[1]))
                    {
                        InitialParams.DataSourceName = listArguments[1].ToString();
                        Console.WriteLine(InitialParams.DataSourceName);
                    }
                }catch(IndexOutOfRangeException ex)
                {
                    Console.WriteLine("Please provide the command line arguments to proceed: dataSourceName, tableName, folderPath, filterDateTimeField, filterDateTimeInterval");                    
                }

                if (!System.String.IsNullOrEmpty(listArguments[2]))
                {
                    InitialParams.TableName = listArguments[2].ToString();
                    Console.WriteLine(InitialParams.TableName);
                }
                
                if (!System.String.IsNullOrEmpty(listArguments[3]))
                {
                    InitialParams.FolderPath = listArguments[3].ToString();
                    Console.WriteLine(InitialParams.FolderPath);
                }
                if (!System.String.IsNullOrEmpty(listArguments[4]))
                {
                    InitialParams.FilterDateTimeField = listArguments[4].ToString();
                    Console.WriteLine(InitialParams.FilterDateTimeField);
                }
                if (!System.String.IsNullOrEmpty(listArguments[5]))
                {
                    InitialParams.FilterDateTimeInterval = listArguments[5].ToString();
                    Console.WriteLine(InitialParams.FilterDateTimeInterval);
                }

                if (!System.String.IsNullOrEmpty(listArguments[6]))
                {
                    InitialParams.PrimaryKey = listArguments[6].ToString();
                    Console.WriteLine(InitialParams.PrimaryKey);
                }
            }
            catch (IndexOutOfRangeException ex)
            {
                Console.WriteLine("Few arguments are not provided : {0}", ex.Message);                
            }
        }
    }
}
