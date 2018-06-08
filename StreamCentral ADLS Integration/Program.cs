using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamCentral.ADLSIntegration
{    
    class Program
    {
       static void Main(string[] args)
        {
            try
            {
                LoadCommandLineArgs(args);
                //Call Method: Create Data Sets, Pipelines for all structures qualified for criteria.

                if(InitialParams.DeployCriteria.Equals("search"))
                {
                    string[] searchText = new string[1];

                    searchText[0] = InitialParams.DataSourceName;

                    ADFOperations.DeployADFDataSetsAndPipelines(searchText);
                }

                ADFOperations.DeployADFDataSetsAndPipelines(InitialParams.DataSourceName,
                    InitialParams.TableName, InitialParams.FolderPath, InitialParams.FilterDateTimeField, InitialParams.FilterDateTimeInterval, CopyOnPremSQLToADLAType.All);


                ADFOperations.DeployADFDataSetsAndPipelines(InitialParams.DataSourceName,
                    InitialParams.TableName, InitialParams.FolderPath, InitialParams.FilterDateTimeField, InitialParams.FilterDateTimeInterval, CopyOnPremSQLToADLAType.Transactional);

                //                ADFOperations.DeployADFDataSetsAndPipelines(_dataSourceName,
                //_tableName, _folderPath, _filterDateTimeField, _filterDateTimeInterval, CopyDataType.Distinct,"sys_id");


                //Console.WriteLine("Delete status of Input Data sets: Start ");

                //ADFOperations.DeleteDatasets("SC_DSI");

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
            }
            catch (IndexOutOfRangeException ex)
            {
                Console.WriteLine("Few arguments are not provided : {0}", ex.Message);                
            }
        }
    }
}
