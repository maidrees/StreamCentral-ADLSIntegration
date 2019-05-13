using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Streamcentral.USQLScriptGenertor
{
    class USQLGenerateScript
    {
        
            static readonly string textFile = "U-SQL-DISTINCT_TEMPLATE_V1.txt";
            static readonly string tableHeaders = "RECORDDATEUTC~Active~B2CGuid~Email~Id~Name~NoEmails";
            static readonly string adlsMainPath = "adl://cgsdatalakedw.azuredatalakestore.net/Careys-Provisioned-Data-Live";
            static readonly string adlsContainerPath = "DL-AstaCloudPowerProject";
            static readonly string adlsDSfolderpath = "DSAstaCloudProjectppiUsers";
            static readonly string adlsDSSourceFilePath = "1Day_Transactional_" + adlsDSfolderpath;
            static readonly string dsUniqueId = "Id";
            static readonly string adlsSourceType = "Transactional";
            static readonly string adlsDestFilePath = "1Day_Distinct_" + adlsDSfolderpath;
            static readonly string adlaDestinationType = "Distinct";
            static readonly string frequency = "1";
            static readonly string frequencyUOM = "Day";
            static readonly string dsRecordDateUTc = "RECORDDATEUTC";

            static void GenerateScript()
            {
                string inputParams, outParams = String.Empty;

                Console.WriteLine("Starting to prepare the USQL Scripts: ");

                inputParams = PrepareInputParams(tableHeaders);

                outParams = PrepareOutputParams(tableHeaders);

                string outadlsSourcePathInFull = FormatADLSSourcePath();

                string outadlsDestFileNameInFull = FormatADLSDestFileInFull();

                FormatUSQLScriptTemplate(inputParams, outParams);
            }

            private static string FormatADLSDestFileInFull()
            {
                String outadlsDestFileNameInFull = String.Empty;

                outadlsDestFileNameInFull = adlsMainPath.Replace("Careys-Provisioned-Data-Live", "USQL_Derived_Data_Test2")
                    + "/" + adlsContainerPath
                    + "/" + adlsDSfolderpath
                    + "/" + adlsDestFilePath + "/Data_" + adlsDSfolderpath;

                return outadlsDestFileNameInFull;
            }

            private static string FormatADLSSourcePath()
            {
                String outadlsSourcePathInfull = string.Empty;

                outadlsSourcePathInfull = adlsMainPath
                    + "/" + adlsContainerPath
                    + "/" + adlsDSfolderpath
                    + "/" + adlsDSSourceFilePath + "/Data_{*}";

                return outadlsSourcePathInfull;

            }


            private static string PrepareInputParams(string tableHeaders)
            {
                string outParams = String.Empty;
                int i = 0;
                foreach (var attr in tableHeaders.Split('~'))
                {
                    if ((attr != null) && (!String.IsNullOrEmpty(attr)))
                    {
                        outParams = outParams + "[" + attr + "]" + " string";

                        if (i < (tableHeaders.Split('~').Length - 1))
                        {
                            outParams = outParams + ",";
                            i++;
                        }
                    }
                }

                Console.WriteLine(outParams);
                return outParams;
            }

            //private static string PrepareOutputParams(string tableHeaders)
            //{
            //    string outParams = String.Empty;
            //    int counter = 0;
            //    foreach (var attr in tableHeaders.Split('~'))
            //    {
            //        outParams = outParams + "[" + attr + "]";

            //        if (counter < (tableHeaders.Split('~').Length - 1))
            //        {
            //            outParams = outParams + ",";
            //            counter++;
            //        }
            //    }

            //    Console.WriteLine(outParams);

            //    //File.WriteAllText("output.txt", outParams);

            //    Console.WriteLine($"File has {counter} lines.");

            //    return outParams;
            //}

            private static string PrepareOutputParams(string tableHeaders)
            {
                string outParams = String.Empty;
                int counter = 0;
                foreach (var attr in tableHeaders.Split('~'))
                {
                    outParams = outParams + "[" + attr + "]";

                    if (counter < (tableHeaders.Split('~').Length - 1))
                    {
                        outParams = outParams + ",";
                        counter++;
                    }
                }

                Console.WriteLine(outParams);

                //File.WriteAllText("output.txt", outParams);

                Console.WriteLine($"File has {counter} lines.");

                return outParams;
            }

            private static string FormatUSQLScriptTemplate(string inParams,
                string outParams)
            {
                // Read file using StreamReader. Reads file line by line  
                using (StreamReader file = new StreamReader(textFile))
                {
                    int counter = 0;
                    string ln;
                    string USQLScript = string.Empty;

                    while ((ln = file.ReadLine()) != null)
                    {
                        string modifiedString = String.Empty;
                        modifiedString = ln;

                        if (ln.Contains("#vitsourcepath#"))
                        {
                            modifiedString = modifiedString.Replace("#vitsourcepath#", FormatADLSSourcePath());
                        }

                        if (ln.Contains("#vitdestfilename#"))
                        {
                            modifiedString = modifiedString.Replace("#vitdestfilename#", FormatADLSDestFileInFull());
                        }

                        if (ln.Contains("#vitinparams#"))
                        {
                            modifiedString = modifiedString.Replace("#vitinparams#", inParams);
                        }

                        if (ln.Contains("#vitfilteredparams#"))
                        {
                            //string filteredParams = string.Empty;
                            //string strToRemove = "[" + dsUniqueId + "],";
                            //filteredParams = outParams.Replace(strToRemove, "");
                            //filteredParams = "[" + dsUniqueId + "]," + filteredParams;

                            modifiedString = modifiedString.Replace("#vitfilteredparams#", outParams);
                        }

                        if (ln.Contains("#vitoutparams#"))
                        {

                            modifiedString = modifiedString.Replace("#vitoutparams#", outParams);
                        }

                        if (ln.Contains("#vitprimid#"))
                        {

                            string primaryKey = string.Empty; int count = 0;
                            foreach (var uniqueId in dsUniqueId.Split(','))
                            {

                                primaryKey = primaryKey + "[" + uniqueId + "]";

                                if (count < dsUniqueId.Split(',').Length - 1)
                                {
                                    primaryKey = primaryKey + ",";
                                }
                                count++;
                            }

                            modifiedString = modifiedString.Replace("#vitprimid#", primaryKey);
                        }

                        if (ln.Contains("#vitorderbydate#"))
                        {
                            string recorddate = "[" + dsRecordDateUTc + "]";
                            modifiedString = modifiedString.Replace("#vitorderbydate#", recorddate);
                        }


                        if (ln.Contains(";"))
                        {
                            modifiedString = modifiedString + Environment.NewLine + Environment.NewLine;
                        }

                        if (ln.Contains("//"))
                        {
                            modifiedString = modifiedString + Environment.NewLine + Environment.NewLine;
                        }

                        USQLScript = USQLScript + modifiedString;
                    }

                    file.Close();

                    Console.WriteLine(USQLScript);

                    string USQLScriptFileName = @"C:\Users\idrees.mohammed\OneDrive - Careys Group PLC\USQL Scripts\" + adlsDestFilePath;
                    File.WriteAllText(USQLScriptFileName, USQLScript);

                    Console.WriteLine($"File has {counter} lines.");
                }

                return outParams;
            }
        
    }
}
