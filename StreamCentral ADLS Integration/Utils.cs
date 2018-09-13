using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace StreamCentral.ADLSIntegration
{
    class Utils
    {
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
            else if (tableName.ToLower().Contains("servicenow"))
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
            else if (tableName.ToLower().Contains("pocbdl") || tableName.ToLower().Contains("bdl"))
            {
                dsType = "CMT";
            }
            else
            {
                dsType = tableName;
            }
            return dsType;
        }

        public static string GetFormattedFolderPath(string tableName)
        {
            string formattedFolderPath = String.Empty;

            formattedFolderPath = tableName.Replace("[", String.Empty).Replace("]", String.Empty).
                    Replace("FACT_", String.Empty).Replace("fact_", String.Empty).Replace("metricdetails", String.Empty);
            return formattedFolderPath;
        }

        public static CopyOnPremSQLToADLAType GetOnPremADLAType(string onPremAdlaType)
        {
            switch (onPremAdlaType.ToLower())
            {
                case "all":
                    {
                        return CopyOnPremSQLToADLAType.All;
                    }
                case "transactional":
                    {
                        return CopyOnPremSQLToADLAType.Transactional;
                    }
                case "distinct":
                    {
                        return CopyOnPremSQLToADLAType.Distinct;
                    }
                case "lastiteration":
                    {
                        return CopyOnPremSQLToADLAType.LastIteration;
                    }
                default:
                    return CopyOnPremSQLToADLAType.All;
            }
        }

        public static Frequency GetDSActivityFrequency(string frequency)
        {
            switch (frequency.ToLower())
            {
                case "year":
                    {
                        return Frequency.Year;
                    }
                case "month":
                    {
                        return Frequency.Month;
                    }
                case "day":
                    {
                        return Frequency.Day;
                    }
                case "hour":
                    {
                        return Frequency.Hour;
                    }
                case "minute":
                    {
                        return Frequency.Minute;
                    }
                default:
                    return Frequency.Day;
            }
        }

        public static SliceType GetSliceType(string value)
        {
            if (!String.IsNullOrEmpty(value))
            {
                if (value.Equals(SliceType.Start.ToString().ToLower()))
                {
                    return SliceType.Start;
                }
                else
                {
                    return SliceType.End;
                }
            }
            return SliceType.End;
        }

        public static string GetCustomizedCompPrefixForFolderPath(string compPrefix)
        {
            string compPrefixPath;
            if (!string.IsNullOrEmpty(compPrefix))
            {

                compPrefixPath = compPrefix.Remove(compPrefix.Length - 1, 1);
                compPrefixPath = compPrefixPath + "/";
                return compPrefixPath;
            }
            return compPrefix;
        }

        public static string GetCustomizedInputOutRefName()
        {
            string InOutRefName;
            InOutRefName = String.Format(InitialParams.ActivityFrequencyInterval + InitialParams.ActivityFrequencyType + "_" +
                InitialParams.OnPremiseADLAType.ToString() + "_" + InitialParams.TablePathInADLS);

            return InOutRefName;
        }

        public static String GetCustomizedInputDataSetName(bool isHeader)
        {
            if (isHeader)
            {
                return String.Format(Constants._inputDSHeaderNameUnformatted, InitialParams.TempCompPrefix,InitialParams.Environment, GetCustomizedInputOutRefName());
            }
            return String.Format(Constants._inputDSDataNameUnformatted, InitialParams.TempCompPrefix, InitialParams.Environment, GetCustomizedInputOutRefName());
        }

        public static String GetCustomizedOutputDataSetName(bool isHeader)
        {
            if (isHeader)
            {
                return String.Format(Constants._outputDSHeaderNameUnformatted, InitialParams.TempCompPrefix, InitialParams.Environment, GetCustomizedInputOutRefName());
            }
            return String.Format(Constants._outputDSDataNameUnformatted, InitialParams.TempCompPrefix, InitialParams.Environment, GetCustomizedInputOutRefName());

        }

        public static String GetCustomizedPipelineName(bool isHeader)
        {
            if (isHeader)
            {
                return String.Format(Constants._pipelineHeaderNameUnformatted, InitialParams.TempCompPrefix, InitialParams.Environment, GetCustomizedInputOutRefName());
            }
            return String.Format(Constants._pipelineDataNameUnformatted, InitialParams.TempCompPrefix, InitialParams.Environment, GetCustomizedInputOutRefName()) ;
        }

        public static string GetCustomizedActivityName(bool isHeader)
        {
            if (isHeader)
            {
                return String.Format(Constants._pipelineHeaderNameUnformatted, InitialParams.TempCompPrefix, InitialParams.Environment, GetCustomizedInputOutRefName());
            }
            return String.Format(Constants._pipelineDataNameUnformatted, InitialParams.TempCompPrefix, InitialParams.Environment, GetCustomizedInputOutRefName());
        }

        public static string GetCustomizedFileName(bool isHeader)
        {

            String fileName = String.Empty;

            if (isHeader)
            {
                fileName= String.Format(Constants._headerFileNameUnformatted,GetCustomizedInputOutRefName());
                return fileName;
            }

            if (InitialParams.OnPremiseADLAType.Equals(CopyOnPremSQLToADLAType.Distinct) ||
                    InitialParams.OnPremiseADLAType.Equals(CopyOnPremSQLToADLAType.All) ||
                    InitialParams.OnPremiseADLAType.Equals(CopyOnPremSQLToADLAType.Flattened) ||
                    InitialParams.OnPremiseADLAType.Equals(CopyOnPremSQLToADLAType.LastIteration))
            {
                fileName = String.Format(Constants._dataFileNameUnformatted);
            }
            else
            {
                fileName = String.Format(Constants._dataFileNameTransactionalUnformatted, GetCustomizedInputOutRefName());
            }
            return fileName; ;
        }

        public static string GetCustomizedFolderPath()
        {
            return String.Format("{0}/{4}DL-{1}/{2}/{3}", InitialParams.FolderPath,
                    InitialParams.DataSourcePathInADLS, InitialParams.TablePathInADLS, GetCustomizedInputOutRefName(),
                    InitialParams.TempPathDeviation);
        }

        public static EnumSourceStructureType GetSourceStructureType(string value)
        {
            string sourceStructureType = String.Empty;

            if (!String.IsNullOrEmpty(value))
            {
                sourceStructureType = value;
            }
            else if (!String.IsNullOrEmpty(ConfigurationManager.AppSettings["sourceType"]))
            {
                sourceStructureType = ConfigurationManager.AppSettings["sourceType"];
            }

            if (sourceStructureType.ToLower().Contains(EnumSourceStructureType.OnPremiseSQLServer.ToString().ToLower()))
            {
                return EnumSourceStructureType.OnPremiseSQLServer;
            }
            else
            {
                return EnumSourceStructureType.AzureSQLServer;
            }
        }

    } 
}
