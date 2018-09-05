using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            else if (tableName.ToLower().Contains("coins"))
            {
                dsType = "CoinsSQL";
            }
            else if(tableName.ToLower().Contains("pocbdl"))
            {
                dsType = "CMT";
            }
            else if (tableName.ToLower().Contains("excel"))
            {
                dsType = "CoinsEXCEL";
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
            switch (onPremAdlaType)
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
            switch(frequency.ToLower())
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
            if(!String.IsNullOrEmpty(value))
            {
                if(value.Equals(SliceType.Start.ToString().ToLower()))
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

    }

  
}
