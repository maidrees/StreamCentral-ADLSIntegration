﻿using System;
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

            if (tableName.ToLower().Contains("seneca") || tableName.ToLower().Contains("gatehousesuite"))
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
            else if (tableName.ToLower().Contains("sharepointlist") || tableName.ToLower().Contains("sharepoint"))
            {
                dsType = "SharePointOnlineList";
            }
            else if (tableName.ToLower().Contains("sharepointonlinedoc") || tableName.ToLower().Contains("sharepointdoc"))
            {
                dsType = "SharePointOnlineDocuments";
            }
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
            else if (tableName.ToLower().Contains("excel"))
            {
                dsType = tableName;
            }
            else
            {
                dsType = tableName;
            }

            return dsType;
        }


    }

  
}
