using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace StreamCentral.ADLSIntegration
{

    class SQLUtils
    {
        public static SqlConnection connection;

        static SQLUtils()
        {
            connection = new SqlConnection(ConfigurationManager.AppSettings["SCDMConn"]);
        }
        public static SqlConnection SQLConnect()
        {
            return connection;
        }

        public static SqlCommand GenerateStoredProcCommand(string spName, string tableName)
        {
            SqlCommand cmd = new SqlCommand(ConfigurationManager.AppSettings[spName], connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            cmd.Parameters.Add(new SqlParameter("@TableName", tableName));
            return cmd;
        }

        public static SqlCommand GenerateStoredProcCommand(string spName, string Param1, string Param2, string Param3)
        {
            SqlCommand cmd = new SqlCommand(ConfigurationManager.AppSettings[spName], connection)
            {
                CommandType = CommandType.StoredProcedure
            };
        
            cmd.Parameters.Add(new SqlParameter("@SearchText01", Param1));
            cmd.Parameters.Add(new SqlParameter("@SearchText02", Param2));
            cmd.Parameters.Add(new SqlParameter("@SearchText03", Param3));

            return cmd;
        }

        public static SqlCommand GenerateSQLQueryCommand(string sqlQuery)
        {
            SqlCommand cmd = new SqlCommand(sqlQuery, connection)
            {
                CommandType = CommandType.Text,
                CommandText = sqlQuery
             };                 

            return cmd;
        }      
    }

}
