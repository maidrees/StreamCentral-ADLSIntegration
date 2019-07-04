using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Newtonsoft.Json.Linq;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDatalakeStorereader
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
                CommandType = CommandType.StoredProcedure,
                CommandTimeout = 0

            };

            cmd.Parameters.Add(new SqlParameter("@TableName", tableName));
            return cmd;
        }

        public static SqlCommand GenerateStoredProcCommand(string spName, string Param1, string Param2, string Param3)
        {
            SqlCommand cmd = new SqlCommand(ConfigurationManager.AppSettings[spName], connection)
            {
                CommandType = CommandType.StoredProcedure,
                 CommandTimeout = 0
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

    class MasterDBUtils
    {
        public static NpgsqlConnection connection;

        static MasterDBUtils()
        {
            try
            {
                connection = new NpgsqlConnection(ConfigurationManager.AppSettings["MasterConnString"]);
                connection.Open();
            }
            catch (NpgsqlException nex)
            {
            }
            catch (Exception ex)
            {
            }
        }
      
        public static NpgsqlConnection MasterDBConnect()
        {
            try
            {
                if (connection == null )
                {
                    connection = new NpgsqlConnection(ConfigurationManager.AppSettings["MasterConnString"]);
                 
                    connection.Open();
                }
                else if (connection != null && connection.State == ConnectionState.Closed)
                {
                    connection = new NpgsqlConnection(ConfigurationManager.AppSettings["MasterConnString"]);
                    connection.Open();
                }

            }
            catch(NpgsqlException ex)
            {
                Console.Write("connection MasterDBConnect"+ex.Message);
            }
            return connection;
        }

        public static NpgsqlCommand GenerateStoredProcCommand(string spName, string tableName)
        {
            NpgsqlCommand cmd = new NpgsqlCommand(ConfigurationManager.AppSettings[spName], connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            cmd.Parameters.Add(new SqlParameter("@TableName", tableName));
            return cmd;
        }

        public static NpgsqlCommand GenerateStoredProcCommand(string spName, string Param1, string Param2, string Param3)
        {
            NpgsqlCommand cmd = new NpgsqlCommand(ConfigurationManager.AppSettings[spName], connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            cmd.Parameters.Add(new SqlParameter("@SearchText01", Param1));
            cmd.Parameters.Add(new SqlParameter("@SearchText02", Param2));
            cmd.Parameters.Add(new SqlParameter("@SearchText03", Param3));

            return cmd;
        }

        public static NpgsqlCommand GenerateSQLQueryCommand(string sqlQuery)
        {
            try
            {
                if (connection!=null && connection.State == ConnectionState.Closed)
                {
                 
                    MasterDBUtils.MasterDBConnect();
                    //connection.Open();
                    
                }
                else
                {
                    
                    MasterDBUtils.MasterDBConnect();
                   
                }
                NpgsqlCommand cmd = new NpgsqlCommand(sqlQuery, connection);
                cmd.CommandTimeout = 0;

                return cmd;
            }
            catch(Exception ex)
            {
                Console.WriteLine("Ex in GenerateSQLQueryCommand " + ex);
                return null;
               
            }
          
        }

        public static void CloseConnections()
        {
            connection.Close();
            connection.Dispose();
        }

    }
}

