using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Security.Permissions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;

namespace AzureDatalakeStorereader
{
    public static class Logging
    {
        public static string _LoggerMode = "INFO", servername, locationtype, queuename, queuetype;
      
        public static string _exepath = String.Empty;
        public static double duration = 1000;

        static Logging()
        {
            _LoggerMode = ConfigurationManager.AppSettings["Logging"];
            if (string.IsNullOrEmpty(_LoggerMode))
            {


                try
                {
                    _exepath = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
                }
                catch
                {
                    _exepath = AppDomain.CurrentDomain.BaseDirectory;
                }

               
                
              
            }
        }

        
     
        public static void WriteToLogFromService(LoggerEnum enumValue, string Message, string fileName)
        {
            WriteToLog(enumValue, Message, fileName);
        }

       
       

        /// <summary>
        /// Writes to log.
        /// </summary>
        /// <param name="message">The message.</param>
        

        /// <summary>
        /// Writes to log.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="logType">Type of the log.</param>
        /// <param name="message">The message.</param>
       

        public static void WriteToLog(LoggerEnum enumValue, string message, string fileName)
        {
            try
            {
                string rgPattern = @"[\\\/:\*\?""<>|]";
                Regex oRegex = new Regex(rgPattern);
                //LoggerEnum enumValue = LoggerEnum.INFO;

                fileName = oRegex.Replace(fileName, "");
                try
                {
                    _exepath = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
                    _exepath = _exepath + "\\";
                }
                catch
                {
                    _exepath = AppDomain.CurrentDomain.BaseDirectory;
                }

                string filePath = _exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH") + "\\" + fileName + ".txt";

                //Console.WriteLine("_LoggerMode" + _LoggerMode.ToString());
                //Console.WriteLine("LoggerEnum" + enumValue.ToString());

                bool isLogCommit = true;/* (LoggerEnum)Enum.Parse(typeof(LoggerEnum), enumValue.ToString(), true) >= (LoggerEnum)Enum.Parse(typeof(LoggerEnum), _LoggerMode.ToString(), true) ? true : false;*/

                if (isLogCommit)
                {
                    if (!Directory.Exists(_exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH")))
                    {
                        Directory.CreateDirectory(_exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH"));
                        string strDataPath = _exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH");
                        FileIOPermission fileIOPerm1 = new FileIOPermission(PermissionState.Unrestricted);
                        fileIOPerm1.SetPathList(FileIOPermissionAccess.AllAccess, strDataPath);
                    }

                    using (StreamWriter s = File.AppendText(filePath))
                    {
                        s.WriteLine(String.Concat(DateTime.Now, "\t: ", enumValue.ToString() + ":" + message));
                        s.Flush();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }
        }

     
        public static void WriteToLog(LoggerEnum enumValue, string message, string MethodName, string fileName)
        {
            try
            {
                string rgPattern = @"[\\\/:\*\?""<>|]";
                Regex oRegex = new Regex(rgPattern);

                fileName = oRegex.Replace(fileName, "");
                try
                {
                    _exepath = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
                    _exepath = _exepath + "\\";
                }
                catch
                {
                    _exepath = AppDomain.CurrentDomain.BaseDirectory;
                }

                string filePath = _exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH") + "\\" + fileName + ".txt";

                bool isLogCommit = (LoggerEnum)Enum.Parse(typeof(LoggerEnum), enumValue.ToString(), true) >= (LoggerEnum)Enum.Parse(typeof(LoggerEnum), _LoggerMode.ToString(), true) ? true : false;

                if (isLogCommit)
                {
                    if (!Directory.Exists(_exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH")))
                    {
                        Directory.CreateDirectory(_exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH"));
                        string strDataPath = _exepath + "Logs" + DateTime.UtcNow.ToString("yyyy_MM_dd_HH");
                        FileIOPermission fileIOPerm1 = new FileIOPermission(PermissionState.Unrestricted);
                        fileIOPerm1.SetPathList(FileIOPermissionAccess.AllAccess, strDataPath);
                    }

                    using (StreamWriter s = File.AppendText(filePath))
                    {
                        s.WriteLine(String.Concat(DateTime.UtcNow.ToString("yyyy_MM_dd_HH"), "\t: ", enumValue.ToString() + ":" + message));
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }
        }

      
        
        /// <summary>
        /// Writes to log exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="MethodName">Name of the method.</param>
        /// <param name="fileName">Name of the file.</param>
        public static void WriteToLogException(string message, string MethodName, string fileName)
        {
            WriteToLog(LoggerEnum.ERROR, message, MethodName, fileName);
        }

    }

    public enum MethodType
    {
        START,
        END
    }

    public enum MethodName
    {
        ONSTART,
        MESSAGERECEIVESTART,
        PROCESSANDSENDDATA,
        MESSAGERECEIVECOMPLETE,
        INSERTCALLDATAINDATAMART,
        ONSTOP
    }

    public enum LoggerEnum
    {
        INFO,
        TRACE,
        DEBUG,
        TIMESPAN,
        ERROR,
        DATA
    }

    public enum ActionType
    {
        Start,
        End
    }
}

