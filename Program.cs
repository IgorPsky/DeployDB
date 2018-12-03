using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Threading;

namespace DeployDB
{
    class Props
    {
        const string CHANGELOG_DIRECTORY_KEY = "-d";
        const string MODE_KEY = "-m";
        const string PROPS_FILE_KEY = "-p";

        private static Props props = null;

        public enum Mode {DEPLOY, CHECKONLY};

        private Dictionary<string, string> parsedArgs;
        private NameValueCollection parsedProps;

        public string changelogRootDir()
        {
            return getArg(CHANGELOG_DIRECTORY_KEY, Directory.GetCurrentDirectory());
        }

        public string getArg(string propName, string defValue)
        {
            try
            {
                return parsedArgs[propName];
            }
            catch(KeyNotFoundException)
            {
                return defValue;
            }
        }

        public string getProp(string propName)
        {
            return parsedProps[propName];
        }

        private Props(string[] args)
        {
            parsedArgs = new Dictionary<string, string>(args.Length/2);
            for (int i = 0; i < args.Length; i += 2)
            {
                parsedArgs.Add(args[i], args[i + 1]);
            }
            parsedProps = ConfigurationManager.AppSettings;
            Console.WriteLine(parsedProps.Count);
        }

        public static Props getProps(string[] args)
        {
            if(props is null) {
                props = new Props(args);
            }
            return props;
        }

        public Mode deployMode()
        {
            switch(getArg(MODE_KEY, "DEPLOY"))
            {
                case "DEPLOY":
                    return Mode.DEPLOY;
                case "CHECKONLY":
                    return Mode.CHECKONLY;
                default:
                    throw new System.ArgumentException("Wrong mode");
            }
        }

        public string propertyFileName ()
        {
            return getArg(PROPS_FILE_KEY, changelogRootDir());
        }
    }

    class Database
    {
        private static Dictionary<string, Database> databases = new Dictionary<string, Database>();
        private Props props;
        private string databaseMnemonicName;
        private SqlConnection connection = null;
        private string connectionString = "";
        private SqlConnectionStringBuilder connectionStringBuilder;
        protected Database(string databaseMnemonicName, Props props) {
            this.props = props;
            this.databaseMnemonicName = databaseMnemonicName;
            this.connectionString = getProp("connection_string");
            this.connectionStringBuilder = new SqlConnectionStringBuilder(getConnectionString());
        }
        public static Database getDatabase(string databaseMnemonicName, Props props)
        {
            if(databases.ContainsKey(databaseMnemonicName))
            {
                return databases[databaseMnemonicName];
            } else
            {
                Database res = new Database(databaseMnemonicName, props);
                databases.Add(databaseMnemonicName, res);
                return res;
            }
        }

        public string getProp(string propName)
        {
            return props.getProp(databaseMnemonicName + "." + propName);
        }

        public string getDatabaseType()
        {
            return getProp("database_type");
        }
        public string getConnectionString()
        {
            return connectionString;
        }
        public SqlConnection getConnection()
        {
            if(connection is null)
            {
                Console.WriteLine("connecting to: " + getConnectionString());
                string connectionString = getConnectionString();
                if (string.IsNullOrEmpty(connectionString))
                {
                    Console.WriteLine("Unknown database: " + databaseMnemonicName);
                }
                else
                {
                    SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                    connection = new SqlConnection(getConnectionString());
                    connection.Open();
                }
            }
            return connection;
        }

        public string getHost()
        {
            return (string)connectionStringBuilder["Data Source"];
        }
        public bool getIntegratedSecurity()
        {
            return (bool)connectionStringBuilder["Integrated Security"];
        }
        public string getInitialCatalog()
        {
            return (string)connectionStringBuilder["Initial Catalog"];
        }
        public string getUserID()
        {
            return ((string)connectionStringBuilder["User ID"]);
        }
        public string getPassword()
        {
            return ((string)connectionStringBuilder["password"]);
        }
    }

    class MsSqlDatabase : Database
    {
        private MsSqlDatabase(string databaseMnemonicName, Props props) : base(databaseMnemonicName, props) { }
    }

    class ChangelogItem : IComparable<ChangelogItem>
    {
        private string changeNo;
        private string fileName;
        private string dbName;

        private FileInfo file;
        private Props props;

        public string getChangeNo() { return changeNo; }
        public string getFileName() { return fileName; }
        public string getFullFileName() { return file.FullName; }
        public string getDBName() { return dbName; }

        public int CompareTo(ChangelogItem other)
        {
            return (this.ToString()).CompareTo(other.ToString());
        }

        private ChangelogItem(FileInfo file, Props props)
        {
            this.file = file;
            this.changeNo = file.Directory.Name;
            this.fileName = file.Name;
            this.dbName = file.Directory.Parent.Name;
            this.props = props;
        }

        public static ChangelogItem getChangelogItem(FileInfo file, Props props)
        {
            return new ChangelogItem(file, props);
        }

        public override string ToString() {
            return dbName + ':' + changeNo + ':' + fileName;
        }

        public string getDatabaseMnemonicName()
        {
            return dbName;
        }

        public Database getDatabase()
        {
            return Database.getDatabase(dbName, props);
        }

        private void createChangelogTable()
        {
            executeSql(
                    "CREATE TABLE SNT_CHANGELOG(" +
                    "   CHANGE_NO varchar(255) NOT NULL," +
                    "   CHANGE_FILE varchar(255) NOT NULL," +
                    "   APPLY_DATE datetime NOT NULL," +
                    "   APPLY_STATUS varchar(50)," +
                    "   APPLICATION_LOG varchar(max)" +
                    ") \n" +
                    "GO\n"
                );
        }

        public bool isDeployed()
        {
            SqlConnection conn = getDatabase().getConnection();
            if (!(conn is null))
            {
                using (SqlCommand cmd = new SqlCommand("select 1 from SNT_CHANGELOG where change_no = @changeNo and change_file = @changeFile", conn))
                {
                    SqlParameter changeNoParam = new SqlParameter();
                    changeNoParam.ParameterName = "@changeNo"; changeNoParam.Value = changeNo;
                    cmd.Parameters.Add(changeNoParam);
                    SqlParameter changeFileParam = new SqlParameter();
                    changeFileParam.ParameterName = "@changeFile"; changeFileParam.Value = fileName;
                    cmd.Parameters.Add(changeFileParam);
                    try
                        {
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            return (reader.HasRows);
                        }
                    } catch (SqlException e)
                    {
                        createChangelogTable();
                        return false;
                    }
                }
            }
            else return false;
        }

        public string getSqlCmdParams()
        {
            string sqlCmdCall = "-x -e -H " + getDatabase().getHost() + " -d " + getDatabase().getInitialCatalog();
            if (!getDatabase().getIntegratedSecurity())
            {
                sqlCmdCall = sqlCmdCall + " -U " + getDatabase().getUserID() + " -P " + getDatabase().getPassword();
            }
            return sqlCmdCall;
        }

        public string getSqlCmdParamsScriptFile()
        {
            string sqlCmdCall = getSqlCmdParams();
            sqlCmdCall = sqlCmdCall + " -i " + getFullFileName();
            return sqlCmdCall;
        }

        public string executeSql(string sql)
        {
            String output = "";
            using (Process p = new Process())
            {
                p.StartInfo.FileName = "sqlcmd.exe";
                p.StartInfo.Arguments = getSqlCmdParams();
                p.StartInfo.UseShellExecute = false;
                p.StartInfo.RedirectStandardOutput = true;
                p.StartInfo.RedirectStandardInput = true;
                p.Start();
                p.StandardInput.WriteLine(sql);
                p.StandardInput.WriteLine("exit\n");
                output = p.StandardOutput.ReadToEnd();
                p.WaitForExit();
                Console.WriteLine("Output:");
                Console.WriteLine(output);
            }
            return output;
        }

        public string executeScript(string fileName)
        {
            String output = "";
            using (Process p = new Process())
            {
                p.StartInfo.FileName = "sqlcmd.exe";
                p.StartInfo.Arguments = getSqlCmdParamsScriptFile();
                p.StartInfo.UseShellExecute = false;
                p.StartInfo.RedirectStandardOutput = true;
                p.Start();
                output = p.StandardOutput.ReadToEnd();
                p.WaitForExit();
                Console.WriteLine("Output:");
                Console.WriteLine(output);
            }
            return output;

        }

        private void saveToChangelog(string sessionOutput)
        {
            using (var cmd = new SqlCommand(
                "merge snt_changelog as changelog "+
                "        using (" +
                "           select @changeNo as change_no, @changeFile as change_file, SYSDATETIME() as apply_date, @applyStatus as apply_status, @applicationLog as application_log " +
                "        ) vals " +
                "        on(vals.change_no = changelog.change_no and vals.change_file = changelog.change_file) " +
                "        when matched then " +
                "        update set apply_date = vals.apply_date, apply_status = vals.apply_status, application_log = concat(changelog.application_log, char(13), '---', char(13), vals.application_log) " +
                "        when not matched then " +
                "        insert(change_no, change_file, apply_date, apply_status, application_log) " +
                "        values(vals.change_no, vals.change_file, vals.apply_date, vals.apply_status, vals.application_log); "
                , getDatabase().getConnection()))
            {
                SqlParameter changeNoParam = new SqlParameter();
                changeNoParam.ParameterName = "@changeNo"; changeNoParam.Value = changeNo;
                cmd.Parameters.Add(changeNoParam);
                SqlParameter changeFileParam = new SqlParameter();
                changeFileParam.ParameterName = "@changeFile"; changeFileParam.Value = fileName;
                cmd.Parameters.Add(changeFileParam);
                SqlParameter applyStatusParam = new SqlParameter();
                applyStatusParam.ParameterName = "@applyStatus"; applyStatusParam.Value = "SUCCESS";
                cmd.Parameters.Add(applyStatusParam);
                SqlParameter applicationLogParam = new SqlParameter();
                applicationLogParam.ParameterName = "@applicationLog"; applicationLogParam.Value = sessionOutput;
                cmd.Parameters.Add(applicationLogParam);
                cmd.ExecuteNonQuery();
            }
        }

        public void Deploy()
        {
            if (props.deployMode() == Props.Mode.DEPLOY)
            {
                string output = executeScript(getFullFileName());
                saveToChangelog(output);
                Console.WriteLine(output);
            }
            else Console.WriteLine("Not deploying, CHECKONLY mode selected");
        }

        public void CheckAndDeploy()
        {
            if (!(getDatabase().getConnection() is null)) { 
                if (!isDeployed())
                {
                    Deploy();
                }
            };
        }
    }

    class Program
    {

        private static List<ChangelogItem> changelogList = new List<ChangelogItem>();

        public static void logLine(string line)
        {
            logLine(3, line);
        }

        public static void logLine(int severity, string line)
        {
            Console.WriteLine(line);
        }

        public static void deploy(Props props) {
            logLine("Working directory: " + props.changelogRootDir());
            logLine("Mode is: " + props.deployMode());
            DirectoryInfo rootDir = new DirectoryInfo(props.changelogRootDir());
            foreach (DirectoryInfo dbdir in rootDir.EnumerateDirectories()) // 1st level is databases
            {
                foreach (DirectoryInfo dir in dbdir.EnumerateDirectories()) // 2nd level is changelof items
                {
                    foreach (FileInfo file in dir.EnumerateFiles())
                    {
                        changelogList.Add(ChangelogItem.getChangelogItem(file, props));
                    }
                }
            }
            changelogList.Sort();
            foreach (ChangelogItem item in changelogList) { item.CheckAndDeploy(); }
        }

        static void Main(string[] args)
		{
            Props props = Props.getProps(args);
            deploy(props);
            // Console.ReadKey();
		}
	}
}
