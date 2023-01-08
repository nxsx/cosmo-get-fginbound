using System;
using System.Data;
using System.Data.OracleClient;
using MySql.Data.MySqlClient;

namespace HTCCosmoGetFgInbound
{
    internal class DatabaseClass
    {
        public static OracleConnection HtcWmsConnection()
        {
            OracleConnectionStringBuilder _htcwmsConnectionStringBuilder = new OracleConnectionStringBuilder();
            _htcwmsConnectionStringBuilder.DataSource = "DataSource";
            _htcwmsConnectionStringBuilder.UserID = "UserID";
            _htcwmsConnectionStringBuilder.Password = "Password";
            _htcwmsConnectionStringBuilder.Unicode = true;

            OracleConnection _htcwmsConnection = new OracleConnection(_htcwmsConnectionStringBuilder.ConnectionString);
            return _htcwmsConnection;
        }

        public static DataTable OracleExecuteQuery(OracleConnection conn, String sql)
        {
            DataTable dt = new DataTable();
            DataSet ds = new DataSet();
            
            conn.Open();

            using (OracleCommand cmd = new OracleCommand())
            {
                cmd.Connection = conn;
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;

                using (OracleDataAdapter adap = new OracleDataAdapter())
                {
                    adap.SelectCommand = cmd;
                    adap.Fill(ds);
                    dt = ds.Tables[0];
                }
            }

            conn.Close();
            return dt;
        }

        public static int OracleExecuteNonQuery(OracleConnection conn, String sql)
        {
            int result = 0;

            conn.Open();

            using (OracleCommand cmd = new OracleCommand())
            {
                cmd.Connection = conn;
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;

                result = cmd.ExecuteNonQuery();
            }

            conn.Close();
            return result;
        }

        public static MySqlConnection CosmoWmsConnection()
        {
            MySqlConnectionStringBuilder _cosmowmsConnectionStringBuilder = new MySqlConnectionStringBuilder();
            _cosmowmsConnectionStringBuilder.Server = "Server";
            _cosmowmsConnectionStringBuilder.UserID = "UserId";
            _cosmowmsConnectionStringBuilder.Password = "Password";

            MySqlConnection _cosmowmsConnection = new MySqlConnection(_cosmowmsConnectionStringBuilder.ConnectionString);
            return _cosmowmsConnection;
        }

        public static DataTable MySqlExecuteQuery(MySqlConnection conn, String sql)
        {
            DataTable dt = new DataTable();
            DataSet ds = new DataSet();

            conn.Open();

            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.Connection = conn;
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;

                using (MySqlDataAdapter adap = new MySqlDataAdapter())
                {
                    adap.SelectCommand = cmd;
                    adap.Fill(ds);
                    dt = ds.Tables[0];
                }
            }

            conn.Close();
            return dt;
        }

        public static int MySqlExecuteNonQuery(MySqlConnection conn, String sql)
        {
            int result = 0;

            conn.Open();

            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.Connection = conn;
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;

                result = cmd.ExecuteNonQuery();
            }

            conn.Close();
            return result;
        }
    }
}
