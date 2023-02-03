using System;
using System.Data;
using System.Data.OracleClient;
using MySql.Data.MySqlClient;

namespace HTCCosmoGetFgInbound
{
    internal class DatabaseService
    {
        private static string getPrdVersion(string sern)
        {
            return sern.Trim().Substring(11, 2);
        }

        public static void rfGetFgInbound()
        {
            MySqlConnection getCosmoConnection = DatabaseClass.CosmoWmsConnection();
            OracleConnection getHtcConnection = DatabaseClass.HtcWmsConnection();

            String getCosmoFgInboundStatement = string.Format("SELECT a.row_id AS 'id'," +
                "DATE_FORMAT(a.created_date, '%Y-%m-%d %H:%i:%s') AS 'date_in'," +
                "a.barcode AS 'sern'," +
                "a.product_code AS 'mat_code'," +
                "a.order_no AS 'order_no'," +
                "b.send_spot AS 'ware_id'," +
                "a.factory_code AS 'plant'," +
                "a.qty AS 'qty' " +
                "FROM cosmo_wms_9771.ods_pro_storage_records_sn a " +
                "LEFT JOIN cosmo_base.md_send_spot b ON a.factory_code = b.factory_code AND a.loc_code = b.user_defined2 " +
                "WHERE a.yd_type = '{0}' AND a.oi_type = '{1}' AND a.add1 IS NULL " +
                "ORDER BY a.created_date ",
                "SHRK",
                "10");

            Console.WriteLine("Query SQL? {0}", getCosmoFgInboundStatement);

            DataTable cosmoResultQuery = DatabaseClass.MySqlExecuteQuery(getCosmoConnection, getCosmoFgInboundStatement);

            int cosmoResultQueryCount = cosmoResultQuery.Rows.Count;
            Console.WriteLine("FG Inbound? {0}", cosmoResultQueryCount.ToString());

            if (cosmoResultQueryCount > 0)
            {
                foreach (DataRow row in cosmoResultQuery.Rows)
                {
                    String postHtcFgInoutStatement = string.Format("INSERT INTO FG_INOUT (ROW_ID,DATE_IN,SERN,MAT_CODE,DC_COLLECTED,DBFLAG,ORDER_ID,DATE_OFFLINE,IN_FLAG,WARE_ID,PLAC_ID,PLANT,CREATED_BY,LAST_UPD_BY,QTY,UNIT,STATUS,OFFLINE_FLAG,PRD_VERSION,PRD_LINE,STATUS_HOLD) " +
                        "VALUES (F_GET_ROW_ID('FG_INOUT'), " +
                        "TO_DATE('{0}', 'YYYY-MM-DD HH24:MI:SS'), " +
                        "'{1}', " +
                        "'{2}', " +
                        "'1', '0', " +
                        "'{3}', " +
                        "TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS'), " +
                        "'ZCR', " +
                        "'{5}', " +
                        "'0088', " +
                        "'{6}', " +
                        "'BGDCOSMOWMS', 'BGDCOSMOWMS', " +
                        "'1', 'PC', '0', '1', 'NONE', " +
                        "'{7}', '0')",
                        row["date_in"].ToString().Trim(),
                        row["sern"].ToString().Trim(),
                        row["mat_code"].ToString().Trim(),
                        row["order_no"].ToString().Trim(),
                        row["date_in"].ToString().Trim(),
                        row["ware_id"].ToString().Trim(),
                        row["plant"].ToString().Trim(),
                        getPrdVersion(row["sern"].ToString().Trim()));

                    Console.WriteLine("Query SQL? {0}", postHtcFgInoutStatement);

                    int htcResultNonQuery = DatabaseClass.OracleExecuteNonQuery(getHtcConnection, postHtcFgInoutStatement);
                    System.Threading.Thread.Sleep(25);

                    if (htcResultNonQuery == 1)
                    {
                        String postCosmoUpdateStatement = string.Format("UPDATE cosmo_wms_9771.ods_pro_storage_records_sn SET add1 = '1' WHERE row_id = '{0}'", row["id"].ToString().Trim());

                        Console.WriteLine("Query SQL? {0}", postCosmoUpdateStatement);

                        int cosmoResultNonQuery = DatabaseClass.MySqlExecuteNonQuery(getCosmoConnection, postCosmoUpdateStatement);
                        System.Threading.Thread.Sleep(25);

                        if (cosmoResultNonQuery == 1)
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failure");
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("Success");
            }

            System.Threading.Thread.Sleep(5000);
        }

        public static void wacGetFgInbound()
        {
            MySqlConnection getCosmoConnection = DatabaseClass.CosmoWmsConnection();
            OracleConnection getHtcConnection = DatabaseClass.HtcWmsConnection();

            String getCosmoFgInboundStatement = string.Format("SELECT a.row_id AS 'id'," +
                "DATE_FORMAT(a.created_date, '%Y-%m-%d %H:%i:%s') AS 'date_in'," +
                "a.barcode AS 'sern'," +
                "a.product_code AS 'mat_code'," +
                "a.order_no AS 'order_no'," +
                "b.send_spot AS 'ware_id'," +
                "a.factory_code AS 'plant'," +
                "a.qty AS 'qty' " +
                "FROM cosmo_wms_9773.ods_pro_storage_records_sn a " +
                "LEFT JOIN cosmo_base.md_send_spot b ON a.factory_code = b.factory_code AND a.loc_code = b.user_defined2 " +
                "WHERE a.yd_type = '{0}' AND a.oi_type = '{1}' AND a.add1 IS NULL " +
                "ORDER BY a.created_date ",
                "SHRK",
                "10");

            Console.WriteLine("Query SQL? {0}", getCosmoFgInboundStatement);

            DataTable cosmoResultQuery = DatabaseClass.MySqlExecuteQuery(getCosmoConnection, getCosmoFgInboundStatement);

            int cosmoResultQueryCount = cosmoResultQuery.Rows.Count;
            Console.WriteLine("FG Inbound? {0}", cosmoResultQueryCount.ToString());

            if (cosmoResultQueryCount > 0)
            {
                foreach (DataRow row in cosmoResultQuery.Rows)
                {
                    String postHtcFgInoutStatement = string.Format("INSERT INTO FG_INOUT (ROW_ID,DATE_IN,SERN,MAT_CODE,DC_COLLECTED,DBFLAG,ORDER_ID,DATE_OFFLINE,IN_FLAG,WARE_ID,PLAC_ID,PLANT,CREATED_BY,LAST_UPD_BY,QTY,UNIT,STATUS,OFFLINE_FLAG,PRD_VERSION,PRD_LINE,STATUS_HOLD) " +
                        "VALUES (F_GET_ROW_ID('FG_INOUT'), " +
                        "TO_DATE('{0}', 'YYYY-MM-DD HH24:MI:SS'), " +
                        "'{1}', " +
                        "'{2}', " +
                        "'1', '0', " +
                        "'{3}', " +
                        "TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS'), " +
                        "'ZCR', " +
                        "'{5}', " +
                        "'0088', " +
                        "'{6}', " +
                        "'BGDCOSMOWMS', 'BGDCOSMOWMS', " +
                        "'1', 'PC', '0', '1', 'NONE', " +
                        "'{7}', '0')",
                        row["date_in"].ToString().Trim(),
                        row["sern"].ToString().Trim(),
                        row["mat_code"].ToString().Trim(),
                        row["order_no"].ToString().Trim(),
                        row["date_in"].ToString().Trim(),
                        row["ware_id"].ToString().Trim(),
                        row["plant"].ToString().Trim(),
                        getPrdVersion(row["sern"].ToString().Trim()));

                    Console.WriteLine("Query SQL? {0}", postHtcFgInoutStatement);

                    int htcResultNonQuery = DatabaseClass.OracleExecuteNonQuery(getHtcConnection, postHtcFgInoutStatement);
                    System.Threading.Thread.Sleep(25);

                    if (htcResultNonQuery == 1)
                    {
                        String postCosmoUpdateStatement = string.Format("UPDATE cosmo_wms_9773.ods_pro_storage_records_sn SET add1 = '1' WHERE row_id = '{0}'", row["id"].ToString().Trim());

                        Console.WriteLine("Query SQL? {0}", postCosmoUpdateStatement);

                        int cosmoResultNonQuery = DatabaseClass.MySqlExecuteNonQuery(getCosmoConnection, postCosmoUpdateStatement);
                        System.Threading.Thread.Sleep(25);

                        if (cosmoResultNonQuery == 1)
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failure");
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("Success");
            }

            System.Threading.Thread.Sleep(5000);
        }

        public static void sacGetFgInbound()
        {
            MySqlConnection getCosmoConnection = DatabaseClass.CosmoWmsConnection();
            OracleConnection getHtcConnection = DatabaseClass.HtcWmsConnection();

            String getCosmoFgInboundStatement = string.Format("SELECT a.row_id AS 'id'," +
                "DATE_FORMAT(a.created_date, '%Y-%m-%d %H:%i:%s') AS 'date_in'," +
                "a.barcode AS 'sern'," +
                "a.product_code AS 'mat_code'," +
                "a.order_no AS 'order_no'," +
                "b.send_spot AS 'ware_id'," +
                "a.factory_code AS 'plant'," +
                "a.qty AS 'qty' " +
                "FROM cosmo_wms_9774.ods_pro_storage_records_sn a " +
                "LEFT JOIN cosmo_base.md_send_spot b ON a.factory_code = b.factory_code AND a.loc_code = b.user_defined2 " +
                "WHERE a.yd_type = '{0}' AND a.oi_type = '{1}' AND a.add1 IS NULL " +
                "ORDER BY a.created_date ",
                "SHRK",
                "10");

            Console.WriteLine("Query SQL? {0}", getCosmoFgInboundStatement);

            DataTable cosmoResultQuery = DatabaseClass.MySqlExecuteQuery(getCosmoConnection, getCosmoFgInboundStatement);

            int cosmoResultQueryCount = cosmoResultQuery.Rows.Count;
            Console.WriteLine("FG Inbound? {0}", cosmoResultQueryCount.ToString());

            if (cosmoResultQueryCount > 0)
            {
                foreach (DataRow row in cosmoResultQuery.Rows)
                {
                    String postHtcFgInoutStatement = string.Format("INSERT INTO FG_INOUT (ROW_ID,DATE_IN,SERN,MAT_CODE,DC_COLLECTED,DBFLAG,ORDER_ID,DATE_OFFLINE,IN_FLAG,WARE_ID,PLAC_ID,PLANT,CREATED_BY,LAST_UPD_BY,QTY,UNIT,STATUS,OFFLINE_FLAG,PRD_VERSION,PRD_LINE,STATUS_HOLD) " +
                        "VALUES (F_GET_ROW_ID('FG_INOUT'), " +
                        "TO_DATE('{0}', 'YYYY-MM-DD HH24:MI:SS'), " +
                        "'{1}', " +
                        "'{2}', " +
                        "'1', '0', " +
                        "'{3}', " +
                        "TO_DATE('{4}', 'YYYY-MM-DD HH24:MI:SS'), " +
                        "'ZCR', " +
                        "'{5}', " +
                        "'0088', " +
                        "'{6}', " +
                        "'BGDCOSMOWMS', 'BGDCOSMOWMS', " +
                        "'1', 'PC', '0', '1', 'NONE', " +
                        "'{7}', '0')",
                        row["date_in"].ToString().Trim(),
                        row["sern"].ToString().Trim(),
                        row["mat_code"].ToString().Trim(),
                        row["order_no"].ToString().Trim(),
                        row["date_in"].ToString().Trim(),
                        row["ware_id"].ToString().Trim(),
                        row["plant"].ToString().Trim(),
                        getPrdVersion(row["sern"].ToString().Trim()));

                    Console.WriteLine("Query SQL? {0}", postHtcFgInoutStatement);

                    int htcResultNonQuery = DatabaseClass.OracleExecuteNonQuery(getHtcConnection, postHtcFgInoutStatement);
                    System.Threading.Thread.Sleep(25);

                    if (htcResultNonQuery == 1)
                    {
                        String postCosmoUpdateStatement = string.Format("UPDATE cosmo_wms_9774.ods_pro_storage_records_sn SET add1 = '1' WHERE row_id = '{0}'", row["id"].ToString().Trim());

                        Console.WriteLine("Query SQL? {0}", postCosmoUpdateStatement);

                        int cosmoResultNonQuery = DatabaseClass.MySqlExecuteNonQuery(getCosmoConnection, postCosmoUpdateStatement);
                        System.Threading.Thread.Sleep(25);

                        if (cosmoResultNonQuery == 1)
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failure");
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("Success");
            }

            System.Threading.Thread.Sleep(5000);
        }
    }
}
