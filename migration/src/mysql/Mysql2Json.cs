using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using MySql.Data.MySqlClient;
using System.Data;
using System.Data.SqlClient;

namespace Mysql2Json
{
    class mysql2json
    {

        // windows ip addr
        static string strIP = "172.26.50.8";
        static string strPort = "1433";
        static string strID = "sa";
        static string strPW = "qwe123!@#";
        public static void Sql2Json(String filename, String tableName)
        {
            string strDatabase = "HANARO";
            try
            {
                string constring = "server=" + strIP + "," + strPort + ";database=" + strDatabase + ";uid=" + strID + ";pwd=" + strPW;
                string sql = "select * from tb_staff_info";

                using (SqlConnection connection = new SqlConnection(constring))
                {
                    SqlCommand command = new SqlCommand(sql, connection);
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();
                    string json_str = sqlDatoToJson(reader);
                    StreamWriter fs = new StreamWriter(new FileStream("./export_data/" + filename + ".json", FileMode.Create));
                    fs.WriteLine(json_str);
                    fs.Close();
                    reader.Close();
                }

                Console.WriteLine(">>> SQL_EXPORT_DONE");
            }
            catch (Exception ex)
            {
                Console.WriteLine("실패");
                Console.WriteLine(ex.ToString());
            }


            // DB Setting;
            // using (MySqlConnection connection = new MySqlConnection("Server=172.26.50.8;Database=HANARO;Port=1433;Uid=sa;Pwd=qwe123!@#"))
            // {
            //     try
            //     {
            //         connection.Open();
            //         string sql = "SELECT * FROM " + tableName;

            //         //ExecuteReader를 이용하여
            //         //연결 모드로 데이터 가져오기
            //         MySqlCommand cmd = new MySqlCommand(sql, connection);
            //         MySqlDataReader table = cmd.ExecuteReader();

            //         string json_str = sqlDatoToJson(table);

            //         StreamWriter fs = new StreamWriter(new FileStream("./export_data/" + filename + ".json", FileMode.Create));
            //         fs.WriteLine(json_str);
            //         fs.Close();
            //         table.Close();
            //         Console.WriteLine(">>> SQL_EXPORT_DONE");
            //     }
            //     catch (Exception ex)
            //     {
            //         Console.WriteLine("실패");
            //         Console.WriteLine(ex.ToString());
            //     }
            // }
        }
        static String sqlDatoToJson(SqlDataReader dataReader)
        {
            var dataTable = new DataTable();
            dataTable.Load(dataReader);
            string JSONString = string.Empty;
            JSONString = JsonConvert.SerializeObject(dataTable);
            return JSONString;
        }

    }
}