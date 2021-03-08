using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using MySql.Data.MySqlClient;
using System.Data;


namespace Mysql2Json
{
    class mysql2json
    {
        public static void Sql2Json(String filename, String tableName)
        {
            // DB Setting;
            using (MySqlConnection connection = new MySqlConnection("Server=localhost;Database=sample;Uid=root;Pwd=test"))
            {
                try
                {
                    connection.Open();
                    string sql = "SELECT * FROM " + tableName;

                    //ExecuteReader를 이용하여
                    //연결 모드로 데이터 가져오기
                    MySqlCommand cmd = new MySqlCommand(sql, connection);
                    MySqlDataReader table = cmd.ExecuteReader();

                    string json_str = sqlDatoToJson(table);

                    StreamWriter fs = new StreamWriter(new FileStream("./export_data/" + filename + ".json", FileMode.Create));
                    fs.WriteLine(json_str);
                    fs.Close();
                    table.Close();
                    Console.WriteLine(">>> SQL_EXPORT_DONE");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("실패");
                    Console.WriteLine(ex.ToString());
                }
            }
        }
        static String sqlDatoToJson(MySqlDataReader dataReader)
        {
            var dataTable = new DataTable();
            dataTable.Load(dataReader);
            string JSONString = string.Empty;
            JSONString = JsonConvert.SerializeObject(dataTable);
            return JSONString;
        }

    }
}