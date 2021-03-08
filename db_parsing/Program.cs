using System;
using System.IO;

using System.Text;
using MySql.Data.MySqlClient;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Data;
using System.Data.OleDb;

using ExportJson;

namespace db_parsing
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var dbfPath = "./sample3.dbf";
            // exportjson.Dbf2Json(dbfPath);
            exportjson.Sql2Json("test02");
            // createTable();
            // insertDB();
            //search();
        }

        static void createTable()
        {
            try
            {
                string strConn = "Server=localhost;Database=sample;Uid=root;Pwd=test;";
                MySqlConnection conn = new MySqlConnection(strConn);
                conn.Open();

                String insertQuery = "create table test02(dumy INT(6), title VARCHAR(20), score INT(3), descr VARCHAR(50), watched BOOL)";
                MySqlCommand command = new MySqlCommand(insertQuery, conn);
                if (command.ExecuteNonQuery() == 1)
                {
                    Console.WriteLine("table 생성 실패");
                }
                else
                {
                    Console.WriteLine("table 생성 성공");
                }
            }

            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        static void insertDB()
        {
            try
            {
                string strConn = "Server=localhost;Database=sample;Uid=root;Pwd=test;";
                MySqlConnection conn = new MySqlConnection(strConn);
                conn.Open();

                String insertQuery = "INSERT INTO test02(dumy,title,score) VALUES(1,'test', 100)";
                MySqlCommand command = new MySqlCommand(insertQuery, conn);
                if (command.ExecuteNonQuery() == 1)
                {
                    Console.WriteLine("insert 성공");
                }
                else
                {
                    Console.WriteLine("insert 실패");
                }
            }

            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        static void search()
        {
            using (MySqlConnection connection = new MySqlConnection("Server=localhost;Database=sample;Uid=root;Pwd=test"))
            {
                try
                {
                    connection.Open();
                    string sql = "SELECT * FROM test02";

                    //ExecuteReader를 이용하여
                    //연결 모드로 데이터 가져오기
                    MySqlCommand cmd = new MySqlCommand(sql, connection);
                    MySqlDataReader table = cmd.ExecuteReader();
                    JArray data_arr = new JArray();

                    string json_str = sqlDatoToJson(table);
                    Console.WriteLine(json_str);
                    //exportjson.Main(json_str);


                    Console.WriteLine(data_arr);
                    table.Close();


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
