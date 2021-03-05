using System;
using System.IO;
using DbfDataReader;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data;
using System.Data.OleDb;

namespace db_parsing
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            dbf_read();
            // createTable();
            // insertDB();
            //search();
        }
        static void dbf_read()
        {
            /**
            string strConn = "Provider=Microsoft.Jet.OLEDB.4.0;"
                   + @"Data Source=./sample.dbf;";
            OleDbConnection conn = new OleDbConnection(strConn);

            conn.Open();

            Console.WriteLine("Database = \t\t" + conn.Database);
            Console.WriteLine("DataSource = \t\t" + conn.DataSource);
            Console.WriteLine("DataServerVersion = \t" + conn.ServerVersion);
            Console.WriteLine("State = \t\t" + conn.State);
            conn.Close();
            Console.WriteLine("State = \t\t" + conn.State);
            Console.ReadLine();
            */
            //
            var skipDeleted = true;

            var dbfPath = "./sample.dbf";
            using (var dbfTable = new DbfTable(dbfPath, Encoding.UTF8))
            {
                var dbfRecord = new DbfRecord(dbfTable);

                while (dbfTable.Read(dbfRecord))
                {
                    if (skipDeleted && dbfRecord.IsDeleted)
                    {
                        continue;
                    }

                    foreach (var dbfValue in dbfRecord.Values)
                    {
                        Console.WriteLine(dbfValue.GetType());
                        // if (dbfValue.GetType() == typeof(DbfDataReader.DbfValueMemo))
                        // {
                        //     // var obj = dbfValue.GetValue();
                        //     // Console.WriteLine(obj);
                        //     Console.WriteLine(dbfValue.GetType());
                        // }
                        // else
                        // {
                        //     var stringValue = dbfValue.ToString();
                        //     Console.WriteLine(stringValue);

                        // }
                        // var obj = dbfValue.GetValue();
                    }
                }
            }
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

                    while (table.Read())
                    {
                        Console.WriteLine("{0} {1} {2}", table["dumy"], table["title"], table["score"]);
                    }
                    table.Close();

                }
                catch (Exception ex)
                {
                    Console.WriteLine("실패");
                    Console.WriteLine(ex.ToString());
                }

            }
        }
    }
}
