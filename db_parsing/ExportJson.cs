using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using DbfDataReader;
using MySql.Data.MySqlClient;
using System.Data;


namespace ExportJson
{
    class exportjson
    {
        // static void Main(String[] args)
        // {
        //     string dir = System.Environment.CurrentDirectory;
        //     Console.WriteLine(dir);
        // }
        public static void Sql2Json(String tableName)
        {
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
                    JArray data_arr = new JArray();

                    string json_str = sqlDatoToJson(table);

                    StreamWriter fs = new StreamWriter(new FileStream("./export_data/sql_export.json", FileMode.Create));
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
        public static void Dbf2Json(String dbfPath)
        {
            var options = new DbfDataReaderOptions
            {
                SkipDeletedRecords = true,
            };
            JArray arr = new JArray();
            using (var reader = new DbfDataReader.DbfDataReader(dbfPath, options))
            {
                while (reader.Read())
                {
                    JObject data = new JObject();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.DbfRecord.Values[i].GetType() == typeof(DbfDataReader.DbfValueMemo))
                        {
                            continue;
                        }
                        else
                        {
                            if (reader.DbfRecord.Values[i].GetType() == typeof(DbfDataReader.DbfValueInt))
                            {
                                data.Add(reader.GetName(i), Convert.ToInt32(reader.DbfRecord.Values[i].ToString()));
                            }
                            else if (reader.DbfRecord.Values[i].GetType() == typeof(DbfDataReader.DbfValueDecimal))
                            {
                                data.Add(reader.GetName(i), Convert.ToDecimal(reader.DbfRecord.Values[i].ToString()));
                            }
                            else if (reader.DbfRecord.Values[i].GetType() == typeof(DbfDataReader.DbfValueBoolean))
                            {
                                data.Add(reader.GetName(i), reader.DbfRecord.Values[i].ToString() == "T" ? true : false);
                            }
                            else
                            {
                                data.Add(reader.GetName(i), reader.DbfRecord.Values[i].ToString());
                            }
                        }
                    }
                    arr.Add(data);
                }

                StreamWriter fs = new StreamWriter(new FileStream("./export_data/export.json", FileMode.Create));
                fs.WriteLine(arr.ToString());
                fs.Close();

            }
            Console.WriteLine(">>> EXPORT DONE ");
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