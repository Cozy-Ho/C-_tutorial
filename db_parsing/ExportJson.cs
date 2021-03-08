using System;
using System.IO;
using Newtonsoft.Json.Linq;
using DbfDataReader;


namespace ExportJson
{
    class exportjson
    {
        public static void Main(String datas)
        {
            string dir = System.Environment.CurrentDirectory;
            Console.WriteLine(dir);
        }
        public static void Dbf2Json(String dbfPath)
        {
            //var dbfPath = "../../../sample3.dbf";
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

                StreamWriter fs = new StreamWriter(new FileStream(String.Format("../../../export.json"), FileMode.Create));
                fs.WriteLine(arr.ToString());
                fs.Close();

            }
            Console.WriteLine("EXPORT DONE >>> ");
        }
    }
}