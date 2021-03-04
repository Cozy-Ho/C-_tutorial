using System;
using System.IO;
using DbfDataReader;
using System.Text;

namespace db_parsing
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
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
                        // Console.WriteLine(dbfValue.GetType());
                        if (dbfValue.GetType() == typeof(DbfDataReader.DbfValueMemo))
                        {
                            var obj = dbfValue.GetValue();
                            Console.WriteLine(obj);
                        }
                        else
                        {
                            var stringValue = dbfValue.ToString();
                            Console.WriteLine(stringValue);

                        }
                        // var obj = dbfValue.GetValue();
                    }
                }
            }
        }
    }
}
