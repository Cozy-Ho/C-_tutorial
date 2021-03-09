using System;
using System.IO;
using System.Diagnostics;
using System.Linq;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using ConvertLogic;

using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using ConvertLogic;

using CreateTable;
using DeleteTable;
using Dynamo2Json;
using Json2Dynamo;
using Mysql2Json;

using System.Configuration;

namespace migration
{
    class migration
    {
        static AppSettingsReader ar = new AppSettingsReader();

            static String key = (string)ar.GetValue("AWS_KEY", typeof(string));
            static String secrete_key = (string)ar.GetValue("AWS_SECRETE_KEY", typeof(string));
            static AmazonDynamoDBClient client = new AmazonDynamoDBClient(key, secrete_key, RegionEndpoint.APNortheast2);
        static void Main(string[] args)
        {
            try
            {
                Boolean flag = true;
                while (flag)
                {
                    Console.WriteLine("=====================================");
                    Console.WriteLine("1 : Create TEST Table");
                    Console.WriteLine("2 : Delete Table");
                    Console.WriteLine("3 : Export Data");
                    Console.WriteLine("4 : Import Data");
                    Console.WriteLine("5 : Convert Data");
                    Console.WriteLine("6 : Mysql to Json");
                    Console.WriteLine("0 : EXIT_PORGRAM");
                    Console.WriteLine("=====================================");
                    Console.WriteLine("명령어를 입력하세요 ( 0 ~ 5 )");
                    Console.WriteLine("=====================================");
                    String command = Console.ReadLine();
                    switch (command)
                    {
                        case "1":
                            {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Tablename to CREATE");
                                String tableName = Console.ReadLine();
                                createtable.CreateExampleTable(tableName, client);
                                break;
                            }
                        case "2":
                            {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Tablename to DELETE");
                                String tableName = Console.ReadLine();
                                deletetable.DeleteTable(tableName, client);
                                break;
                            }
                        case "3":
                            {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Tablename");
                                String tableName = Console.ReadLine();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Filename to save data");
                                String filename = Console.ReadLine();
                                Stopwatch stopwatch = new Stopwatch();
                                stopwatch.Start();
                                dynamo2json.Export(filename, tableName, client);
                                // dynamo2json.Export_low(filename, tableName);
                                stopwatch.Stop();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("실행시간 : " + (stopwatch.ElapsedMilliseconds / 1000).ToString() + "s");
                                Console.WriteLine("=====================================");
                                break;
                            }
                        case "4":
                            {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Tablename");
                                String tableName = Console.ReadLine();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Filename to save data");
                                String filename = Console.ReadLine();
                                Stopwatch stopwatch = new Stopwatch();
                                stopwatch.Start();
                                // json2dynamo.Import_low(filename, tableName);
                                json2dynamo.Import(filename, tableName, client);
                                stopwatch.Stop();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("실행시간 : " + (stopwatch.ElapsedMilliseconds / 1000).ToString() + "s");
                                Console.WriteLine("=====================================");
                                break;
                            }
                        case "5":
                            {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Filename to Convert data");
                                String input_filename = Console.ReadLine();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Output Filename");
                                String output_filename = Console.ReadLine();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("적용할 규칙 ( add, update, delete )");
                                String rule = Console.ReadLine();
                                Convert(input_filename, output_filename, rule);
                                break;
                            }
                        case "6":
                        {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input MySQL tablename");
                                String tableName = Console.ReadLine();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input result_file name");
                                String filename = Console.ReadLine();
                                mysql2json.Sql2Json(filename, tableName);
                                break;
                        }
                        case "0":
                            {
                                flag = false;
                                break;
                            }
                    }
                }

            }
            catch (AmazonDynamoDBException e) { Console.WriteLine(e.Message); }
            catch (AmazonServiceException e) { Console.WriteLine(e.Message); }
            catch (Exception e) { Console.WriteLine(e.Message); }
        }

        private static void Convert(String input_filename, String output_filename, String rule)
        {
            var r = File.ReadAllText(input_filename + @".json");
            JArray movies = JArray.Parse(r);
            Console.WriteLine(movies.Count());
            JArray ret_arr = null;

            if (rule.ToUpper() == "ADD")
            {
                ret_arr = convert.add(movies);
            }
            else if (rule.ToUpper() == "UPDATE")
            {
                ret_arr = convert.update(movies);
            }
            else if (rule.ToUpper() == "DELETE")
            {
                ret_arr = convert.delete(movies);
            }

            // Console.WriteLine(movies);
            FileStream fs = new FileStream(output_filename + ".json", FileMode.Create, FileAccess.Write);
            StreamWriter sw = new StreamWriter(fs, System.Text.Encoding.UTF8);
            sw.Write(ret_arr);
            sw.Close();
            fs.Close();
        }
    }
}