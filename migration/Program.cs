using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Diagnostics;
using Amazon.DynamoDBv2.DataModel;
using System.Threading;
using ConvertLogic;
using DotNetEnv;

namespace migration
{
    [Serializable()]
    public class Movie
    {
        public int dumy { get; set; }
        public string id { get; set; }
        public string title { get; set; }
        public string desc { get; set; }
        public int score { get; set; }
        public bool watched { get; set; }
        public Dictionary<string, string> info { get; set; }
        public string s_title { get; set; }
        public string s_desc { get; set; }
        public int s_score { get; set; }
    }
    class migration
    {
        private static String key = "";
        private static String s_key = "";
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient(key, s_key, RegionEndpoint.APNortheast2);
        private static string tableName = "test02-movie5";

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
                    Console.WriteLine("0 : EXIT_PORGRAM");
                    Console.WriteLine("=====================================");
                    Console.WriteLine("명령어를 입력하세요 ( 0 ~ 5 )");
                    Console.WriteLine("=====================================");
                    String command = Console.ReadLine();
                    switch (command)
                    {
                        case "1":
                            {
                                CreateExampleTable();
                                break;
                            }
                        case "2":
                            {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Tablename to DELETE");
                                tableName = Console.ReadLine();
                                DeleteTable();
                                break;
                            }
                        case "3":
                            {
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Tablename");
                                tableName = Console.ReadLine();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Filename to save data");
                                String filename = Console.ReadLine();
                                Stopwatch stopwatch = new Stopwatch();
                                stopwatch.Start();
                                Export(filename);
                                // Export_low(filename);
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
                                tableName = Console.ReadLine();
                                Console.WriteLine("=====================================");
                                Console.WriteLine("Input Filename to save data");
                                String filename = Console.ReadLine();
                                Stopwatch stopwatch = new Stopwatch();
                                stopwatch.Start();
                                // Import_low(filename);
                                Import(filename);
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

        private static void CreateExampleTable()
        {
            DotNetEnv.Env.Load();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(DotNetEnv.Env.GetString("KEY"), DotNetEnv.Env.GetString("SECRETE_KEY"), RegionEndpoint.APNortheast2);
            Console.WriteLine("\n*** Creating table ***");
            var request = new CreateTableRequest
            {
                AttributeDefinitions = new List<AttributeDefinition>()
            {
                new AttributeDefinition
                {
                    AttributeName = "Id",
                    AttributeType = "N"
                },
                new AttributeDefinition
                {
                    AttributeName = "ReplyDateTime",
                    AttributeType = "N"
                }
            },
                KeySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement
                {
                    AttributeName = "Id",
                    KeyType = "HASH" //Partition key
                },
                new KeySchemaElement
                {
                    AttributeName = "ReplyDateTime",
                    KeyType = "RANGE" //Sort key
                }
            },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 6
                },
                TableName = tableName
            };
            var response = client.CreateTable(request);

            var tableDescription = response.TableDescription;
            Console.WriteLine("{1}: {0} \t ReadsPerSec: {2} \t WritesPerSec: {3}",
                      tableDescription.TableStatus,
                      tableDescription.TableName,
                      tableDescription.ProvisionedThroughput.ReadCapacityUnits,
                      tableDescription.ProvisionedThroughput.WriteCapacityUnits);

            string status = tableDescription.TableStatus;
            Console.WriteLine(tableName + " - " + status);

            WaitUntilTableReady(tableName);
        }

        private static void DeleteTable()
        {
            DotNetEnv.Env.Load();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(DotNetEnv.Env.GetString("KEY"), DotNetEnv.Env.GetString("SECRETE_KEY"), RegionEndpoint.APNortheast2);
            Console.WriteLine("=====================================");
            Console.WriteLine("\n*** Deleting table ***");
            var request = new DeleteTableRequest
            {
                TableName = tableName
            };

            var response = client.DeleteTable(request);

            Console.WriteLine("Table is being deleted...");
            Console.WriteLine("=====================================");
        }

        private static void WaitUntilTableReady(string tableName)
        {
            DotNetEnv.Env.Load();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(DotNetEnv.Env.GetString("KEY"), DotNetEnv.Env.GetString("SECRETE_KEY"), RegionEndpoint.APNortheast2);
            string status = null;
            // Let us wait until table is created. Call DescribeTable.
            do
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                try
                {
                    var res = client.DescribeTable(new DescribeTableRequest
                    {
                        TableName = tableName
                    });

                    Console.WriteLine("Table name: {0}, status: {1}",
                              res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException)
                {
                    // DescribeTable is eventually consistent. So you might
                    // get resource not found. So we handle the potential exception.
                }
            } while (status != "ACTIVE");
        }

        private static void Export_low(String filename)
        {
            // Define scan conditions
            Dictionary<string, Condition> conditions = new Dictionary<string, Condition>();

            // Title attribute should contain the string "Adventures"
            Condition titleCondition = new Condition();
            // titleCondition.ComparisonOperator = ComparisonOperator.CONTAINS;
            // titleCondition.AttributeValueList.Add(new AttributeValue { S = "Adventures" });
            // conditions["Title"] = titleCondition;

            // Pages attributes must be greater-than the numeric value "200"
            Condition pagesCondition = new Condition();
            // pagesCondition.ComparisonOperator = ComparisonOperator.GT; ;
            // pagesCondition.AttributeValueList.Add(new AttributeValue { N = "200" });
            // conditions["Pages"] = pagesCondition;


            // Define marker variable
            Dictionary<string, AttributeValue> startKey = null;

            do
            {
                // Create Scan request
                ScanRequest request = new ScanRequest
                {
                    TableName = tableName,
                    ExclusiveStartKey = startKey,
                    ScanFilter = conditions
                };

                // Issue request
                ScanResult result = client.Scan(request);

                // View all returned items
                List<Dictionary<string, AttributeValue>> items = result.Items;
                foreach (Dictionary<string, AttributeValue> item in items)
                {
                    Console.WriteLine("Item:");
                    foreach (var keyValuePair in item)
                    {
                        // Console.WriteLine(keyValuePair)s;
                        // Console.WriteLine("{0} : S={1}, N={2}, SS=[{3}], NS=[{4}]",
                        //     keyValuePair.Key,
                        //     keyValuePair.Value.S,
                        //     keyValuePair.Value.N,
                        //     string.Join(", ", keyValuePair.Value.SS ?? new List<string>()),
                        //     string.Join(", ", keyValuePair.Value.NS ?? new List<string>()));
                        Console.WriteLine(keyValuePair.Value);
                    }
                }

                // Set marker variable
                startKey = result.LastEvaluatedKey;
            } while (startKey != null && startKey.Count > 0);
        }
        private static void Export(String filename)
        {
            DotNetEnv.Env.Load();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(DotNetEnv.Env.GetString("KEY"), DotNetEnv.Env.GetString("SECRETE_KEY"), RegionEndpoint.APNortheast2);
            Table ThreadTable = Table.LoadTable(client, tableName);

            ScanFilter scanFilter = new ScanFilter();

            ScanOperationConfig config = new ScanOperationConfig()
            {
                AttributesToGet = new List<string> { "dumy", "id", "title", "score", "desc", "watched", "info", "s_title", "s_score", "s_desc" },
                Filter = scanFilter,
                Select = SelectValues.SpecificAttributes
            };
            List<Document> documentList = new List<Document>();

            // List<String> movies = new List<String>();
            Search search = ThreadTable.Scan(config);
            List<Movie> data = new List<Movie>();

            FileStream fs = new FileStream(filename + ".json", FileMode.Create, FileAccess.Write);
            StreamWriter sw = new StreamWriter(fs, System.Text.Encoding.UTF8);

            int tot_count = 0;
            JArray ret_arr = new JArray();
            do
            {
                documentList = search.GetNextSet();
                tot_count += documentList.Count;
                Console.WriteLine(documentList.Count);
                foreach (var document in documentList)
                {
                    // var str = document.ToJson();
                    ret_arr.Add(JsonConvert.DeserializeObject<JObject>(document.ToJson()));

                }
            } while (!search.IsDone);
            sw.Write(ret_arr);

            sw.Close();
            fs.Close();
            Console.WriteLine(tot_count + "개 데이터 저장 완료.");
        }

        private static void Import(String filename)
        {
            DotNetEnv.Env.Load();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(DotNetEnv.Env.GetString("KEY"), DotNetEnv.Env.GetString("SECRETE_KEY"), RegionEndpoint.APNortheast2);
            var r = File.ReadAllText(filename + @".json");
            JArray movies = JArray.Parse(r);

            Table movie = Table.LoadTable(client, tableName);
            var batchWrite = movie.CreateBatchWrite();
            Console.WriteLine(movies.Count());
            int i = 0;
            // Console.WriteLine(movies);
            // Console.WriteLine("NEW_MOVIE >>> ");
            // Console.WriteLine(movies[i]["info"]);
            foreach (JObject parsedObject in movies.Children<JObject>())
            {
                var t_movie = new Document();
                var obj = new Document();
                foreach (JProperty parsedProperty in parsedObject.Properties())
                {

                    // Console.WriteLine("key >>>" + parsedProperty.Name);
                    // Console.WriteLine("val >>>" + parsedProperty.Value);
                    string key = parsedProperty.Name;
                    // Console.WriteLine(">>>" + key);
                    if (parsedProperty.Value.GetType() != typeof(JObject))
                    {
                        string value = (string)parsedProperty.Value;
                        if (key == "dumy" || key == "score" || key == "s_score")
                        {
                            t_movie[key] = Int32.Parse(value);
                        }
                        else if (value == "false" || value == "true" || value == "False" || value == "True")
                        {
                            t_movie[key] = bool.Parse(value);
                        }
                        else
                        {
                            t_movie[key] = value;
                        }

                    }
                    else
                    {
                        foreach (JProperty mapProperty in parsedProperty.Value)
                        {
                            string map_key = mapProperty.Name;
                            string map_val = (string)mapProperty.Value;
                            obj.Add(map_key, map_val);
                        }
                        t_movie[key] = obj;
                    }
                }
                batchWrite.AddDocumentToPut(t_movie);
                if (i % 25 == 0)
                {
                    batchWrite.Execute();

                    batchWrite = movie.CreateBatchWrite();
                    Console.WriteLine("SEND_QUERY>>>" + i / 25);

                }
                i++;
            }
            batchWrite.Execute();
            Console.WriteLine("DONE");
        }

        private static void Import_low(String filename)
        {

            List<WriteRequest> query = new List<WriteRequest>();
            using (StreamReader r = new StreamReader(filename + ".json"))
            {

                string json = r.ReadToEnd();
                // Console.WriteLine(json);
                List<Movie> movies = JsonConvert.DeserializeObject<List<Movie>>(json);
                Console.WriteLine(movies.Count());
                int count = 0;
                foreach (var data in movies)
                {
                    Dictionary<string, AttributeValue> movie = new Dictionary<string, AttributeValue>();
                    count++;
                    var info = new Dictionary<string, AttributeValue>();
                    info["lang"] = new AttributeValue { S = data.info["lang"].ToString() };
                    if (data.info.ContainsKey("dubbing"))
                    {
                        info["dubbing"] = new AttributeValue { S = data.info["dubbing"].ToString() };
                    }
                    else if (data.info.ContainsKey("subtitle"))
                    {
                        info["subtitle"] = new AttributeValue { S = data.info["subtitle"].ToString() };
                    }
                    bool watched;
                    if (data.watched.ToString() == "True" || data.watched.ToString() == "1" || data.watched.ToString() == "true") watched = true;
                    else watched = false;

                    movie["dumy"] = new AttributeValue { N = data.dumy.ToString() };
                    movie["id"] = new AttributeValue { S = data.id.ToString() };
                    movie["title"] = new AttributeValue { S = data.title.ToString() };
                    movie["desc"] = new AttributeValue { S = data.desc.ToString() };
                    movie["score"] = new AttributeValue { N = data.score.ToString() };
                    movie["watched"] = new AttributeValue { BOOL = watched };
                    movie["info"] = new AttributeValue { M = info };
                    movie["s_title"] = new AttributeValue { S = data.s_title.ToString() };
                    movie["s_desc"] = new AttributeValue { S = data.s_desc.ToString() };
                    movie["s_score"] = new AttributeValue { N = data.s_score.ToString() };

                    // Console.WriteLine(data.info.ContainsKey("lang"));
                    query.Add(new WriteRequest
                    {
                        PutRequest = new PutRequest { Item = movie }
                    });
                    Dictionary<string, List<WriteRequest>> requestItems = new Dictionary<string, List<WriteRequest>>();

                    if (count % 25 == 0 || movies.Count() - count < 25)
                    {

                        requestItems[tableName] = query;

                        BatchWriteItemRequest request = new BatchWriteItemRequest { RequestItems = requestItems };
                        BatchWriteItemResult result;
                        do
                        {
                            result = client.BatchWriteItem(request);
                            request.RequestItems = result.UnprocessedItems;
                            // if (result.UnprocessedItems.Count > 0)
                            // {
                            //     Console.WriteLine("UNPOCESSED>>>");
                            // }

                        } while (result.UnprocessedItems.Count > 0);
                        Console.WriteLine("DONE >>> " + count / 25);
                        query.Clear();
                    }
                }
            }
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