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

namespace migration
{
    class migration
    {
        private static readonly String key = "";
        private static readonly String s_key = "";
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient(key, s_key, RegionEndpoint.APNortheast2);
        private static string tableName = "test02-movie5";

        static void Main(string[] args)
        {
            try
            {
                Boolean flag = true;
                while (flag)
                {
                    Console.WriteLine("1 : Create TEST Table");
                    Console.WriteLine("2 : Delete Table");
                    Console.WriteLine("3 : Export Data");
                    Console.WriteLine("4 : Import Data");
                    Console.WriteLine("0 : EXIT_PORGRAM");
                    Console.WriteLine("명령어를 입력하세요");
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
                                Console.WriteLine("Insert Tablename to DELETE");
                                tableName = Console.ReadLine();
                                DeleteExampleTable();
                                break;
                            }
                        case "3":
                            {
                                Console.WriteLine("Insert Tablename");
                                tableName = Console.ReadLine();
                                Console.WriteLine("Insert Filename to save data");
                                String filename = Console.ReadLine();
                                Stopwatch stopwatch = new Stopwatch();
                                stopwatch.Start();
                                Export(filename);
                                // Export_low(filename);
                                stopwatch.Stop();
                                Console.WriteLine("실행시간 : " + (stopwatch.ElapsedMilliseconds / 1000).ToString() + "s");
                                break;
                            }
                        case "4":
                            {
                                Console.WriteLine("Insert Tablename");
                                tableName = Console.ReadLine();
                                Console.WriteLine("Insert Filename to save data");
                                String filename = Console.ReadLine();
                                Stopwatch stopwatch = new Stopwatch();
                                stopwatch.Start();
                                // Import_low(filename);
                                Import(filename);
                                stopwatch.Stop();
                                Console.WriteLine("실행시간 : " + (stopwatch.ElapsedMilliseconds / 1000).ToString() + "s");
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

        private static void DeleteExampleTable()
        {
            Console.WriteLine("\n*** Deleting table ***");
            var request = new DeleteTableRequest
            {
                TableName = tableName
            };

            var response = client.DeleteTable(request);

            Console.WriteLine("Table is being deleted...");
        }

        private static void WaitUntilTableReady(string tableName)
        {
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
            // List<Json> data = new List<Json>();

            // 파일이 존재하면 삭제하고 다시 만드는 로직 추가.
            FileStream fs = new FileStream(filename + ".json", FileMode.Append, FileAccess.Write);
            StreamWriter sw = new StreamWriter(fs, System.Text.Encoding.UTF8);
            sw.Write("[");
            do
            {
                documentList = search.GetNextSet();
                Console.WriteLine(documentList.Count);
                foreach (var document in documentList)
                {
                    // PrintDocument(document);
                    // Console.WriteLine(document.ToJson());
                    // data.Append(document.ToJson());
                    var str = document.ToJson();
                    sw.Write(str.ToString());
                    sw.Write(",");
                    sw.Flush();
                }
            } while (!search.IsDone);
            sw.Write("]");
            sw.Close();
            fs.Close();
        }
        private static void Import_low(String filename)
        {
            var r = File.ReadAllText(filename + @".json");
            JArray movies = JArray.Parse(r);

            Console.WriteLine(movies.Count());
            List<WriteRequest> query = new List<WriteRequest>();
            for (int i = 0; i < movies.Count(); i++)
            {
                Dictionary<string, AttributeValue> movie = new Dictionary<string, AttributeValue>();
                var dumy = movies[i]["dumy"].ToString();
                var id = movies[i]["id"].ToString();
                var title = movies[i]["title"].ToString();
                var desc = movies[i]["desc"].ToString();
                var score = movies[i]["score"].ToString();
                int watched;
                if (movies[i]["watched"].ToString() == "True" || movies[i]["watched"].ToString() == "1")
                {
                    watched = 1;
                }
                else
                {
                    watched = 0;
                }
                var info = movies[i]["info"].ToString();
                var s_title = movies[i]["s_title"].ToString();
                var s_desc = movies[i]["s_desc"].ToString();
                var s_score = movies[i]["s_score"].ToString();

                movie["dumy"] = new AttributeValue { N = dumy };
                movie["id"] = new AttributeValue { S = id };
                movie["title"] = new AttributeValue { S = title };
                movie["desc"] = new AttributeValue { S = desc };
                movie["score"] = new AttributeValue { N = score };
                movie["watched"] = new AttributeValue { N = watched.ToString() };
                movie["info"] = new AttributeValue { S = info };
                movie["s_title"] = new AttributeValue { S = s_title };
                movie["s_desc"] = new AttributeValue { S = s_desc };
                movie["s_score"] = new AttributeValue { N = s_score };

                query.Add(new WriteRequest
                {
                    PutRequest = new PutRequest { Item = movie }
                });
                // Console.WriteLine(query);


                if (i % 25 == 0)
                {

                    Dictionary<string, List<WriteRequest>> requestItems = new Dictionary<string, List<WriteRequest>>();
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
                    Console.WriteLine("DONE >>> " + i / 25);
                    query.Clear();
                }
            }




        }
        private static void Import(String filename)
        {
            var r = File.ReadAllText(filename + @".json");
            JArray movies = JArray.Parse(r);

            Table movie = Table.LoadTable(client, tableName);
            var batchWrite = movie.CreateBatchWrite();

            Console.WriteLine(movies.Count());
            for (int i = 0; i < movies.Count(); i++)
            {
                var t_movie = new Document();
                // Dictionary<string, object> t_movie = new Dictionary<string, object>();
                // Movie t_movie = new Movie();

                var dumy = movies[i]["dumy"].ToObject<int>();
                var id = movies[i]["id"].ToObject<string>();
                var title = movies[i]["title"].ToObject<string>();
                var desc = movies[i]["desc"].ToObject<string>();
                var score = movies[i]["score"].ToObject<int>();
                bool watched;
                if (movies[i]["watched"].ToString() == "True" || movies[i]["watched"].ToObject<int>() == 1)
                {
                    watched = true;
                }
                else
                {
                    watched = false;
                }

                var info = movies[i]["info"].ToObject<object>();
                var s_title = movies[i]["s_title"].ToObject<string>();
                var s_desc = movies[i]["s_desc"].ToObject<string>();
                var s_score = movies[i]["s_score"].ToObject<int>();
                // Console.WriteLine(movies[i]["watched"].ToString());
                t_movie["dumy"] = dumy;
                t_movie["id"] = id;
                t_movie["title"] = title;
                t_movie["desc"] = desc;
                t_movie["score"] = score;
                t_movie["watched"] = watched;
                t_movie["info"] = info.ToString();
                t_movie["s_title"] = s_title;
                t_movie["s_desc"] = s_desc;
                t_movie["s_score"] = s_score;

                // Console.WriteLine(t_movie);
                batchWrite.AddDocumentToPut(t_movie);
                if (i % 25 == 0)
                {
                    batchWrite.Execute();

                    batchWrite = movie.CreateBatchWrite();
                    Console.WriteLine("SEND_QUERY>>>" + i / 25);

                }
            }
        }
    }
}