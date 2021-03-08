using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon;
using Amazon.DynamoDBv2.DataModel;
using DotNetEnv;

namespace Json2Dynamo
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

    class json2dynamo
    {
        public static void Import(String filename, String tableName)
        {
            DotNetEnv.Env.Load();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(DotNetEnv.Env.GetString("KEY"), DotNetEnv.Env.GetString("SECRETE_KEY"), RegionEndpoint.APNortheast2);
            var r = File.ReadAllText("./export_data/" + filename + @".json");
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
        public static void Import_low(String filename, String tableName)
        {
            DotNetEnv.Env.Load();
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(DotNetEnv.Env.GetString("KEY"), DotNetEnv.Env.GetString("SECRETE_KEY"), RegionEndpoint.APNortheast2);

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
    }
}