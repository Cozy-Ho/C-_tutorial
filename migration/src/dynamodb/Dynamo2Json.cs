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

using System.Configuration;

namespace Dynamo2Json
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
    class dynamo2json
    {
        public static void Export(String filename, String tableName, AmazonDynamoDBClient client)
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
            List<Movie> data = new List<Movie>();

            FileStream fs = new FileStream("./export_data/" + filename + ".json", FileMode.Create, FileAccess.Write);
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
        public static void Export_low(String filename, String tableName, AmazonDynamoDBClient client)
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
    }
}