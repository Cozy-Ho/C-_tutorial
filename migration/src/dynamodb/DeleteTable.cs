using System;
using System.Linq;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon;
using Amazon.DynamoDBv2.DataModel;

using System.Configuration;

namespace DeleteTable
{
    class deletetable
    {
        public static void DeleteTable(String tableName, AmazonDynamoDBClient client)
        {
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
    }
}