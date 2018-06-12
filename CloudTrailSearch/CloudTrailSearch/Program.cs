using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CloudTrailSearch
{
    class Program
    {
        const string S3_BUCKET = "normj-east-1-trail";
        const string REPORT_TABLE = "CloudTrailRuntimeReport";

        static Amazon.RegionEndpoint REGION = Amazon.RegionEndpoint.USEast1;

        public static async Task Main(string[] args)
        {
            using (var s3Client = new AmazonS3Client(REGION))
            {
                var listRequest = new ListObjectsRequest
                {
                    BucketName = S3_BUCKET,
                    Prefix = DetermineKeyPrefix()
                };

                var runtimes = new Dictionary<string, int>();
                string nextMarker = null;
                do
                {
                    listRequest.Marker = nextMarker;
                    var listResponse = await s3Client.ListObjectsAsync(listRequest);

                    foreach (var s3o in listResponse.S3Objects)
                    {
                        await ProcessObjectAsync(s3Client, s3o.Key, runtimes);
                    }
                    nextMarker = listResponse.NextMarker;
                } while (!string.IsNullOrEmpty(nextMarker));

                await PersistRuntimeCountAsync(runtimes);
            }
        }

        private static async Task ProcessObjectAsync(IAmazonS3 s3Client, string key, IDictionary<string, int> runtimes)
        {
            try
            {
                Console.WriteLine($"Processing {key}");

                using (var compressedStream = (await s3Client.GetObjectAsync(S3_BUCKET, key)).ResponseStream)
                using (var stream = new GZipStream(compressedStream, CompressionMode.Decompress))
                using (var reader = new StreamReader(stream))
                {
                    var json = await reader.ReadToEndAsync();
                    var root = JsonConvert.DeserializeObject(json) as JObject;

                    foreach(JObject record in root["Records"])
                    {
                        if(record["eventSource"] == null || !string.Equals("lambda.amazonaws.com", record["eventSource"].ToString()) ||
                            record["eventName"] == null || !record["eventName"].ToString().StartsWith("CreateFunction"))
                        {
                            continue;
                        }

                        var runtime = record["requestParameters"]["runtime"]?.ToString();
                        Console.WriteLine($"Found CreateFunction call with {runtime}");

                        if (runtimes.ContainsKey(runtime))
                            runtimes[runtime]++;
                        else
                            runtimes[runtime] = 1;
                    }
                }
            }
            catch(Exception e)
            {
                Console.Error.WriteLine($"Error processing object {key}: {e.Message}");
            }
        }

        static string DetermineKeyPrefix()
        {

            string prefix;

            if(!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AWS_BATCH_JOB_ARRAY_INDEX")))
            {
                var batchArrayindex = int.Parse(Environment.GetEnvironmentVariable("AWS_BATCH_JOB_ARRAY_INDEX"));
                DateTime searchDate = DateTime.Today.AddDays(-batchArrayindex);
                prefix = $"logs/AWSLogs/626492997873/CloudTrail/us-east-1/{searchDate.Year}/{searchDate.Month.ToString("D2")}/{searchDate.Day.ToString("D2")}";
            }
            else
            {
                prefix = $"logs/AWSLogs/626492997873/CloudTrail/us-east-1/2018/05/29";
            }

            Console.WriteLine($"S3 Key Prefix: {prefix}");

            return prefix;

        }

        static async Task PersistRuntimeCountAsync(IDictionary<string, int> runtimes)
        {
            if(runtimes.Count == 0)
            {
                Console.WriteLine("No CreateFunction calls found");
                return;
            }

            using (var ddbClient = new AmazonDynamoDBClient(REGION))
            {
                foreach(var runtime in runtimes.Keys)
                {
                    var request = new UpdateItemRequest
                    {
                        TableName = REPORT_TABLE,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            { "Runtime", new AttributeValue{S = runtime} }
                        },
                        UpdateExpression = "ADD #a :increment",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            {"#a", "Creates"}
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            {":increment", new AttributeValue{N = runtimes[runtime].ToString()}},
                        }
                    };

                    await ddbClient.UpdateItemAsync(request);
                }
            }
        }
    }
}
