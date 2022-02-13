using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Configuration;

namespace AzureBlobExamples
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddUserSecrets<Program>()
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();

            var connectionString = configuration.GetConnectionString("StorageAccount");
            
            var container = new BlobContainerClient(connectionString, "sample-container");

            await container.CreateIfNotExistsAsync();

            await TestSlowReadBlobWithFragmentation(container);

            Console.WriteLine("Done, press ENTER to exit.");
            Console.ReadLine();
        }

        private static async Task TestSlowReadBlobWithFragmentation(BlobContainerClient container)
        {
            var buffer = new byte[100_000_000];
            new Random().NextBytes(buffer);

            await RunUploadBlockBlobByMultipleFragmentsTest(container, buffer, 1);
            await RunUploadBlockBlobByMultipleFragmentsTest(container, buffer, 100);
            await RunUploadBlockBlobByMultipleFragmentsTest(container, buffer, 1000);
            await RunUploadBlockBlobByMultipleFragmentsTest(container, buffer, 10000);
            await RunUploadBlockBlobByMultipleFragmentsTest(container, buffer, 50000);
        }


        private static async Task RunUploadBlockBlobByMultipleFragmentsTest(BlobContainerClient container, byte[] content, int blockCount)
        {
            var blobClient = container.GetBlockBlobClient($"app-{Guid.NewGuid():N}");

            var uploadTime = await UploadBlockBlobAsync(blobClient, content, blockCount).WrapDuration();

            var (downloadedContent, downloadTime) = await blobClient.DownloadContentAsync().WrapDuration();
            
            var (_, deleteTime) = await blobClient.DeleteAsync().WrapDuration();

            var contentValid = Tools.IsEquals(content, downloadedContent.Value.Content.ToArray());

            Console.WriteLine(
                $"Block count: {blockCount}, " +
                $"block size: {content.Length / blockCount}, " +
                $"upload time: {uploadTime}, " +
                $"download time: {downloadTime}, " +
                $"delete time: {deleteTime}, content valid: {contentValid}");
        }

        private static async Task UploadBlockBlobAsync(BlockBlobClient client, byte[] content, int blockCount)
        {
            var position = 0;
            var blockSize = content.Length / blockCount;
            var blockIds = new List<string>();

            var tasks = new List<Task>();

            while (position < content.Length)
            {
                var blockId = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
                blockIds.Add(blockId);
                tasks.Add(UploadBlockAsync(client, blockId, content, position, blockSize));
                position += blockSize;
            }

            await Task.WhenAll(tasks);
            await client.CommitBlockListAsync(blockIds);
        }

        private static async Task UploadBlockAsync(BlockBlobClient client, string blockId, byte[] content, int position, int blockSize)
        {
            await using var blockContent = new MemoryStream(content, position, Math.Min(blockSize, content.Length - position));
            await client.StageBlockAsync(blockId, blockContent);
        }
    }
}
