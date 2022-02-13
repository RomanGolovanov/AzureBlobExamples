using System;
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

            var uploadTime = await blobClient.UploadUsingMultipleBlocksAsync(content, blockCount).WrapDuration();

            var (downloadedContent, downloadTime) = await blobClient.DownloadContentAsync().WrapDuration();
            
            var (_, deleteTime) = await blobClient.DeleteAsync().WrapDuration();

            var contentValid = Tools.IsEquals(content, downloadedContent.Value.Content.ToArray());

            Console.WriteLine(
                $"Block count: {blockCount}, " +
                $"block size: {content.Length / blockCount}, " +
                $"upload time: {uploadTime}, " +
                $"download time: {downloadTime}, " +
                $"delete time: {deleteTime}, " +
                $"content valid: {contentValid}");
        }
    }
}
