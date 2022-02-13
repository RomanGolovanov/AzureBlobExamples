using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Specialized;

namespace AzureBlobExamples
{
public static class BlockBlobClientExtensions
{
    public static async Task UploadUsingMultipleBlocksAsync(this BlockBlobClient client, byte[] content, int blockCount)
    {
        if(client == null) throw new ArgumentNullException(nameof(client));
        if(content == null) throw new ArgumentNullException(nameof(content));
        if(blockCount < 0 || blockCount > content.Length) throw new ArgumentOutOfRangeException(nameof(blockCount));

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