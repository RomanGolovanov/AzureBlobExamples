using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace AzureBlobExamples
{
    public static class Tools
    {
        public static async Task<TimeSpan> WrapDuration(this Task task)
        {
            var stopwatch = Stopwatch.StartNew();

            await task;

            return stopwatch.Elapsed;
        } 

        public static async Task<(T Result, TimeSpan Duration)> WrapDuration<T>(this Task<T> task)
        {
            var stopwatch = Stopwatch.StartNew();
            return (await task, stopwatch.Elapsed);
        } 

        public static bool IsEquals(byte[] left, byte[] right)
        {
            if (left.Length != right.Length)
            {
                return false;
            }

            var i = 0;
            while (i < left.Length && left[i]==right[i]) 
            {
                i++;
            }

            return i == left.Length;
        }
    }
}