// Copyright (c) Six Labors.
// Licensed under the Apache License, Version 2.0.

using System.Buffers;
using System.Runtime.CompilerServices;

namespace SixLabors.ImageSharp.Memory
{
    /// <summary>
    /// Implements <see cref="MemoryAllocator"/> by allocating memory from <see cref="ArrayPool{T}"/>.
    /// </summary>
    public sealed partial class ArrayPoolMemoryAllocator : MemoryAllocator
    {
        private readonly int maxArraysPerBucketNormalPool;

        private readonly int maxArraysPerBucketLargePool;

        private int bufferCapacityInBytes;

        /// <summary>
        /// The <see cref="ArrayPool{T}"/> for small-to-medium buffers which is not kept clean.
        /// </summary>
        private ArrayPool<byte> normalArrayPool;

        /// <summary>
        /// The <see cref="ArrayPool{T}"/> for huge buffers, which is not kept clean.
        /// </summary>
        private ArrayPool<byte> largeArrayPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayPoolMemoryAllocator"/> class.
        /// </summary>
        public ArrayPoolMemoryAllocator()
            : this(DefaultMaxPooledBufferSizeInBytes, DefaultBufferSelectorThresholdInBytes)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayPoolMemoryAllocator"/> class.
        /// </summary>
        /// <param name="maxPoolSizeInBytes">The maximum size of pooled arrays. Arrays over the thershold are gonna be always allocated.</param>
        public ArrayPoolMemoryAllocator(int maxPoolSizeInBytes)
            : this(maxPoolSizeInBytes, GetLargeBufferThresholdInBytes(maxPoolSizeInBytes))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayPoolMemoryAllocator"/> class.
        /// </summary>
        /// <param name="maxPoolSizeInBytes">The maximum size of pooled arrays. Arrays over the thershold are gonna be always allocated.</param>
        /// <param name="poolSelectorThresholdInBytes">Arrays over this threshold will be pooled in <see cref="largeArrayPool"/> which has less buckets for memory safety.</param>
        public ArrayPoolMemoryAllocator(int maxPoolSizeInBytes, int poolSelectorThresholdInBytes)
            : this(maxPoolSizeInBytes, poolSelectorThresholdInBytes, DefaultLargePoolBucketCount, DefaultNormalPoolBucketCount)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayPoolMemoryAllocator"/> class.
        /// </summary>
        /// <param name="maxPoolSizeInBytes">The maximum size of pooled arrays. Arrays over the thershold are gonna be always allocated.</param>
        /// <param name="poolSelectorThresholdInBytes">The threshold to pool arrays in <see cref="largeArrayPool"/> which has less buckets for memory safety.</param>
        /// <param name="maxArraysPerBucketLargePool">Max arrays per bucket for the large array pool.</param>
        /// <param name="maxArraysPerBucketNormalPool">Max arrays per bucket for the normal array pool.</param>
        public ArrayPoolMemoryAllocator(
            int maxPoolSizeInBytes,
            int poolSelectorThresholdInBytes,
            int maxArraysPerBucketLargePool,
            int maxArraysPerBucketNormalPool)
            : this(
                maxPoolSizeInBytes,
                poolSelectorThresholdInBytes,
                maxArraysPerBucketLargePool,
                maxArraysPerBucketNormalPool,
                DefaultBufferCapacityInBytes)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayPoolMemoryAllocator"/> class.
        /// </summary>
        /// <param name="maxPoolSizeInBytes">The maximum size of pooled arrays. Arrays over the thershold are gonna be always allocated.</param>
        /// <param name="poolSelectorThresholdInBytes">The threshold to pool arrays in <see cref="largeArrayPool"/> which has less buckets for memory safety.</param>
        /// <param name="maxArraysPerBucketLargePool">Max arrays per bucket for the large array pool.</param>
        /// <param name="maxArraysPerBucketNormalPool">Max arrays per bucket for the normal array pool.</param>
        /// <param name="bufferCapacityInBytes">The length of the largest contiguous buffer that can be handled by this allocator instance.</param>
        public ArrayPoolMemoryAllocator(
            int maxPoolSizeInBytes,
            int poolSelectorThresholdInBytes,
            int maxArraysPerBucketLargePool,
            int maxArraysPerBucketNormalPool,
            int bufferCapacityInBytes)
        {
            Guard.MustBeGreaterThan(maxPoolSizeInBytes, 0, nameof(maxPoolSizeInBytes));
            Guard.MustBeLessThanOrEqualTo(poolSelectorThresholdInBytes, maxPoolSizeInBytes, nameof(poolSelectorThresholdInBytes));

            this.MaxPoolSizeInBytes = maxPoolSizeInBytes;
            this.PoolSelectorThresholdInBytes = poolSelectorThresholdInBytes;
            this.BufferCapacityInBytes = bufferCapacityInBytes;
            this.maxArraysPerBucketLargePool = maxArraysPerBucketLargePool;
            this.maxArraysPerBucketNormalPool = maxArraysPerBucketNormalPool;

            this.InitArrayPools();
        }

        /// <summary>
        /// Gets the maximum size of pooled arrays in bytes.
        /// </summary>
        public int MaxPoolSizeInBytes { get; }

        /// <summary>
        /// Gets the threshold to pool arrays in <see cref="largeArrayPool"/> which has less
        /// buckets for memory safety.
        /// </summary>
        public int PoolSelectorThresholdInBytes { get; }

        /// <summary>
        /// Gets the length of the largest contiguous buffer that can be handled by this
        /// allocator instance.
        /// <remarks>
        /// Given values are automatically rounded up to the next bucket size to prevent
        /// unnecessary allocations when querying exhausted pools.
        /// </remarks>
        /// </summary>
        public int BufferCapacityInBytes
        {
            get { return this.bufferCapacityInBytes; }

            // Setter is internal for easy configuration in tests
            internal set { this.bufferCapacityInBytes = RoundUp(value); }
        }

        /// <inheritdoc />
        public override void ReleaseRetainedResources()
        {
            this.InitArrayPools();
        }

        /// <inheritdoc />
        protected internal override int GetBufferCapacityInBytes() => this.BufferCapacityInBytes;

        /// <inheritdoc />
        public override IMemoryOwner<T> Allocate<T>(int length, AllocationOptions options = AllocationOptions.None)
        {
            Guard.MustBeGreaterThanOrEqualTo(length, 0, nameof(length));
            int itemSizeBytes = Unsafe.SizeOf<T>();
            int bufferSizeInBytes = RoundUp(length * itemSizeBytes);
            if (bufferSizeInBytes < 0 || bufferSizeInBytes > this.BufferCapacityInBytes)
            {
                ThrowInvalidAllocationException<T>(length, this.BufferCapacityInBytes);
            }

            ArrayPool<byte> pool = this.GetArrayPool(bufferSizeInBytes);
            byte[] byteArray = pool.Rent(bufferSizeInBytes);

            var buffer = new Buffer<T>(byteArray, length, pool);
            if (options == AllocationOptions.Clean)
            {
                buffer.GetSpan().Clear();
            }

            return buffer;
        }

        /// <inheritdoc />
        public override IManagedByteBuffer AllocateManagedByteBuffer(int length, AllocationOptions options = AllocationOptions.None)
        {
            Guard.MustBeGreaterThanOrEqualTo(length, 0, nameof(length));

            int bufferSize = RoundUp(length);
            ArrayPool<byte> pool = this.GetArrayPool(bufferSize);
            byte[] byteArray = pool.Rent(bufferSize);

            var buffer = new ManagedByteBuffer(byteArray, length, pool);
            if (options == AllocationOptions.Clean)
            {
                buffer.GetSpan().Clear();
            }

            return buffer;
        }

        private static int GetLargeBufferThresholdInBytes(int maxPoolSizeInBytes)
        {
            return maxPoolSizeInBytes / 4;
        }

        [MethodImpl(InliningOptions.ColdPath)]
        private static void ThrowInvalidAllocationException<T>(int length, int capacity) =>
            throw new InvalidMemoryOperationException(
                $"Requested allocation: {length} elements of {typeof(T).Name} is over the capacity ({capacity}) bytes of the MemoryAllocator.");

        private ArrayPool<byte> GetArrayPool(int bufferSizeInBytes)
        {
            return bufferSizeInBytes <= this.PoolSelectorThresholdInBytes ? this.normalArrayPool : this.largeArrayPool;
        }

        private void InitArrayPools()
        {
            this.largeArrayPool = ArrayPool<byte>.Create(this.MaxPoolSizeInBytes, this.maxArraysPerBucketLargePool);
            this.normalArrayPool = ArrayPool<byte>.Create(this.PoolSelectorThresholdInBytes, this.maxArraysPerBucketNormalPool);
        }

        // https://twitter.com/marcgravell/status/1233025769074458624
        // https://github.com/mgravell/Pipelines.Sockets.Unofficial/commit/6740ea4f79a9ae75fda9de23d06ae4a614a516cf#diff-910f8cc974aaa5b985b4d5921e6854a5R191
        private static int RoundUp(int capacity)
        {
            if (capacity <= 1)
            {
                return capacity;
            }

            // We need to do this because array-pools stop buffering beyond
            // a certain point, and just give us what we ask for; if we don't
            // apply upwards rounding *ourselves*, then beyond that limit, we
            // end up *constantly* allocating/copying arrays, on each copy
            //
            // Note we subtract one because it is easier to round up to the *next* bucket size, and
            // subtracting one guarantees that this will work
            //
            // If we ask for, say, 913; take 1 for 912; that's 0000 0000 0000 0000 0000 0011 1001 0000
            // so lz is 22; 32-22=10, 1 << 10= 1024
            //
            // or for 2: lz of 2-1 is 31, 32-31=1; 1<<1=2
            int limit = 1 << (32 - LeadingZeros(capacity - 1));
            return limit < 0 ? int.MaxValue : limit;

            static int LeadingZeros(int x) // https://stackoverflow.com/questions/10439242/count-leading-zeroes-in-an-int32
            {
#if SUPPORTS_RUNTIME_INTRINSICS
                if (System.Runtime.Intrinsics.X86.Lzcnt.IsSupported)
                {
                    return (int)System.Runtime.Intrinsics.X86.Lzcnt.LeadingZeroCount((uint)x);
                }
                else
#endif
                {
                    const int numIntBits = sizeof(int) * 8; // Compile time constant

                    // Do the smearing
                    x |= x >> 1;
                    x |= x >> 2;
                    x |= x >> 4;
                    x |= x >> 8;
                    x |= x >> 16;

                    // Count the ones
                    x -= x >> 1 & 0x55555555;
                    x = (x >> 2 & 0x33333333) + (x & 0x33333333);
                    x = (x >> 4) + x & 0x0f0f0f0f;
                    x += x >> 8;
                    x += x >> 16;

                    return numIntBits - (x & 0x0000003f); // Subtract # of 1s from 32
                }
            }
        }
    }
}
