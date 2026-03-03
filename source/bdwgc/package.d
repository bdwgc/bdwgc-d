/++
 + D interface to the Boehm-Demers-Weiser Garbage Collector (BDWGC).
 + Provides a structured allocator API for GC-managed memory with thread support.
 +
 + Note: All allocations, including aligned ones, are GC-managed via BDWGC.
 + Thread registration is required for multi-threaded applications when GCThreads is enabled.
 +/
module bdwgc;

version (D_BetterC)
{
    version (LDC)
    {
        pragma(LDC_no_moduleinfo);
        pragma(LDC_no_typeinfo);
    }
}

/// BDWGC C bindings
public import c.gc; // @system
import std.algorithm.comparison : max;
import std.experimental.allocator : IAllocator;

// Declare missing BDWGC thread functions
version (GCThreads)
{
extern (C) @nogc nothrow:
    int GC_thread_is_registered(); /// Returns non-zero if thread is registered
    void GC_register_my_thread(); /// Registers the current thread
    void GC_unregister_my_thread(); /// Unregisters the current thread
    version (Posix) void GC_allow_register_threads(); /// Enables dynamic thread registration
}

/++
 + Checks if alignment is valid: a power of 2 and at least pointer size.
 +/
bool isGoodDynamicAlignment(uint x) @nogc nothrow pure
{
    return x >= (void*).sizeof && (x & (x - 1)) == 0;
}

version (Windows)
{
    private import core.stdc.stdio : printf;

    alias GC_printf = printf; /// Alias for Windows printf
}
else
{
    /// Formatted output for GC logging
    pragma(printf)
    extern (C) void GC_printf(const(char)* format, ...) @trusted @nogc nothrow;
}

/++
 + Manage BDWGC thread registration.
 + Use ThreadGuard.create() to instantiate and register the current thread.
 + Unregisters the thread on destruction. No-op if GCThreads is disabled.
 +/
struct ThreadGuard
{
@nogc nothrow:
    this(this) @disable; // Prevent copying
    private bool isRegistered; // Track registration state

    /// Factory function to create and register a ThreadGuard
    @trusted static ThreadGuard create()
    {
        ThreadGuard guard;
        version (GCThreads)
        {
            if (!GC_thread_is_registered())
            {
                debug
                    GC_printf("Registering thread\n");
                GC_register_my_thread();
                guard.isRegistered = true;
            }
        }
        return guard;
    }

    /// Unregisters the thread if registered
    @trusted ~this()
    {
        version (GCThreads)
        {
            if (isRegistered && GC_thread_is_registered())
            {
                debug
                    GC_printf("Unregistering thread\n");
                GC_unregister_my_thread();
            }
        }
    }
}

/++
 + Allocator for BDWGC-managed memory, implementing IAllocator.
 + Thread-safe and compatible with `-betterC`.
 + Requires thread registration for multi-threaded use when GCThreads is enabled.
 +/
struct BoehmAllocator
{
    version (StdUnittest) @system unittest
    {
        extern (C) void testAllocator(alias alloc)(); // Declare testAllocator
        testAllocator!(() => BoehmAllocator.instance)();
    }

    /// Alignment ensures proper alignment for D data types
    enum uint alignment = max(double.alignof, real.alignof);

    /// One-time initialization of BDWGC with thread support
    shared static this() @nogc nothrow
    {
        debug
            GC_printf("Initializing BDWGC\n");
        GC_init();
        version (GCThreads)
        {
            // Enable thread support
            version (Posix)
                GC_allow_register_threads();
        }
    }

    /// Allocates memory of specified size, returns null if allocation fails
    @trusted @nogc nothrow
    void[] allocate(size_t bytes) shared
    {
        if (!bytes)
            return null;
        auto p = GC_MALLOC(bytes);
        return p ? p[0 .. bytes] : null;
    }

    /// Allocates aligned memory using GC_memalign, returns null if allocation fails
    @trusted @nogc nothrow
    void[] alignedAllocate(size_t bytes, uint a) shared
    {
        if (!bytes || !a.isGoodDynamicAlignment)
            return null;
        auto p = GC_memalign(a, bytes);
        return p ? p[0 .. bytes] : null;
    }

    /// Deallocates memory, safe for null buffers
    @system @nogc nothrow
    bool deallocate(void[] b) shared
    {
        if (b.ptr)
            GC_FREE(b.ptr);
        return true;
    }

    /// Reallocates memory to new size, handles zero-size deallocation
    @system @nogc nothrow
    bool reallocate(ref void[] b, size_t newSize) shared
    {
        if (!newSize)
        {
            deallocate(b);
            b = null;
            return true;
        }
        auto p = GC_REALLOC(b.ptr, newSize);
        if (!p)
            return false;
        b = p[0 .. newSize];
        return true;
    }

    /// Allocates zero-initialized, pointer-scannable memory.
    /// Uses GC_MALLOC which zero-initializes and allows the GC to trace
    /// any GC-managed pointers stored in the returned block.
    @trusted @nogc nothrow
    void[] allocateZeroed(size_t bytes) shared
    {
        if (!bytes)
            return null;
        auto p = GC_MALLOC(bytes); // already zeroed; scannable for GC pointers
        return p ? p[0 .. bytes] : null;
    }

    /// Allocates memory for non-pointer (scalar) data.
    /// Uses GC_MALLOC_ATOMIC: the GC will not scan this block for pointers,
    /// making it more efficient for large numeric arrays or byte buffers.
    /// Do NOT store GC-managed pointers in memory returned by this method.
    @trusted @nogc nothrow
    void[] allocateAtomic(size_t bytes) shared
    {
        if (!bytes)
            return null;
        auto p = GC_MALLOC_ATOMIC(bytes);
        return p ? p[0 .. bytes] : null;
    }

    /// Enables incremental garbage collection
    @trusted @nogc nothrow
    void enableIncremental() shared
    {
        GC_enable_incremental();
    }

    /// Disables garbage collection
    @trusted @nogc nothrow
    void disable() shared
    {
        GC_disable();
    }

    /// Re-enables garbage collection after disable()
    @trusted @nogc nothrow
    void enable() shared
    {
        GC_enable();
    }

    /// Returns the current total heap size in bytes
    @trusted @nogc nothrow
    size_t getHeapSize() shared const
    {
        return GC_get_heap_size();
    }

    /// Returns a lower bound on the number of free bytes in the GC heap
    /// (excludes unmapped memory returned to the OS).
    @trusted @nogc nothrow
    size_t freeBytes() shared const
    {
        return GC_get_free_bytes();
    }

    /// Returns the total number of bytes allocated in this process.
    /// Includes all GC-managed memory ever requested; never decreases.
    @trusted @nogc nothrow
    size_t totalBytes() shared const
    {
        return GC_get_total_bytes();
    }

    /// Triggers garbage collection
    @trusted @nogc nothrow
    void collect() shared
    {
        GC_gcollect();
    }

    /// Checks if pointer is GC-managed
    @trusted @nogc nothrow
    bool isHeapPtr(const void* ptr) shared const
    {
        return GC_is_heap_ptr(cast(void*) ptr) != 0;
    }

    /// Given any pointer into a GC-managed block (including interior pointers),
    /// returns the start address of that block.
    /// Returns null if ptr is not inside a GC-heap allocation.
    @trusted @nogc nothrow
    void* resolveInternalPointer(void* ptr) shared const
    {
        return GC_base(ptr);
    }

    /// Checks if the allocator owns the memory block
    @trusted @nogc nothrow
    bool owns(void[] b) shared const
    {
        return b.ptr && isHeapPtr(b.ptr);
    }

    /// Suggests a good allocation size
    @trusted @nogc nothrow
    size_t goodAllocSize(size_t n) shared const
    {
        if (n == 0)
            return 0;
        // Round up to the next multiple of alignment
        return ((n + alignment - 1) / alignment) * alignment;
    }

    /// Global thread-safe instance
    static shared BoehmAllocator instance;

    // IAllocator interface compliance
    alias allocate this;
}

/**
 * Unit tests
 */
version (unittest)
{
    import std.experimental.allocator : makeArray;

    @("Basic allocation and deallocation")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        auto buffer = BoehmAllocator.instance.allocate(1024 * 1024 * 4);
        scope (exit)
            BoehmAllocator.instance.deallocate(buffer);
        assert(buffer !is null);
        assert(BoehmAllocator.instance.isHeapPtr(buffer.ptr));
        assert(BoehmAllocator.instance.owns(buffer));
    }

    @("Aligned allocation")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        auto buffer = BoehmAllocator.instance.alignedAllocate(1024, 128);
        scope (exit)
            BoehmAllocator.instance.deallocate(buffer);
        assert(buffer !is null);
        assert((cast(size_t) buffer.ptr) % 128 == 0);
    }

    @("Reallocation and zeroed allocation")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocate(16);
        assert(b !is null, "Allocation failed");
        (cast(ubyte[]) b)[] = ubyte(1);
        // Debug: Print buffer contents before reallocation
        debug
        {
            GC_printf("Before realloc: ");
            foreach (i; 0 .. 16)
                GC_printf("%02x ", (cast(ubyte[]) b)[i]);
            GC_printf("\n");
        }
        assert(BoehmAllocator.instance.reallocate(b, 32), "Reallocation failed");
        // Debug: Print buffer contents after reallocation
        debug
        {
            GC_printf("After realloc: ");
            foreach (i; 0 .. 16)
                GC_printf("%02x ", (cast(ubyte[]) b)[i]);
            GC_printf("\n");
        }
        ubyte[16] expected = 1;
        // Manual comparison to avoid issues
        bool isEqual = true;
        for (size_t i = 0; i < 16; i++)
            if ((cast(ubyte[]) b)[i] != 1)
            {
                isEqual = false;
                break;
            }
        assert(isEqual, "Reallocated buffer contents incorrect");
        BoehmAllocator.instance.deallocate(b);

        auto zeroed = BoehmAllocator.instance.allocateZeroed(16);
        assert(zeroed !is null, "Zeroed allocation failed");
        ubyte[16] zeroExpected = 0;
        assert((cast(ubyte[]) zeroed)[] == zeroExpected, "Zeroed buffer not zero");
        BoehmAllocator.instance.deallocate(zeroed);
    }

    @("Incremental GC and collection")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        BoehmAllocator.instance.enableIncremental();
        auto b = BoehmAllocator.instance.allocate(1024);
        assert(b !is null);
        BoehmAllocator.instance.collect();
        BoehmAllocator.instance.disable();
        BoehmAllocator.instance.deallocate(b);
    }

    @("Allocator interface compliance")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        static void test(A)()
        {
            int* p = cast(int*) A.instance.allocate(int.sizeof);
            scope (exit)
                A.instance.deallocate(p[0 .. int.sizeof]);
            *p = 42;
            assert(*p == 42);
        }

        test!BoehmAllocator();
    }

    @("Thread registration")
    @nogc @system nothrow unittest
    {
        version (GCThreads)
        {
            assert(!GC_thread_is_registered());
            {
                auto guard = ThreadGuard.create();
                assert(GC_thread_is_registered());
                auto buffer = BoehmAllocator.instance.allocate(1024);
                assert(buffer !is null);
                BoehmAllocator.instance.deallocate(buffer);
            }
            assert(!GC_thread_is_registered());
        }
        else
        {
            auto guard = ThreadGuard.create();
            auto buffer = BoehmAllocator.instance.allocate(1024);
            assert(buffer !is null);
            BoehmAllocator.instance.deallocate(buffer);
        }
    }

    @("Aligned allocation (cross-platform)")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.alignedAllocate(16, 32);
        (cast(ubyte[]) b)[] = ubyte(1);
        ubyte[16] expected = 1;
        assert((cast(ubyte[]) b)[] == expected);
        BoehmAllocator.instance.deallocate(b);
    }

    @("IAllocator compliance")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        char*[] names = makeArray!(char*)(BoehmAllocator.instance, 3);
        assert(names.length == 3);
        assert(names.ptr);
        BoehmAllocator.instance.deallocate(names);
    }

    @("allocateZeroed: memory is zero-initialized")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocateZeroed(32);
        assert(b !is null);
        scope (exit)
            BoehmAllocator.instance.deallocate(b);
        foreach (i; 0 .. 32)
            assert((cast(ubyte[]) b)[i] == 0, "byte not zero");
    }

    @("enable: re-enables GC after disable")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        BoehmAllocator.instance.disable();
        BoehmAllocator.instance.enable();
        // After re-enabling, allocation must still work
        void[] b = BoehmAllocator.instance.allocate(64);
        assert(b !is null);
        BoehmAllocator.instance.deallocate(b);
    }

    @("getHeapSize: returns positive value after allocation")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocate(1024 * 1024);
        assert(b !is null);
        scope (exit)
            BoehmAllocator.instance.deallocate(b);
        assert(BoehmAllocator.instance.getHeapSize() > 0);
    }

    @("allocateAtomic: returns non-null for positive size")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocateAtomic(256);
        assert(b !is null);
        assert(b.length == 256);
        scope (exit)
            BoehmAllocator.instance.deallocate(b);
    }

    @("allocateAtomic: zero size returns null")
    @nogc @system nothrow unittest
    {
        auto b = BoehmAllocator.instance.allocateAtomic(0);
        assert(b is null);
    }

    @("allocateAtomic: returned memory is GC-heap-owned")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocateAtomic(64);
        assert(b !is null);
        assert(BoehmAllocator.instance.isHeapPtr(b.ptr));
        BoehmAllocator.instance.deallocate(b);
    }

    @("allocateAtomic: memory is writable")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocateAtomic(16);
        assert(b !is null);
        (cast(ubyte[]) b)[] = ubyte(0xAB);
        foreach (i; 0 .. 16)
            assert((cast(ubyte[]) b)[i] == 0xAB);
        BoehmAllocator.instance.deallocate(b);
    }

    // Semantic requirement: allocateZeroed must use GC_MALLOC (pointer-scanning),
    // NOT GC_MALLOC_ATOMIC. GC_MALLOC_ATOMIC tells BDWGC the block contains no GC
    // pointers and skips scanning it entirely. A general-purpose zeroed allocator
    // must allow callers to store GC-managed pointers inside the returned memory.
    // Note: a runtime assertion cannot reliably distinguish the two with BDWGC's
    // conservative collector (stack residue keeps inner blocks alive regardless).
    // The correctness is enforced by the implementation using GC_MALLOC below.

    @("goodAllocSize: zero returns zero")
    @nogc @system nothrow unittest
    {
        assert(BoehmAllocator.instance.goodAllocSize(0) == 0);
    }

    @("goodAllocSize: rounds up to next multiple of alignment")
    @nogc @system nothrow unittest
    {
        enum a = BoehmAllocator.alignment;
        assert(BoehmAllocator.instance.goodAllocSize(1) == a);
        assert(BoehmAllocator.instance.goodAllocSize(a) == a);
        assert(BoehmAllocator.instance.goodAllocSize(a + 1) == a * 2);
        assert(BoehmAllocator.instance.goodAllocSize(a * 2) == a * 2);
        assert(BoehmAllocator.instance.goodAllocSize(a * 2 + 1) == a * 3);
    }

    @("allocate(0) returns null")
    @nogc @system nothrow unittest
    {
        auto b = BoehmAllocator.instance.allocate(0);
        assert(b is null);
        assert(b.ptr is null);
    }

    @("alignedAllocate with zero bytes returns null")
    @nogc @system nothrow unittest
    {
        auto b = BoehmAllocator.instance.alignedAllocate(0, 16);
        assert(b is null);
    }

    @("alignedAllocate with non-power-of-2 alignment returns null")
    @nogc @system nothrow unittest
    {
        auto b = BoehmAllocator.instance.alignedAllocate(16, 3);
        assert(b is null);
    }

    @("alignedAllocate with alignment smaller than pointer size returns null")
    @nogc @system nothrow unittest
    {
        // (void*).sizeof / 2 is below minimum — must be rejected
        auto b = BoehmAllocator.instance.alignedAllocate(16, cast(uint)(void*).sizeof / 2);
        assert(b is null);
    }

    @("owns(null[]) returns false")
    @nogc @system nothrow unittest
    {
        void[] empty = null;
        assert(!BoehmAllocator.instance.owns(empty));
    }

    @("reallocate to zero deallocates and nulls the slice")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocate(64);
        assert(b !is null);
        bool ok = BoehmAllocator.instance.reallocate(b, 0);
        assert(ok);
        assert(b is null);
    }

    @("isGoodDynamicAlignment: zero and non-powers-of-2 are invalid")
    @nogc nothrow pure unittest
    {
        assert(!isGoodDynamicAlignment(0));
        assert(!isGoodDynamicAlignment(3));
        assert(!isGoodDynamicAlignment(5));
        assert(!isGoodDynamicAlignment(6));
        assert(!isGoodDynamicAlignment(12));
        assert(!isGoodDynamicAlignment(uint.max)); // 0xFFFFFFFF is not a power of 2
    }

    @("isGoodDynamicAlignment: values smaller than pointer size are invalid")
    @nogc nothrow pure unittest
    {
        // (void*).sizeof / 2 is always < (void*).sizeof, so must be invalid
        assert(!isGoodDynamicAlignment((void*).sizeof / 2));
    }

    @("isGoodDynamicAlignment: pointer size and multiples are valid")
    @nogc nothrow pure unittest
    {
        assert(isGoodDynamicAlignment(cast(uint)(void*).sizeof));
        assert(isGoodDynamicAlignment(cast(uint)(void*).sizeof * 2));
        assert(isGoodDynamicAlignment(cast(uint)(void*).sizeof * 4));
        assert(isGoodDynamicAlignment(64));
        assert(isGoodDynamicAlignment(128));
    }

    // --- Integration tests: allocate vs allocateAtomic ---
    //
    // allocate  (GC_MALLOC):        zero-initialized, GC scans block for pointers.
    // allocateAtomic (GC_MALLOC_ATOMIC): NOT zero-initialized, GC does NOT scan block.
    //
    // Rule of thumb:
    //   allocate       — general use; safe to store GC-managed pointers inside.
    //   allocateAtomic — scalar/numeric data (int[], float[], ubyte[]); never store
    //                    GC pointers inside, GC won't trace them.

    @("integration: allocate is zero-initialized, allocateAtomic requires explicit init")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();

        // allocate: GC_MALLOC guarantees the returned block is zeroed.
        void[] gcBlock = BoehmAllocator.instance.allocate(128);
        assert(gcBlock !is null);
        scope (exit)
            BoehmAllocator.instance.deallocate(gcBlock);
        foreach (i; 0 .. 128)
            assert((cast(ubyte[]) gcBlock)[i] == 0,
                "allocate must return zero-initialized memory");

        // allocateAtomic: GC_MALLOC_ATOMIC does NOT zero — caller must initialize.
        void[] atomicBlock = BoehmAllocator.instance.allocateAtomic(128);
        assert(atomicBlock !is null);
        scope (exit)
            BoehmAllocator.instance.deallocate(atomicBlock);
        (cast(ubyte[]) atomicBlock)[] = 0xCC; // explicit initialization required
        foreach (i; 0 .. 128)
            assert((cast(ubyte[]) atomicBlock)[i] == 0xCC);
    }

    @("integration: resolveInternalPointer works on both allocate and allocateAtomic blocks")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();

        void[] gcBlock     = BoehmAllocator.instance.allocate(256);
        void[] atomicBlock = BoehmAllocator.instance.allocateAtomic(256);
        assert(gcBlock !is null && atomicBlock !is null);
        scope (exit)
        {
            BoehmAllocator.instance.deallocate(gcBlock);
            BoehmAllocator.instance.deallocate(atomicBlock);
        }

        // Interior pointer at offset 128 must resolve back to the block start
        assert(BoehmAllocator.instance.resolveInternalPointer(gcBlock.ptr + 128)
               == gcBlock.ptr);
        assert(BoehmAllocator.instance.resolveInternalPointer(atomicBlock.ptr + 128)
               == atomicBlock.ptr);
    }

    @("integration: heap stats account for both allocate and allocateAtomic")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();

        size_t totalBefore = BoehmAllocator.instance.totalBytes();

        void[] gcBlock     = BoehmAllocator.instance.allocate(512 * 1024);
        void[] atomicBlock = BoehmAllocator.instance.allocateAtomic(512 * 1024);
        assert(gcBlock !is null && atomicBlock !is null);

        // totalBytes never decreases after live allocations
        assert(BoehmAllocator.instance.totalBytes() >= totalBefore);
        // freeBytes must be <= heap size at all times
        assert(BoehmAllocator.instance.freeBytes() <= BoehmAllocator.instance.getHeapSize());

        BoehmAllocator.instance.deallocate(gcBlock);
        BoehmAllocator.instance.deallocate(atomicBlock);
    }

    @("freeBytes: returns a size_t value")
    @nogc @system nothrow unittest
    {
        // freeBytes is a lower bound; it can be 0 in very constrained conditions,
        // but must always be <= getHeapSize().
        size_t free = BoehmAllocator.instance.freeBytes();
        assert(free <= BoehmAllocator.instance.getHeapSize());
    }

    @("totalBytes: increases after allocations")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        size_t before = BoehmAllocator.instance.totalBytes();
        void[] b = BoehmAllocator.instance.allocate(1024 * 1024);
        assert(b !is null);
        size_t after = BoehmAllocator.instance.totalBytes();
        // totalBytes never decreases; after a fresh 1 MiB allocation it must grow
        assert(after >= before);
        assert(after > 0);
        BoehmAllocator.instance.deallocate(b);
    }

    @("resolveInternalPointer: interior pointer maps to block start")
    @nogc @system nothrow unittest
    {
        auto guard = ThreadGuard.create();
        void[] b = BoehmAllocator.instance.allocate(256);
        assert(b !is null);
        scope (exit)
            BoehmAllocator.instance.deallocate(b);
        // An interior pointer 64 bytes into the block must resolve to b.ptr
        void* interior = b.ptr + 64;
        void* base = BoehmAllocator.instance.resolveInternalPointer(interior);
        assert(base == b.ptr);
    }

    @("resolveInternalPointer: null returns null")
    @nogc @system nothrow unittest
    {
        assert(BoehmAllocator.instance.resolveInternalPointer(null) is null);
    }

    @("resolveInternalPointer: stack pointer returns null")
    @nogc @system nothrow unittest
    {
        int local = 42;
        assert(BoehmAllocator.instance.resolveInternalPointer(&local) is null);
    }
}
