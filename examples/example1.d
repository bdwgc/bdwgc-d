/++
 + Example using the BDWGC allocator to allocate and reallocate memory,
 + printing heap size periodically.
 +/
import bdwgc;

enum max = 10_000_000;

extern (C)
void main() @trusted
{
    foreach (i; 0 .. max)
    {
        // Allocate pointer-sized memory for p (zero-initialized, pointer-scannable)
        void[] pBuf = BoehmAllocator.instance.allocateZeroed(size_t.sizeof);
        if (!pBuf.ptr)
        {
            debug
                GC_printf("Failed to allocate p at iteration %d\n", cast(int)i);
            break;
        }
        auto p = cast(int**) pBuf.ptr;

        // Allocate pointer-sized memory for q
        void[] qBuf = BoehmAllocator.instance.allocate(size_t.sizeof);
        if (!qBuf.ptr)
        {
            debug
                GC_printf("Failed to allocate q at iteration %d\n", cast(int)i);
            BoehmAllocator.instance.deallocate(pBuf);
            break;
        }

        // Reallocate q to twice the size and assign to *p
        if (!BoehmAllocator.instance.reallocate(qBuf, size_t.sizeof * 2))
        {
            debug
                GC_printf("Failed to reallocate q at iteration %d\n", cast(int)i);
            BoehmAllocator.instance.deallocate(pBuf);
            BoehmAllocator.instance.deallocate(qBuf);
            break;
        }
        *p = cast(int*) qBuf.ptr;

        // Periodically print heap size
        if (i % 100_000 == 0)
        {
            const heap = BoehmAllocator.instance.getHeapSize();
            version (Windows)
            {
                GC_printf("heap size: %u\n", cast(uint) heap);
            }
            else
            {
                GC_printf("heap size: %ld\n", heap);
            }
        }
    }
}
