/++
 + -betterC compatible example using the BoehmAllocator to manage C++ Person structs
 + via C++ FFI, populate them, collect garbage, and print details.
 +/
import bdwgc;

extern (C++) {
    struct Person {
        char* name;
        int age;
    }

    Person* newPerson() @nogc nothrow;
    void createPerson(Person* p, const(char)* name, int age) @nogc nothrow;
}

// Compile-time data for Person instances
private struct PersonData {
    const(char)* name;
    int age;
}

extern (C) @nogc nothrow:

void main() @trusted {
    // Register main thread for BDWGC
    auto guard = ThreadGuard.create();
    debug
        GC_printf("Main started\n");

    // Define Person data
    immutable PersonData[2] peopleData = [
        PersonData("John".ptr, 42),
        PersonData("Sarah".ptr, 35)
    ];
    Person*[2] people;

    // Create and populate Person instances
    foreach (i, ref p; people) {
        p = newPerson();
        if (!p) {
            debug
                GC_printf("Failed to create Person #%zu\n", i);
            goto cleanup;
        }
        createPerson(p, peopleData[i].name, peopleData[i].age);
        if (!p.name) {
            debug
                GC_printf("Failed to initialize Person #%zu name\n", i);
            goto cleanup;
        }
    }

    foreach (i, p; people) {
        GC_printf("%s is %d years old.\n", p.name, p.age);
    }

    // Perform GC collection and report heap size
    BoehmAllocator.instance.collect();
    debug
    {
        GC_printf("GC collection triggered\n");
        GC_printf("heap size after collect: %zu bytes\n", BoehmAllocator.instance.getHeapSize());
    }

cleanup:
    // Clear references to aid GC
    foreach (ref p; people)
        p = null;

    debug
        GC_printf("Main finishing\n");
}
