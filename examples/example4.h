#ifndef PERSON_H
#define PERSON_H

#include <gc_cpp.h>

// default: extern(C++)

struct Person {
    char* name;
    int age;
};

Person* newPerson();
void createPerson(Person* p, const char* name, int age);

#endif // PERSON_H
