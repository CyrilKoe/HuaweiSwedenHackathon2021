#include "scheduler.h"

#include "utils.h"
#include "dag.h"

struct ProcessorSchedule output[numberOfProcessors];

void scheduler(){
    for(int i = 0; i < dagsCount; i++) {
        printf("---DAG %i---\n", i);
        print_dag_tasks(i);
        print_dag_dependencies(i);
    }
}