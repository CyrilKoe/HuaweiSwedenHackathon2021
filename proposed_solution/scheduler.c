#include "scheduler.h"

#include "utils.h"
#include "dag.h"

#include <stdlib.h>
#include <stdio.h>

struct ProcessorSchedule output[numberOfProcessors];

void link_dependancies_to_tasks(int whichDag);

void scheduler(){
    for(int i = 0; i < dagsCount; i++) {
        printf("---DAG %i---\n", i);
        link_dependancies_to_tasks(i);
        //build_max_start_time(i);
    }
}

// O(n*m)
void link_dependancies_to_tasks(int whichDag) {
    task_t *currentTask = input[whichDag]->firstTask;
    while(currentTask != NULL) {
        dependancy_t *currentDep = input[whichDag]->firstDependency;
        while(currentDep != NULL) {
            if(currentDep->beforeID == currentTask->taskID) {
                currentTask->sons[currentTask->num_sons++] = currentDep;
                currentDep->beforeTask = currentTask;
            }
            if(currentDep->afterID == currentTask->taskID) {
                currentTask->parents[currentTask->num_parents++] = currentDep;
                currentDep->afterTask = currentTask;
            } 
            currentDep = currentDep->next;
        }
        currentTask = currentTask->next;
    }
}
