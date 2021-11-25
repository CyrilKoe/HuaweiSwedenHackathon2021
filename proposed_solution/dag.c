
#include "dag.h"

#include "utils.h"

#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h> 
#include <math.h>

struct DAG * input[N];
int dagsCount = 0; // total number of DAGs (0 indexed in array "input")

/////////////////////////
// CREATE FUNCTIONS /////
/////////////////////////

void initialize(int whichDag){
    input[whichDag] = malloc(sizeof(struct DAG));
    input[whichDag]->listOfTasks = NULL;
    input[whichDag]->listOfDependencies = NULL;
    input[whichDag]->lastDependency = NULL;
    input[whichDag]->lastTask = NULL;
    input[whichDag]->firstDependency = NULL;
    input[whichDag]->firstTask = NULL;
}

void add_task_to_list(int whichDag, int taskID, int executionTime, int taskType){  
    input[whichDag]->tasksCount++;
    if(input[whichDag]->lastTask == NULL){
        input[whichDag]->listOfTasks = malloc(sizeof(struct task));
        input[whichDag]->lastTask = input[whichDag]->listOfTasks;
        input[whichDag]->firstTask = input[whichDag]->lastTask;
    }
    else{
        input[whichDag]->lastTask->next = malloc(sizeof(struct task));
        input[whichDag]->lastTask = input[whichDag]->lastTask->next;
    }
    input[whichDag]->lastTask->taskID = taskID;
    input[whichDag]->lastTask->executionTime = executionTime;
    input[whichDag]->lastTask->taskType = taskType;
    input[whichDag]->lastTask->next = NULL;
    return;
}

void add_dependency_to_list(int whichDag, int beforeID, int afterID, int transferTime){
    
    if(input[whichDag]->lastDependency == NULL){
        input[whichDag]->listOfDependencies = malloc(sizeof(struct dependency));
        input[whichDag]->lastDependency = input[whichDag]->listOfDependencies;
        input[whichDag]->firstDependency = input[whichDag]->lastDependency;
    }
    else{
        input[whichDag]->lastDependency->next = malloc(sizeof(struct dependency));
        input[whichDag]->lastDependency = input[whichDag]->lastDependency->next;
    }
    input[whichDag]->lastDependency->beforeID = beforeID;
    input[whichDag]->lastDependency->afterID = afterID;
    input[whichDag]->lastDependency->transferTime = transferTime;
    input[whichDag]->lastDependency->next = NULL;
    return;
}


/////////////////////////
// PRINT FUNCTIONS //////
/////////////////////////


void print_dag_tasks(int whichDag){ // "whichDag" is index of DAG in array "input"
    struct task * current = input[whichDag]->firstTask;
    while(current != NULL){
        printf("%d ", current->taskID);
        current = current->next;
    }
    printf("\n");
}

void print_dag_dependencies(int whichDag){ // "whichDag" is index of DAG in array "input"
    struct dependency * current = input[whichDag]->firstDependency;
    while(current != NULL){
        printf("FROM: %d TO: %d COST: %d\n", current->beforeID, current->afterID, current->transferTime);
        current = current->next;
    }
    printf("\n");
}
