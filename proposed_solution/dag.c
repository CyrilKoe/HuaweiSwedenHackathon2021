
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

task_t *new_task_t() {
    task_t *result = malloc(sizeof(task_t));
    result->num_parents = 0;
    result->num_sons = 0;
    memset(result->sons, 0x0, sizeof(dependancy_t *)*tasksPerGraph);
    memset(result->parents, 0x0, sizeof(dependancy_t *)*tasksPerGraph);
    return result;
}

dependancy_t *new_dependancy_t() {
    dependancy_t *result = malloc(sizeof(dependancy_t));
    result->beforeTask = NULL;
    result->afterTask = NULL;
    return result;
}

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
        input[whichDag]->listOfTasks = new_task_t();
        input[whichDag]->lastTask = input[whichDag]->listOfTasks;
        input[whichDag]->firstTask = input[whichDag]->lastTask;
    }
    else{
        input[whichDag]->lastTask->next = new_task_t();
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
        input[whichDag]->listOfDependencies = malloc(sizeof(dependancy_t));
        input[whichDag]->lastDependency = input[whichDag]->listOfDependencies;
        input[whichDag]->firstDependency = input[whichDag]->lastDependency;
    }
    else{
        input[whichDag]->lastDependency->next = malloc(sizeof(dependancy_t));
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
    task_t * current = input[whichDag]->firstTask;
    while(current != NULL){
        printf("%d ", current->taskID);
        current = current->next;
    }
    printf("\n");
}

void print_dag_dependencies(int whichDag){ // "whichDag" is index of DAG in array "input"
    dependancy_t * current = input[whichDag]->firstDependency;
    while(current != NULL){
        printf("FROM: %d TO: %d COST: %d\n", current->beforeID, current->afterID, current->transferTime);
        current = current->next;
    }
    printf("\n");
}
