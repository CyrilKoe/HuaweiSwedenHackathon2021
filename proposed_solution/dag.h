#ifndef DAG_H__
#define DAG_H__

#include "utils.h"

struct task{
    int taskID;
    int executionTime;
    int taskType;
    struct task * next;
};

struct dependency{ //"afterID" can be executed only after finishing "beforeID"
    int beforeID;
    int afterID;
    int transferTime;
    struct dependency * next;
};

struct DAG{
    int dagID;
    int dagType; 
    int arrivalTime;
    int deadlineTime;
    int tasksCount;
    struct task * listOfTasks; // list of all tasks (just their IDs and execution times, order doesn't matter) of DAG
    struct dependency * listOfDependencies;// all edges (dependencies) of DAG (NULL if there are no dependencies)
    struct task * lastTask;
    struct dependency * lastDependency;
    struct task * firstTask;
    struct dependency * firstDependency;
};

void initialize(int whichDag);

void add_task_to_list(int whichDag, int taskID, int executionTime, int taskType);
void add_dependency_to_list(int whichDag, int beforeID, int afterID, int transferTime);

extern int dagsCount; // total number of DAGs (0 indexed in array "input")
extern struct DAG * input[N];

#endif