#ifndef DAG_H__
#define DAG_H__

#include "utils.h"

struct dependancy;
struct task;

typedef struct task{
    int taskID;
    int executionTime;
    int taskType;
    struct task *next;
    // Added
    int maxStartTime; // gives the worst time to start a task in order to finish the tasks after in time
    int num_sons;
    struct dependancy *sons[tasksPerGraph];
    int num_parents;
    struct dependancy *parents[tasksPerGraph];
    
} task_t;

typedef struct dependancy{ //"afterID" can be executed only after finishing "beforeID"
    int beforeID;
    int afterID;
    int transferTime;
    struct dependancy * next;
    // Added
    task_t *beforeTask;
    task_t *afterTask;
} dependancy_t;

struct DAG{
    int dagID;
    int dagType; 
    int arrivalTime;
    int deadlineTime;
    int tasksCount;
    task_t * listOfTasks; // list of all tasks (just their IDs and execution times, order doesn't matter) of DAG
    dependancy_t * listOfDependencies;// all edges (dependencies) of DAG (NULL if there are no dependencies)
    task_t * lastTask;
    dependancy_t * lastDependency;
    task_t * firstTask;
    dependancy_t * firstDependency;
};

task_t* find_task_in_dag(int taskID, int whichDag);

void initialize(int whichDag);

void add_task_to_list(int whichDag, int taskID, int executionTime, int taskType);
void add_dependency_to_list(int whichDag, int beforeID, int afterID, int transferTime);

extern int dagsCount; // total number of DAGs (0 indexed in array "input")
extern struct DAG * input[N];

void print_dag_tasks(int whichDag);
void print_dag_dependencies(int whichDag);

#endif