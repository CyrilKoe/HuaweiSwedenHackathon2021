#ifndef DAG_H__
#define DAG_H__

#include "utils.h"

#include <stdbool.h>

struct dependancy;
struct task;

typedef struct task{
    int taskID;
    int executionTime;
    int taskType;
    struct task *next;
    // Added //
    int whichDag;
    int remainingTime; // gives the worst time until the end of this DAG
    int worstStart; // gives the worst time to start
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
    // Added //
    task_t *beforeTask;
    task_t *afterTask;
    bool is_scheduled;
} dependancy_t;

typedef struct DAG{
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
    // Added //
    task_t * root; // Fake node representing a root
    bool is_scheduled; // Used for full dag scheduling
} dag_t;

task_t* find_task_in_dag(int taskID, int whichDag);

task_t *new_task_t();
void initialize(int whichDag);
void add_task_to_list(int whichDag, int taskID, int executionTime, int taskType);
void add_dependency_to_list(int whichDag, int beforeID, int afterID, int transferTime);

extern int dagsCount; // total number of DAGs (0 indexed in array "input")
extern struct DAG * input[N];

void print_task(task_t *currentTask);
void print_dag_tasks(int whichDag, bool extended);
void print_dag_dependencies(int whichDag);

#endif