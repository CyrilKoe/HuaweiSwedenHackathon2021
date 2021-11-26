#include "scheduler.h"

#include "utils.h"
#include "dag.h"

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <assert.h>

struct ProcessorSchedule output[numberOfProcessors];

void link_dependancies_to_tasks(int whichDag);
int build_max_start_time(int whichDag, task_t *currentTask);
void add_roots_to_dag(int whichDag);
task_t **ordered_tasks_criteria(int num_tasks, int whichDag, int (*criteria)(task_t *));
int first_proc_available();

///// CRITERIAS ////
int get_remaining_time(task_t *task) {
    return - task->remainingTime;
}

///// SCHEDULER /////
void scheduler()
{
    int num_tasks = 0;

    for (int i = 0; i < dagsCount; i++)
    {
        //printf("---DAG [id] %i [arrival] %i, [deadline] %i ---\n", i, input[i]->arrivalTime, input[i]->deadlineTime);
        num_tasks += input[i]->tasksCount;
        link_dependancies_to_tasks(i);
        add_roots_to_dag(i);
        build_max_start_time(i, input[i]->root);
        //print_dag_tasks(i, true);
    }


    // Attributes 
    for (int i = 0; i < num_tasks; i++)
    {
        int procID = first_proc_available();
        int last_task_size = output[procID].numberOfTasks;
        // Attribute it with lookup history and idk
        
    }


    for (int dagIdx = 0; dagIdx < dagsCount; dagIdx++) {
        //printf("dag number %d with exec time %d\n", dagIdx, compute_max_dag_execution_time(dagIdx));
        
        task_t **ordered_tasks_topo = ordered_tasks_criteria(input[dagIdx]->tasksCount, dagIdx, &get_remaining_time);
        //printf("Dag number %d topo order of %d tasks: ", dagIdx, input[dagIdx]->tasksCount);
        for (int j = 0; j < input[dagIdx]->tasksCount; j++) {
            //printf("%d ", ordered_tasks_topo[j]->taskID); 
        }
        //printf("\n");
    }

    // Until all dags are passed    
    for (int i = 0; i < dagsCount; i++) {
        // Find the first DAG available
        int chosen_dag = -1;
        int min_arrival_time = INT_MAX;

        for (int j = 0; j < dagsCount; j++) {
            if (!input[j]->is_scheduled && input[j]->arrivalTime < min_arrival_time) {
                min_arrival_time = input[i]->arrivalTime;
                chosen_dag = j;
            }
        }

        int chosen_PN = first_proc_available();
        task_t **ordered_tasks_topo = ordered_tasks_criteria(input[chosen_dag]->tasksCount, chosen_dag, &get_remaining_time);
        input[chosen_dag]->is_scheduled = true;

        for(int task_idx = 0; task_idx < input[chosen_dag]->tasksCount; task_idx++) {

            task_t *task_to_add = ordered_tasks_topo[task_idx];
            
            int last_task_scheduled = output[chosen_PN].numberOfTasks;
            int start_time;
            if (last_task_scheduled != 0) {
                start_time = output[chosen_PN].startTime[last_task_scheduled-1] + output[chosen_PN].exeTime[last_task_scheduled-1];
            } else {
                start_time = 0;
            }
            if (start_time < input[chosen_dag]->arrivalTime) {
                start_time = input[chosen_dag]->arrivalTime;
            }
            output[chosen_PN].startTime[last_task_scheduled] = start_time;
            output[chosen_PN].exeTime[last_task_scheduled] = task_to_add->executionTime;
            output[chosen_PN].taskIDs[last_task_scheduled] = task_to_add->taskID;
            output[chosen_PN].numberOfTasks++;
            assert(last_task_scheduled < N);
        
        }
    }

    // find an idle PN and ask it to schedule the DAG
    
}


// O(n*m) fill the pointers between dependancies and tasks
void link_dependancies_to_tasks(int whichDag)
{
    // For each task and for each dep
    task_t *currentTask = input[whichDag]->firstTask;
    while (currentTask != NULL)
    {
        // By the way save the corresponding dag id
        currentTask->whichDag = whichDag;
        dependancy_t *currentDep = input[whichDag]->firstDependency;
        while (currentDep != NULL)
        {
            // If this dep has this node as a source
            if (currentDep->beforeID == currentTask->taskID)
            {
                currentTask->sons[currentTask->num_sons++] = currentDep;
                currentDep->beforeTask = currentTask;
            }
            // If this dep has this node as a target
            if (currentDep->afterID == currentTask->taskID)
            {
                currentTask->parents[currentTask->num_parents++] = currentDep;
                currentDep->afterTask = currentTask;
            }
            currentDep = currentDep->next;
        }
        currentTask = currentTask->next;
    }
}

bool lookup_history(int procID, task_t *task)
{
    int history_depth = 0;
    for (int j = output[procID].numberOfTasks - 1; j >= 0; j--)
    {
        if (output[procID].tasks[j]->taskType == task->taskType)
            return true;
        history_depth++;
        if (history_depth == historyOfProcessor)
            return false;
    }
    return false;
}

// Return the first proc avaiable knowing that we started filling output with tasks
int first_proc_available()
{
    int min = INT_MAX;
    int min_id = 0;
    for (int procID = 0; procID < numberOfProcessors; procID++)
    {
        int last_task = output[procID].numberOfTasks-1;
        if(last_task == -1) {
            return procID;
        }
        int last_execution_end = output[procID].startTime[last_task]+output[procID].exeTime[last_task];
        if(last_execution_end < min) {
            min_id = procID;
            min = last_execution_end;
        }
    }
    return min_id;
}

// Create a fake node to have a unique root in each dag
void add_roots_to_dag(int whichDag)
{
    task_t *root = new_task_t();
    root->taskID = -1;
    root->executionTime = 0;
    root->taskType = -1;
    root->next = NULL;

    task_t *currentTask = input[whichDag]->firstTask;
    while (currentTask != NULL)
    {
        if (currentTask->num_parents == 0)
        {
            // Create a fake dependancy
            dependancy_t *dep = malloc(sizeof(dependancy_t));
            dep->beforeID = -1;
            dep->afterID = currentTask->taskID;
            dep->transferTime = 0;
            dep->next = NULL;
            dep->beforeTask = root;
            dep->afterTask = currentTask;
            // Add the fake relation to the fake root
            root->sons[root->num_sons++] = dep;
        }
        currentTask = currentTask->next;
    }

    input[whichDag]->root = root;
}

// O(n) computes the remaining time until dag termination
// remaining time : path that takes the longest time to be executed
int build_max_start_time(int whichDag, task_t *currentTask)
{
    if (currentTask->num_sons == 0)
    {
        // Last task just has his worst execution time
        int upstream_time = currentTask->executionTime;
        if (upstream_time > currentTask->remainingTime)
        {
            currentTask->remainingTime = upstream_time;
            currentTask->worstStart = input[whichDag]->deadlineTime - currentTask->remainingTime;
        }
        // Pass it to the task before
        return currentTask->remainingTime;
    }

    for (int i = 0; i < currentTask->num_sons; i++)
    {
        // Tasks before first removes the worst communication time
        int upstream_time = build_max_start_time(whichDag, currentTask->sons[i]->afterTask) + currentTask->sons[i]->transferTime;
        // Keep the longest time as the worst
        if (upstream_time > currentTask->remainingTime)
        {
            currentTask->remainingTime = upstream_time;
            currentTask->worstStart = input[whichDag]->deadlineTime - currentTask->remainingTime;
        }
    }

    // Pass it to the task before
    return currentTask->remainingTime;
}


// Quick sort algo
void swap(task_t **a, task_t **b)
{
    task_t *t = *a;
    *a = *b;
    *b = t;
}

// Quick sort algo
int partition(task_t **arr, int low, int high, int (*criteria)(task_t *))
{
    task_t *pivot = arr[high];
    int i = (low - 1);
    for (int j = low; j <= high - 1; j++)
    {
        if (criteria(arr[j]) <= criteria(pivot))
        {
            i++;
            swap(&arr[i], &arr[j]);
        }
    }
    swap(&arr[i + 1], &arr[high]);
    return (i + 1);
}

// Quick sort algo
void quickSort(task_t **arr, int low, int high, int (*criteria)(task_t *))
{
    if (low < high)
    {
        int pi = partition(arr, low, high, criteria);
        quickSort(arr, low, pi - 1, criteria);
        quickSort(arr, pi + 1, high, criteria);
    }
}

// O(nlog(n)) Order the taks sof the DAG nb dagCount by the criteria
task_t **ordered_tasks_criteria(int num_tasks, int whichDag, int (*criteria)(task_t *))
{
    task_t **result = malloc(sizeof(task_t *) * num_tasks);
    int ptr = 0;

    
    task_t *currentTask = input[whichDag]->firstTask;
    while (currentTask != NULL)
    {
        result[ptr] = currentTask;
        currentTask = currentTask->next;
        ptr++;
    }
    

    assert(ptr == num_tasks);

    quickSort(result, 0, num_tasks - 1, criteria);

    return result;
}

// UTILS 
int compute_max_dag_execution_time(int dag_index) {
    int exec_time = 0;
    task_t *task = input[dag_index]->listOfTasks;
    while ((task = task->next) != NULL) 
    {
        exec_time += task->executionTime;
    }

    return exec_time;
}

int compute_actual_execution_time(int procID, task_t *task) {
    if (lookup_history(procID, task)) {
        return 9 * task->executionTime / 10;
    } else return task->executionTime;
}