#include "scheduler.h"

#include "utils.h"
#include "dag.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <assert.h>

struct ProcessorSchedule output[numberOfProcessors];

void link_dependancies_to_tasks(int whichDag);
int build_max_start_time(int whichDag, task_t *currentTask);
void add_roots_to_dag(int whichDag);
task_t **ordered_tasks_worst_start(int num_tasks, int dagsCount);
bool first_proc_available();

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

    // Sort all the tasks of all dag by their maximum acceptable start time
    task_t **ordered_tasks = ordered_tasks_worst_start(num_tasks, dagsCount);

    // Just verify the sort
    int last_worst_start = 0;
    for (int i = 0; i < num_tasks; i++)
    {
        assert(last_worst_start <= ordered_tasks[i]->worstStart);
        last_worst_start = ordered_tasks[i]->worstStart;
    }

    // Easy attribution
    for (int i = 0; i < num_tasks; i++)
    {
        int procID = first_proc_available();
        int last_task_size = output[procID].numberOfTasks;
        // Attribute it with lookup history and idk
        
    }
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
    int history = 0;
    for (int j = output[procID].numberOfTasks - 1; j >= 0; j--)
    {
        if (output[procID].tasks[j]->taskType == task->taskType)
            return true;
        history++;
        if (history == historyOfProcessor)
            return false;
    }
    return false;
}

bool first_proc_available()
{
    int min = 1<<30;
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
int partition(task_t **arr, int low, int high)
{
    task_t *pivot = arr[high];
    int i = (low - 1);
    for (int j = low; j <= high - 1; j++)
    {
        if (arr[j]->worstStart <= pivot->worstStart)
        {
            i++;
            swap(&arr[i], &arr[j]);
        }
    }
    swap(&arr[i + 1], &arr[high]);
    return (i + 1);
}

// Quick sort algo
void quickSort(task_t **arr, int low, int high)
{
    if (low < high)
    {
        int pi = partition(arr, low, high);
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

// O(nlog(n))
task_t **ordered_tasks_worst_start(int num_tasks, int dagsCount)
{
    task_t **result = malloc(sizeof(task_t *) * num_tasks);
    int ptr = 0;

    for (int whichDag = 0; whichDag < dagsCount; whichDag++)
    {
        task_t *currentTask = input[whichDag]->firstTask;
        while (currentTask != NULL)
        {
            result[ptr] = currentTask;
            currentTask = currentTask->next;
            ptr++;
        }
    }

    assert(ptr == num_tasks);

    quickSort(result, 0, num_tasks - 1);

    return result;
}