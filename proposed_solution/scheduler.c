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
    for (int i = 0; i < num_tasks; i++) {
        assert(last_worst_start < ordered_tasks[i]->worstStart);
        //printf("%i ", ordered_tasks[i]->worstStart);
    }
    //printf("\n");
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
    int upstream_max_time = 0;

    if (currentTask->num_sons == 0)
    {
        // Last task just has his worst execution time
        upstream_max_time = currentTask->executionTime;
        currentTask->remainingTime = upstream_max_time;
        currentTask->worstStart = input[whichDag]->deadlineTime - currentTask->remainingTime;
        // Pass it to the task before
        return upstream_max_time;
    }

    int tmp_max_time = 0;
    for (int i = 0; i < currentTask->num_sons; i++)
    {
        // Tasks before first removes the worst communication time
        tmp_max_time = build_max_start_time(whichDag, currentTask->sons[i]->afterTask) + currentTask->sons[i]->transferTime;
        // Keep the shortest time as the worst
        if (tmp_max_time > upstream_max_time)
            upstream_max_time = tmp_max_time;
    }
    // Remove its own execution time
    upstream_max_time = upstream_max_time + currentTask->executionTime;
    currentTask->remainingTime = upstream_max_time;
    currentTask->worstStart = input[whichDag]->deadlineTime - currentTask->remainingTime;
    // Pass it to the task before
    return upstream_max_time;
}

// Quick sort algo
void swap(task_t **a, task_t **b)
{
    task_t * t = *a;
    *a = *b;
    *b = t;
}

// Quick sort algo
int partition(task_t ** arr, int low, int high)
{
    task_t * pivot = arr[high];
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
void quickSort(task_t ** arr, int low, int high)
{
    if (low < high)
    {
        int pi = partition(arr, low, high);
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

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

    quickSort(result, 0, num_tasks-1);

    return result;
}