#include "scheduler.h"

#include "utils.h"
#include "dag.h"

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <assert.h>

/////////////////////////////
// ENUMS ////////////////////
/////////////////////////////

typedef struct augmented_task
{
    task_t *task;
    bool is_scheduled;
    int which_pn;
    int start_time;
    int num_sons;
    struct augmented_task **sons;
    int num_parents;
    struct augmented_task **parents;
} augmented_task_t;

typedef struct augmented_dag
{
    dag_t *dag;
    int size;
    augmented_task_t *elems[tasksPerGraph];
    augmented_task_t *root;
    bool is_completed;
} augmented_dag_t;

typedef struct queue_dps
{
    int first;
    int last;
    augmented_task_t *elems[tasksPerGraph];
} queue_dfs_t;

typedef struct scheduling_dag
{
    augmented_dag_t *aug_dag;
    queue_dfs_t *queue;
    int cpus[numberOfProcessors];
} scheduling_dag_t;

typedef struct history
{
    augmented_dag_t *cpu_history[numberOfProcessors];
    int last_addeds[numberOfProcessors];
} history_t;

struct ProcessorSchedule output[numberOfProcessors];

void link_dependancies_to_tasks(int whichDag);
void add_roots_to_dag(int whichDag);

task_t **order_tasks_criteria(int num_tasks, task_t **to_order, int (*criteria)(task_t *));

int compute_estimation_completion_time(int numProc, int whichDag);
int average_parallel_nodes(int dagId, int start_time, int stop_time);

void shitty_sort(int *T, int N_arr);

scheduling_dag_t *new_scheduling_dag(task_t *root);

/////////////////////////////
// MAIN /////////////////////
/////////////////////////////

void scheduler()
{
    int num_tasks = 0;

    int *dag_arrivals = malloc(dagsCount * sizeof(int));
    scheduling_dag_t **scheduling_dags = malloc(sizeof(scheduling_dag_t *) * dagsCount);
    for (int i = 0; i < dagsCount; i++)
    {
        scheduling_dags[i] = NULL;
        dag_arrivals[i] = input[i]->arrivalTime;
        num_tasks += input[i]->tasksCount;
        link_dependancies_to_tasks(i);
        add_roots_to_dag(i);
    }
    shitty_sort(dag_arrivals, dagsCount);

    int current_time = dag_arrivals[0];
    while (1)
    {
        // Count num dag ready
        for (int i = 0; i < dagsCount; i++)
        {
            if ((scheduling_dags[i] == NULL) && (input[i]->arrivalTime <= current_time))
            {
                scheduling_dags[i] = new_scheduling_dag(input[i]->root);
            }
        }
    }
}

/////////////////////////////
// HELPER START //////////////
/////////////////////////////

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

void shitty_sort(int *T, int N_arr)
{
    int i, j, c;
    for (i = 0; i < N_arr - 1; i++)
    {
        for (j = i + 1; j < N_arr; j++)
        {
            if (T[i] > T[j])
            {
                c = T[i];
                T[i] = T[j];
                T[j] = c;
            }
        }
    }
}

/////////////////////////////
// QUEUE DPS ////////////////
/////////////////////////////

augmented_task_t *new_augmented_task(task_t *task)
{
    augmented_task_t *result = malloc(sizeof(augmented_task_t));
    result->task = task;
    result->which_pn = -1;
    result->start_time = -1;
    result->num_sons = task->num_sons;
    result->num_parents = task->num_parents;
    result->sons = malloc(task->num_sons * sizeof(augmented_task_t *));
    for(int i = 0; i < result->num_sons; i++)
        result->sons[i] = NULL;
    result->parents = malloc(task->num_parents * sizeof(augmented_task_t *));
    for(int i = 0; i < result->num_parents; i++)
        result->parents[i] = NULL;
    return result;
}

augmented_task_t *
new_augmented_task_recu(augmented_dag_t *aug_dag, task_t *task)
{
    augmented_task_t *result = malloc(sizeof(augmented_task_t));
    result->task = task;
    result->which_pn = -1;
    result->start_time = -1;
    result->num_sons = 0;
    result->num_parents = 0;
    result->sons = malloc(task->num_sons * sizeof(augmented_task_t *));
    result->parents = malloc(task->num_parents * sizeof(augmented_task_t *));
    if (task->taskID != -1)
        aug_dag->elems[aug_dag->size++] = result;
    else
        aug_dag->root = result;
    for (int i = 0; i < result->num_sons; i++)
    {
        augmented_task_t *new_son = NULL;
        for (int j = 0; j < aug_dag->size; j++)
        {
            if (aug_dag->elems[j]->task == task->sons[i]->afterTask)
            {
                new_son = aug_dag->elems[j];
                break;
            }
        }
        if (new_son == NULL)
        {
            new_son = new_augmented_task_recu(aug_dag, task->sons[i]->afterTask);
        }

        new_son->parents[new_son->num_parents++] = result;
        result->sons[result->num_sons++] = new_son;
    }

    return result;
}

augmented_dag_t *new_augmented_dag_recu(task_t *root)
{
    augmented_dag_t *result = malloc(sizeof(augmented_dag_t));
    result->size = 0;
    for (int i = 0; i < tasksPerGraph; i++)
    {
        result->elems[i] = NULL;
    }
    result->root = new_augmented_task_recu(result, root);
    return result;
}

void link_augmented_dag(augmented_dag_t *aug_dag)
{
    for (int i = 0; i < aug_dag->size; i++)
    {
        for (int j = 0; j < aug_dag->elems[i]->task->num_sons; j++)
            for (int k = 0; k < aug_dag->size; k++)
                if (aug_dag->elems[i]->task->sons[j]->afterTask->taskID == aug_dag->elems[k]->task->taskID)
                    aug_dag->elems[i]->sons[j] = aug_dag->elems[k];
        for (int j = 0; j < aug_dag->elems[i]->task->num_parents; j++)
            for (int k = 0; k < aug_dag->size; k++)
                if (aug_dag->elems[i]->task->parents[j]->beforeTask->taskID == aug_dag->elems[k]->task->taskID)
                    aug_dag->elems[i]->parents[j] = aug_dag->elems[k];
    }
}

augmented_dag_t *new_augmented_dag(dag_t *dag)
{
    augmented_dag_t *result = malloc(sizeof(augmented_dag_t));
    result->dag = dag;
    result->is_completed = false;
    result->root = new_augmented_dag_task(dag->root);
    result->size = dag->tasksCount;
    task_t *task = dag->firstTask;
    int i = 0;
    while (task != NULL)
    {
        result->elems[i] = new_augmented_task(task);
        i++;
        task = task->next;
    }
    link_augmented_dag(result);
    return result;
}

augmented_dag_t copy_augmented_dag(augmented_dag_t *origin)
{
    augmented_dag_t *result = new_augmented_dag(origin->dag);

    for (int i = 0; i < result->size; i++)
    {
        result->elems[i]->is_scheduled =  origin->elems[i]->is_scheduled;
        result->elems[i]->start_time =  origin->elems[i]->start_time;
        result->elems[i]->which_pn =  origin->elems[i]->which_pn;
    }
}

void remove_augmented_dag(augmented_dag_t *aug_dag)
{
}

// Prepare a queue and all the graph fake nodes for a true graph
queue_dfs_t *new_queue()
{
    queue_dfs_t *result = malloc(sizeof(queue_dfs_t *));
    result->first = 0;
    result->last = 0;
}

scheduling_dag_t *new_scheduling_dag(task_t *root)
{
    scheduling_dag_t *result = malloc(sizeof(scheduling_dag_t));
    result->aug_dag = new_augmented_dag(root);
    result->queue = new_queue();
    for (int i = 0; i < numberOfProcessors; i++)
    {
        result->cpus[i] = -1;
    }
}

void remove_scheduling_dag(scheduling_dag_t *sch_dag)
{
    remove_audgmented_dag(sch_dag->aug_dag);
    free(sch_dag);
}

scheduling_dag_t *copy_scheduling_dag(scheduling_dag_t *origin)
{
    scheduling_dag_t *result = malloc(sizeof(scheduling_dag_t));
    result->aug_dag = copy_augmented_dag(origin->aug_dag);
}

augmented_task_t *dequeue(queue_dfs_t *queue)
{
    if (queue->first == queue->last)
    {
        return NULL;
    }
    augmented_task_t *result = queue->elems[queue->first++];
    if (queue->first == tasksPerGraph)
        queue->first = 0;
    return result;
}

void enqueue(queue_dfs_t *queue, augmented_task_t *elem)
{
    queue->elems[queue->last++] = elem;
    if (queue->last == tasksPerGraph)
        queue->last = 0;
    assert(queue->first != queue->last);
}

/////////////////////////////////////
// DFS MULTI TASKS HELPERS //////////
/////////////////////////////////////

history_t *create_history()
{
    history_t *result = malloc(sizeof(history_t));
    for (int i = 0; i < numberOfProcessors; i++)
    {
        result->cpu_history[i] = malloc(N * sizeof(augmented_task_t *));
        result->last_added[i] = 0;
    }
    return result;
}

bool lookup_history(augmented_task_t *element, history_t *history, int pn)
{
    int history_depth = 0;
    for (int j = history->last_added[pn] - 1; j >= 0; j--)
    {
        if (output[pn].tasks[j]->taskType == element->task->taskType)
            return true;
        history_depth++;
        if (history_depth == historyOfProcessor)
            return false;
    }
    return false;
}

int get_elem_ready_time(augmented_task_t *elem, int pn)
{
    int result = 0;
    for (int i = 0; i < elem->num_parents; i++)
    {
        int this_time = elem->parents[i]->start_time + elem->parents[i]->task->executionTime;
        if (pn != elem->parents[i]->which_pn)
        {
            this_time += elem->task->parents[i]->transferTime;
        }
        if (this_time > result)
        {
            result = this_time;
        }
    }
    return result;
}

int are_parents_scheduled(augmented_task_t *elem)
{
    for (int i = 0; i < elem->num_parents; i++)
    {
        if (!elem->parents[i]->is_scheduled)
            return false;
    }
    return true;
}

int is_elem_ready(augmented_task_t *elem, int time, int pn)
{
    if (!are_parents_scheduled(elem))
        return false;
    return get_elem_ready_time(elem, pn) >= time;
}

// Place the first element to be ready on the front
int place_first_element_ready(queue_dfs_t *queue, int cpu)
{
    assert(queue->first != queue->last);

    int current = queue->first;
    int min_ready_time = INT_MAX;
    int min_index = -1;
    while (current != queue->last)
    {
        int this_time = get_elem_ready_time(queue->elems[current], cpu);
        if (min_ready_time > this_time)
        {
            min_ready_time = this_time;
            min_index = current;
        }
        if (current++ == queue->last)
            current = 0;
    }
    augmented_task_t *tmp = queue->elems[queue->first];
    queue->elems[queue->first] = queue->elems[min_index];
    queue->elems[min_index] = tmp;
    return min_ready_time;
}

int execute_elem(augmented_task_t *elem, int time, int pn)
{
    assert(is_elem_ready(elem, time, pn));
    elem->is_scheduled = true;
    elem->start_time = time;
    elem->which_pn = pn;
    return time + elem->task->executionTime;
}

void multi_dfs(int allocated_cpus[numberOfProcessors], queue_dfs_t *queue, int start_time, int stop_time)
{
    int cpu_times[numberOfProcessors];
    task_t *to_execute[numberOfProcessors];
    for (int i = 0; i < numberOfProcessors; i++)
    {
        cpu_times[i] = start_time;
        to_execute[i] = NULL;
    }

    int current_time = start_time;

    while (1)
    {
        for (int which_cpu = 0; which_cpu < numberOfProcessors; which_cpu++)
        {
            // Non allocated or busy
            if (allocated_cpus[which_cpu] == -1 || cpu_times[which_cpu] > current_time)
            {
                continue;
            }
            // Nothing to run
            if (to_execute[which_cpu] == NULL)
            {
                if (queue->first == queue->last)
                {
                    continue;
                }
                int min_ready_time = place_first_element_ready(queue, which_cpu);

                if (min_ready_time > cpu_times[which_cpu])
                {
                    cpu_times[which_cpu] = min_ready_time;
                    continue;
                }

                to_execute[which_cpu] = dequeue(queue);
            }
            // Run
            cpu_times[which_cpu] = execute_elem(to_execute[which_cpu], current_time, which_cpu);
            augmented_task_t *executed = to_execute[which_cpu];
            to_execute[which_cpu] = NULL;

            for (int i = 0; i < executed->num_sons; i++)
            {
                if (!are_parents_scheduled(executed->sons[i]))
                {
                    continue;
                }
                int end_time = get_elem_ready_time(executed->sons[i], which_cpu) + executed->sons[i]->task->executionTime;

                if (end_time < stop_time && is_elem_ready(executed->sons[i], cpu_times[which_cpu], which_cpu) && to_execute[which_cpu] == NULL)
                {
                    to_execute[which_cpu] = executed->sons[i];
                }
                else
                {
                    enqueue(queue, executed->sons[i]);
                }
            }
        }

        int next_time = INT_MAX;
        for (int which_cpu = 0; which_cpu < numberOfProcessors; which_cpu++)
        {
            // Non allocated
            if (allocated_cpus[which_cpu] != -1 && cpu_times[which_cpu] < next_time)
            {
                next_time = cpu_times[which_cpu];
            }
        }
        if (current_time == next_time)
        {
            return;
        }
        current_time = next_time;
    }
}

/////////////////////////////
// INITIAL HELPERS //////////
/////////////////////////////

// Create a fake node to have a unique root in each dag
void add_roots_to_dag(int whichDag)
{
    task_t *root = new_task_t();
    root->taskID = -1;
    root->executionTime = 0;
    root->taskType = -1;
    root->next = NULL;
    root->is_scheduled = true;

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

/////////////////////////////
// QUICK SORT ///////////////
/////////////////////////////

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
task_t **ordered_tasks_criteria(int num_tasks, task_t **to_order, int (*criteria)(task_t *))
{
    task_t **result = malloc(sizeof(task_t *) * num_tasks);
    int ptr = 0;

    for (int i = 0; i < num_tasks; i++)
    {
        result[i] = to_order[i];
    }

    quickSort(result, 0, num_tasks - 1, criteria);

    return result;
}

task_t **order_tasks_list(int num_tasks, task_t *list, int (*criteria)(task_t *))
{
    task_t **result = malloc(sizeof(task_t *) * num_tasks);
    int ptr = 0;

    task_t *currentTask = list;
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

/////////////////////////////
// EXECUTIONS TIMES  ////////
/////////////////////////////

int compute_actual_execution_time(int procID, history_t history, augmented_task_t *elem)
{
    if (lookup_history(procID, elem), history)
    {
        return 9 * task->executionTime / 10;
    }
    else
        return task->executionTime;
}

int compute_estimation_completion_time(int numProc, int whichDag)
{
    if (numProc == 0)
    {
        return INT_MAX;
    }
    int execution_time = 0;
    int communication_time = 0;
    task_t *currentTask = input[whichDag]->firstTask;
    while (currentTask != NULL)
    {
        execution_time += currentTask->executionTime;
        for (int i = 0; i < currentTask->num_sons; i++)
        {
            communication_time += currentTask->sons[i]->transferTime;
        }
        currentTask = currentTask->next;
    }
    return (int)(((float)execution_time / numProc) + ((float)communication_time * (numProc - 1) / numProc));
}

/////////////////////////////
// AVG PARALLEL /////////////
/////////////////////////////

struct queue_item
{
    task_t *task;
    int depth;
    int start_time;
};

/*
int average_parallel_nodes(int dagId, int start_time, int stop_time)
{
    struct queue_item **queue = malloc(sizeof(struct queue_item *) * (input[dagId]->tasksCount + 1)); // Count the root
    for (int i = 0; i < input[dagId]->tasksCount; i++)
    {
        queue[i] = NULL;
    }

    int queue_start = 0;
    int queue_end = 0;

    struct queue_item *root_item = malloc(sizeof(struct queue_item));
    root_item->depth = 0;
    root_item->task = input[dagId]->root;
    root_item->start_time = start_time;
    queue[queue_end++] = root_item;

    int current_depth = 0;
    int start_depth = 0;
    int num_parallel_nodes = 0;
    int total_exec_time = 0;
    int result = 0;
    //int

    while ((queue_end - queue_start) > 0)
    {
        struct queue_item *item = queue[queue_start++];

        if (item->start_time > stop_time)
        {
            continue;
        }

        if (!item->task->is_scheduled)
        {
            if (item->depth == current_depth)
            {
                num_parallel_nodes++;
            }
            else
            {
                result += num_parallel_nodes; // a terme, ponderer par exec time
                num_parallel_nodes = 0;
                current_depth++;
            }
        }

        for (int i = 0; i < item->task->num_sons; i++)
        {
            bool already_there = false;
            for (int j = 0; j < queue_end; j++)
            {
                if (queue[j]->task->taskID == item->task->sons[i]->afterTask->taskID)
                {
                    already_there = true;
                }
            }
            if (already_there)
                continue;

            struct queue_item *next_item = malloc(sizeof(struct queue_item));
            next_item->depth = item->depth + 1;
            next_item->task = item->task->sons[i]->afterTask;

            if (is_ready(item->task->sons[i]->afterTask))
            {
                next_item->start_time = start_time;
                start_depth = item->depth;
            }
            else
            {
                next_item->start_time = item->start_time + item->task->executionTime;
            }
            queue[queue_end++] = next_item;
        }
    }

    for (int i = 0; i < input[dagId]->tasksCount; i++)
    {
        if (queue[i] != NULL)
        {
            free(queue[i]);
        }
    }
    free(queue);

    return (current_depth - start_depth) == 0 ? 1 : result / (current_depth - start_depth);
}
*/