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
void full_dag_solution();
task_t **ordered_tasks_criteria(int num_tasks, int whichDag, int (*criteria)(task_t *));
void nth_proc_available(int *PN_IDs, int nProcsAllocated);
int compute_estimation_completion_time(int numProc, int whichDag);
bool is_ready(task_t* task);
void schedule_task(task_t* task_to_add, int PN_Id, int current_time);
task_t *peek_first_ready(task_t **queue_tasks_to_explore, int *start_index_queue, int *queue_size, int current_time);
void second_solution();
void DFS_N_PNs(int *PN_Ids, int nProcsAllocated, task_t **queue_tasks_to_explore, int *start_index_queue, int *queue_size, task_t **tasks_per_PN, int stop_time, int current_time);
int average_parallel_nodes(int dagId, int start_time, int stop_time);

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


    second_solution();

    // find an idle PN and ask it to schedule the DAG
    
}

// First solution
void full_dag_solution() {
    // Until all dags are passed    
    for (int i = 0; i < dagsCount; i++) {
        // Find the first DAG available
        int chosen_dag = -1;
        int min_arrival_time = INT_MAX;

        for (int j = 0; j < dagsCount; j++) {
            if (!input[j]->is_scheduled && input[j]->arrivalTime < min_arrival_time) {
                min_arrival_time = input[j]->arrivalTime;
                chosen_dag = j;
            }
        }

        //printf("Lets schedule dag %d and arrival time %d\n", input[chosen_dag]->dagID, input[chosen_dag]->arrivalTime);

        int chosen_PN = 0;
        //nth_proc_available(chosen_PNs, n);
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
            output[chosen_PN].exeTime[last_task_scheduled] = compute_actual_execution_time(chosen_PN, task_to_add);
            output[chosen_PN].taskIDs[last_task_scheduled] = task_to_add->taskID;
            output[chosen_PN].tasks[last_task_scheduled] = task_to_add;
            output[chosen_PN].numberOfTasks++;
            assert(last_task_scheduled < N);
        }
    }
}
//typedef struct task_queue {
//    int begin;
//} task_queue_t;

void second_solution() {
    /*
        for every dag, assign a number of processors needed, lets call this x
        then, DFS with x nodes walking in the graph, they share a queue and try to pick tasks without paying communication 
    */
    
    // compute array of arrival times 
    int arrival_time = 0; 
    int *arrival_times = malloc(sizeof(int)* dagsCount);
    int n_arrival_times = 0; 
    for (int i = 0; i < dagsCount; i++) {
        input[i]->queue_tasks_to_explore = NULL;
        input[i]->is_scheduled = false;
        input[i]->is_scheduled_in_interval = false;
        arrival_time = input[i]->arrivalTime;
        for (int j = 0; j< n_arrival_times; j++) {
            if (arrival_time == arrival_times[j]) {
                break;
            }
        }
        arrival_times[n_arrival_times] = arrival_time;
        n_arrival_times++;
    }
    
    int start_time = 0;
    
    int n_dags_to_schedule = 0;
    int n_dags_scheduled = 0;

    for (int j = 0; j < dagsCount; j++) {
        if(input[j]->arrivalTime <= start_time) {
            n_dags_to_schedule++;
        }
    }

    // Until all dags are passed    
    for (int i = 0; i < dagsCount; i++) {
        // Find the first DAG available
        int stop_time = INT_MAX;
        
        for (int j = 0; j < dagsCount; j++) {
            if(input[j]->arrivalTime > start_time && input[j]->arrivalTime < stop_time) {
                stop_time = input[j]->arrivalTime;
            }
        }
    
        int chosen_dag = -1;
        int min_max_start_time = INT_MAX;
        int next_start_time = INT_MAX;


        for (int j = 0; j < dagsCount; j++) {
            if (!input[j]->is_scheduled && !input[j]->is_scheduled_in_interval && input[j]->arrivalTime <= start_time){
                int max_start_time = input[j]->deadlineTime + input[j]->arrivalTime - start_time - compute_max_dag_execution_time(j); // margin
                if (max_start_time < min_max_start_time) {
                    min_max_start_time = max_start_time;
                    chosen_dag = j;
                }
            } 
        }

        input[chosen_dag]->is_scheduled_in_interval = true;

        n_dags_scheduled++;

        // we chose the DAg we are going to schedule 
        // input[chosen_dag]->is_scheduled = true; // todo: scheduled last task DAG
        

        int nProcsAllocated = 0;  
        int PN_Ids[numberOfProcessors];
        int late = 0; // time lost because allocated processor is late

        printf("dag %03i start_time %i arrival time %i duration %i deadline %i", input[chosen_dag]->dagID, start_time, input[chosen_dag]->arrivalTime, compute_max_dag_execution_time(chosen_dag), input[chosen_dag]->deadlineTime);
        printf("\naverage parallel nodes = %i\n", average_parallel_nodes(chosen_dag, start_time, stop_time));
        //while(compute_estimation_completion_time(nProcsAllocated, chosen_dag) > input[chosen_dag]->deadlineTime - 0*late) {
        
        for (int lls = 0; lls < 1 || (lls < average_parallel_nodes(chosen_dag, start_time, stop_time) && lls < 8); lls++) {
            nth_proc_available(PN_Ids, nProcsAllocated);
            nProcsAllocated++;
            assert(nProcsAllocated <= numberOfProcessors);

            for (int n = 0; n < nProcsAllocated; n++) {
                late += output[PN_Ids[n]].readyTime > input[chosen_dag]->arrivalTime ? 
                    output[PN_Ids[n]].readyTime - input[chosen_dag]->arrivalTime: 0;
            }
        }
    
        printf(" num proc %i : ", nProcsAllocated);
        for (int k = 0; k < nProcsAllocated; k++) {
            printf("%d ", PN_Ids[k]);
        }
        printf("\n");
        

        // schedule tasks between procs = DFS 
        if(input[chosen_dag]->queue_tasks_to_explore == NULL) {
            input[chosen_dag]->queue_tasks_to_explore = malloc(sizeof(task_t*) * input[chosen_dag]->tasksCount);
            input[chosen_dag]->start_index_queue = 0;
            input[chosen_dag]->queue_size = 0;
            // enqueue nProcsAllocatedNodes so that all PN start with a node 
            // more tyasks than nPNsallocated ofc
            for (int k = 0; k < input[chosen_dag]->root->num_sons; k++) {
                input[chosen_dag]->queue_tasks_to_explore[k] = input[chosen_dag]->root->sons[k]->afterTask; //enqueue 
                input[chosen_dag]->queue_size++;
            }
        }

        task_t **tasks_per_PN = malloc(sizeof(task_t*) * nProcsAllocated);
        for(int k = 0; k < nProcsAllocated; k++) {
            tasks_per_PN[k] = NULL;
        }

        DFS_N_PNs(PN_Ids, nProcsAllocated, input[chosen_dag]->queue_tasks_to_explore, &(input[chosen_dag]->start_index_queue), 
                                    &(input[chosen_dag]->queue_size), tasks_per_PN, stop_time, input[chosen_dag]->arrivalTime);
        
        free(tasks_per_PN);
        

        if (n_dags_scheduled == n_dags_to_schedule) {
            n_dags_to_schedule = 0;
            n_dags_scheduled = 0;

            start_time = stop_time;

            for (int j = 0; j < dagsCount; j++) {
                input[j]->is_scheduled_in_interval = false;
                if(input[j]->arrivalTime <= start_time && !input[j]->is_scheduled) {
                    n_dags_to_schedule++;
                }
            }
        }
    }
}





void print_queue(task_t **queue_tasks_to_explore, int *start_index_queue, int *queue_size) {
    printf("task: ");
    for(int i = *start_index_queue; i < *start_index_queue+*queue_size; i++) {
        printf("%i ", queue_tasks_to_explore[i]->taskID);
    }
    printf("\n");
}

void DFS_N_PNs(int *PN_Ids, int nProcsAllocated, task_t **queue_tasks_to_explore, int *start_index_queue, int *queue_size, 
                                                            task_t **tasks_per_PN, int stop_time, int current_time) {
    for (int i = 0; i < nProcsAllocated; i++) {
        task_t *current_task =  tasks_per_PN[i];
        int PN_Id = PN_Ids[i];
 
        if (output[PN_Id].readyTime > current_time) {
            continue;
        }

        //print_queue(queue_tasks_to_explore, start_index_queue, queue_size);
        
        if (current_task == NULL || current_task->is_scheduled) {
            // Nothing to dequeue
            if((current_task = peek_first_ready(queue_tasks_to_explore, start_index_queue, queue_size, current_time)) == NULL) {
                continue;
            }

        }
        
        schedule_task(current_task, PN_Id, current_time);
        //printf("task %i scheduled by cpu %i\n", current_task->taskID, PN_Id);

        int num_sons = current_task->num_sons;

        int sons_scheduled = 0;
        int sons_not_ready = 0;
        int no_time_for_this_son = 0;
        int first_schedulable =-1;
        for (int j = 0; j < num_sons; j++) {
            if (current_task->sons[j]->afterTask->is_scheduled) {
                sons_scheduled++;
            } else if (current_task->sons[j]->afterTask->executionTime + current_time > stop_time) {
                no_time_for_this_son++;
            } else if (!is_ready(current_task->sons[j]->afterTask)) {
                sons_not_ready++;
            } else {
                // arrive here if the son is runnable
                if (first_schedulable == -1){
                    first_schedulable = j; // if this son is not schedulable, lets remember to schedule the first next schedulable son
                    tasks_per_PN[i] = current_task->sons[first_schedulable]->afterTask;
                } else { // we already have a runnable son to contniue with, lets queue the other swons
                    queue_tasks_to_explore[(*start_index_queue)+(*queue_size)] = current_task->sons[j]->afterTask;
                    *queue_size += 1;
                }
                continue;
            }
        }
        
    }

    int min_next_time = INT_MAX;
    for (int i = 0; i < nProcsAllocated; i++) {
        int PN_Id = PN_Ids[i];
        if (output[PN_Id].readyTime > current_time && output[PN_Id].readyTime < stop_time) {
            if(output[PN_Id].readyTime < min_next_time) {
                min_next_time = output[PN_Id].readyTime;
            }
        }
    }

    if (min_next_time < INT_MAX) {
        DFS_N_PNs(PN_Ids, nProcsAllocated, queue_tasks_to_explore, start_index_queue, queue_size, tasks_per_PN, stop_time, min_next_time);
    }
}

task_t *peek_first_ready(task_t **queue_tasks_to_explore, int *start_index_queue, int *queue_size, int current_time) {
    if(*queue_size == 0) {
        return NULL;
    }
    for(int i = *start_index_queue; i < *queue_size + *start_index_queue; i++) {
        task_t *t = queue_tasks_to_explore[i];
        for (int j = 0; j < t->num_parents; j++) {
            task_t *parent = t->parents[j]->beforeTask;
            if (parent->endingTime > current_time) {
                continue;
            }
        }
        queue_tasks_to_explore[i] = queue_tasks_to_explore[*start_index_queue];
        (*start_index_queue) += 1;
        (*queue_size) -= 1;
        return t;
    }
    return NULL;
}

// Note we are note supposed to be ready to run before the current_time
void schedule_task(task_t * task_to_add, int PN_Id, int current_time) {
    // Include the communications times
    int max_time_task_is_ready = 0;
    for(int i = 0; i < task_to_add->num_parents; i++) {
        assert(task_to_add->parents[i]->beforeTask->is_scheduled);
        int time_parent_finishes = task_to_add->parents[i]->beforeTask->endingTime;
        if(task_to_add->parents[i]->beforeTask->whichPn != PN_Id) {
            time_parent_finishes += task_to_add->parents[i]->transferTime;
        }
        if(max_time_task_is_ready < time_parent_finishes) {
            max_time_task_is_ready = time_parent_finishes;
        }
    }
    // Check the max btw current_time or time_parent_finishes
    int time_to_start = (current_time > max_time_task_is_ready) ? current_time : max_time_task_is_ready;
    int numberOfTasks = output[PN_Id].numberOfTasks;
    output[PN_Id].startTime[numberOfTasks] = time_to_start;
    output[PN_Id].exeTime[numberOfTasks] = compute_actual_execution_time(PN_Id, task_to_add);
    output[PN_Id].taskIDs[numberOfTasks] = task_to_add->taskID;
    output[PN_Id].tasks[numberOfTasks] = task_to_add;
    output[PN_Id].readyTime = time_to_start + task_to_add->executionTime;
    task_to_add->is_scheduled = true;
    task_to_add->whichPn = PN_Id;
    task_to_add->endingTime = time_to_start + task_to_add->executionTime;
    output[PN_Id].numberOfTasks++;

    // set is scheduled if all tasks scheduled
    task_t *t = input[task_to_add->whichDag]->listOfTasks;
    while (t != NULL) {
        if (!t->is_scheduled) {
            return;
        }
        t = t->next;
    }
    input[task_to_add->whichDag]->is_scheduled = true;
}

bool is_ready(task_t* task) {
    for(int i = 0; i < task->num_parents; i++) {
        if(! task->parents[i]->beforeTask->is_scheduled)
            return false;
    }
    return true;
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
void nth_proc_available(int *PN_IDs, int nProcsAllocated)
{
    int min_ready_time = INT_MAX;
    for (int i = 0; i < numberOfProcessors; i++) {
        bool already_picked = false;
        for (int j = 0; j < nProcsAllocated; j++) {
            if (PN_IDs[j] == i) {
                already_picked = true; 
            }
        }
        if (!already_picked) {
            if (output[i].readyTime < min_ready_time) {
                PN_IDs[nProcsAllocated] = i;
                min_ready_time = output[i].readyTime;
            }
        }
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

task_t **order_tasks_list(int num_tasks, task_t *list, int (*criteria)(task_t *)) {
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


// UTILS 
int compute_max_dag_execution_time(int dag_index) {
    int exec_time = 0;
    task_t *task = input[dag_index]->listOfTasks;
    while (task != NULL) 
    {
        if (!task->is_scheduled) {
            exec_time += task->executionTime;
        }
        task = task->next;
    }

    return exec_time;
}

int compute_actual_execution_time(int procID, task_t *task) {
    if (lookup_history(procID, task)) {
        return 9 * task->executionTime / 10;
    } else return task->executionTime;
}

int compute_estimation_completion_time(int numProc, int whichDag) {
    if(numProc == 0) {
        return INT_MAX;
    }
    int execution_time = 0;
    int communication_time = 0;
    task_t *currentTask = input[whichDag]->firstTask;
    while(currentTask != NULL) {
        execution_time += currentTask->executionTime;
        for(int i = 0; i < currentTask->num_sons; i++) {
            communication_time += currentTask->sons[i]->transferTime;
        }
        currentTask = currentTask->next;
    }
    return (int) ( ((float) execution_time / numProc) + ((float) communication_time * (numProc-1) / numProc) );
}


struct queue_item {
    task_t *task;
    int depth;
    int start_time;  
};

int average_parallel_nodes(int dagId, int start_time, int stop_time) {
    struct queue_item ** queue = malloc(sizeof(struct queue_item *) * (input[dagId]->tasksCount + 1)); // Count the root
    for(int i = 0; i < input[dagId]->tasksCount; i++) {
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

    while((queue_end - queue_start) > 0) {
        struct queue_item *item = queue[queue_start++];

        if(item->start_time > stop_time) {
            continue;
        }
        
        if(!item->task->is_scheduled) {
            if (item->depth == current_depth) {
                num_parallel_nodes++;
            } else {
                result += num_parallel_nodes; // a terme, ponderer par exec time
                num_parallel_nodes = 0;
                current_depth++;
            }

        } 

        for(int i = 0; i < item->task->num_sons; i++) {
            bool already_there = false;
            for (int j = 0; j < queue_end; j++) {
                if (queue[j]->task->taskID == item->task->sons[i]->afterTask->taskID) {
                    already_there = true;
                }
            }
            if (already_there) continue; 

            struct queue_item *next_item = malloc(sizeof(struct queue_item));
            next_item->depth = item->depth+1;
            next_item->task = item->task->sons[i]->afterTask;

            if(is_ready(item->task->sons[i]->afterTask)) {
                next_item->start_time = start_time;
                start_depth = item->depth;
            } else {
                next_item->start_time = item->start_time + item->task->executionTime;
            }
            queue[queue_end++] = next_item;
        }
    }

    for(int i = 0; i < input[dagId]->tasksCount; i++) {
        if(queue[i] != NULL) {
            free(queue[i]);
        }
    }
    free(queue);

    return (current_depth - start_depth) == 0 ? 1 : result / (current_depth - start_depth);
}