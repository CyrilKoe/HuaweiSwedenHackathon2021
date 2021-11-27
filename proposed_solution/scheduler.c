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
int compute_estimation_completion_time(int numProc, int whichDag);
bool is_ready(task_t* task);
bool schedule_task(tas_t* task_to_add, int PN_Id, int current_time);

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

    full_dag_solution();

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
            output[chosen_PN].exeTime[last_task_scheduled] = compute_actual_execution_time(chosen_PN, task_to_add);
            output[chosen_PN].taskIDs[last_task_scheduled] = task_to_add->taskID;
            output[chosen_PN].tasks[last_task_scheduled] = task_to_add;
            output[chosen_PN].numberOfTasks++;
            assert(last_task_scheduled < N);
        }
    }
}

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
        arrival_time = input[i]->arrivalTime;
        for (int j = 0; j< n_arrival_times; j++) {
            if (arrival_time == arrival_times[j]) {
                break;
            }
        }
        arrival_times[n_arrival_times] = arrival_time;
        n_arrival_times++;
    }
    
    int current_time = 0;
    int stop_time = 0;
    // Until all dags are passed    
    for (int i = 0; i < dagsCount; i++) {
        // Find the first DAG available
    
        int chosen_dag = -1;
        int min_max_start_time = INT_MAX;

        for (int j = 0; j < dagsCount; j++) {
            if(intput[j]->arrivalTime > current_time)
                continue;

            int max_start_time = input[j]->deadlineTime + intput[j]->arrivalTime - compute_max_dag_execution_time(j);
            if (!input[j]->is_scheduled && max_start_time < min_max_start_time) {
                min_max_start_time = max_start_time;
                chosen_dag = j;
            }
        }
        

        int nProcsAllocated = 0;
        while(compute_estimation_completion_time(nProcsAllocated, whichDag) > input[chosen_dag]->arrivalTime + input[chosen_dag]->deadlineTime) {
            nProcsAllocated++;
        }


        // schedule tasks between procs = DFS 
        task_t **queue_tasks_to_explore = malloc(sizeof(task_t*) * input[chosen_dag]->tasksCount);
        task_t **tasks_per_PN = malloc(sizeof(task_t*) * nProcsAllocated);
        int start_index_queue = -1;
        int queue_size = 0;
        // find a topological order for the DAG we want to schedule
        task_t **ordered_tasks_topo = ordered_tasks_criteria(input[chosen_dag]->tasksCount, chosen_dag, &get_remaining_time);

        
        // enqueue nProcsAllocatedNodes so that all PN start with a node 
        for (int i = 0; i < PN_Ids; i++) { 
            // more tyasks than nPNsallocated ofc
            queue_tasks_to_explore[start_index_queue++] = ordered_tasks_topo[i]; //enqueue 
            queue_size++;
        }
        
        
        free(ordered_tasks_topo);
        free(queue_tasks_to_explore);
    }
}

void DFS_N_PNs(int *PN_Ids, int nProcsAllocated, task_t **queue_tasks_to_explore, int *start_index_queue, int *queue_size, 
                                                            task_t **tasks_per_PN, int stop_time) {
    // as long as there is only one path, we move forward for every PN
    
    for (int i = 0; i < PN_Ids; i++) {
        task_t *current_task =  tasks_per_PN[i];
        int PN_Id = PN_Ids[i];
        // TODO CURRENT TIME LIE AU PARENT EN CAS DE DEADLOCK
        int current_time = output[PN_Id].startTime[output[PN_Id].numberOfTasks] + output[PN_Id].exeTime[output[PN_Id].numberOfTasks];
        schedule_task(current_task, PN_Id, current_time);

        int num_sons = current_task->num_sons;

        int sons_scheduled = 0;
        int sons_not_ready = 0;
        int no_time_for_this_son = 0;
        bool *sons_runnable = malloc(sizeof(bool) * num_sons); // list of sons, true if runnable
        int sons_not_runnable; 
        int first_schedulable = 0;
        for (int j = 0; j < num_sons; j++) {

            if (tasks_per_PN[i]->sons[j]->afterTask->is_scheduled) {
                sons_scheduled++;
            } else if (current_task->sons[j]->afterTask->executionTime + current_time > stop_time) {
                no_time_for_this_son++;
            } else if (!is_ready(current_task)) {
                sons_not_ready++;
            } else {
                sons_runnable[j] = true;
                continue;
            }
            first_schedulable++; // if this son is not schedulable, lets remember to schedule the first next schedulable son
            sons_runnable[j] = false;
        }
        int sons_not_runnable = sons_scheduled + sons_not_ready + no_time_for_this_son;
        int num_sons_runnable - num_sons - sons_not_runnable; 
        if (num_sons_runnable == 0) { 
            // dequeue
        } else {
            if (num_sons_runnable > 1) { 
                // queue the other tasks
                for (int k = first_schedulable + 1; k < num_sons; k++) {
                    queue_tasks_to_explore[start_index_queue+queue_size+1] = 
                }
            } 
            // go with this task 
            tasks_per_PN[i] = current_task->sons[first_schedulable]->afterTask;
        }
        
    }
    DFS_N_PNs(PN_Ids, nProcsAllocated, queue_tasks_to_explore, start_index_queue, queue_size, 
                                                            tasks_per_PN, stop_time);
}

bool schedule_task(tas_t* task_to_add, int PN_Id, int current_time) {
    int last_task_scheduled = output[PN_Id].numberOfTasks;
    output[PN_Id].startTime[last_task_scheduled] = current_time;
    output[PN_Id].exeTime[last_task_scheduled] = compute_actual_execution_time(PN_Id, task_to_add);
    output[PN_Id].taskIDs[last_task_scheduled] = task_to_add->taskID;
    output[PN_Id].tasks[last_task_scheduled] = task_to_add;
    output[PN_Id].numberOfTasks++;
}

bool is_ready(task_t* task) {
    for(int i = 0; i < task->num_parents; i++) {
        if(! task->parents[i]->is_scheduled)
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
int first_proc_available()
{
    int min = INT_MAX;
    int min_id = 0;
    for (int procID = 0; procID < numberOfProcessors; procID++)
    {
        if (output[procID].numberOfTasks == 0) {
            return procID;
        }
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

int compute_estimation_completion_time(int numProc, int whichDag) {
    int execution_time = 0;
    int communication_time = 0;
    task_t *currentTask = input[whichDag]->firstTask;
    while(currentTask != NULL) {
        execution_time += currentTask->executionTime;
        for(int i = 0; i < currentTask->num_sons; i++) {
            communication_time += currentTask->sons[i]->transferTime;
        }
    }
    return (int) ( ((float) execution_time / numProc) + ((float) communication_time * (numProc-1) / numProc) )
}