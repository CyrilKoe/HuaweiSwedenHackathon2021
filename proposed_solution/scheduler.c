#include "scheduler.h"

#include "utils.h"
#include "dag.h"

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <assert.h>
#include <math.h>

/////////////////////////////
// ENUMS ////////////////////
/////////////////////////////

typedef struct augmented_task
{
    task_t *task;
    bool is_scheduled;
    int which_pn;
    int start_time;
    int exe_time;
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

typedef struct queue_dfs
{
    int first;
    int last;
    augmented_task_t *elems[tasksPerGraph];
} queue_dfs_t;

typedef struct scheduling_dag
{
    augmented_dag_t *aug_dag;
    queue_dfs_t queue;
    int cpus[numberOfProcessors];
} scheduling_dag_t;

typedef struct cpus_allocated
{
    int new_start_time[numberOfProcessors]; // For each dag on this cpu
    int dag_idx[numberOfProcessors];
} cpus_allocated_t;

typedef struct decision_list
{
    int size;
    scheduling_dag_t **sch_dag;
    int *max_start_time;
    int *remaining_time;
    float **cpu_usage; // size nb of proc
    int **cpus;
} decision_list_t;

struct ProcessorSchedule output[numberOfProcessors];

static void *safe_malloc(size_t n)
{
    void *p = malloc(n);
    if (!p)
    {
        fprintf(stderr, "Out of memory(%ul bytes)\n", (unsigned long)n);
        exit(EXIT_FAILURE);
    }
    return p;
}

void link_dependancies_to_tasks(int whichDag);
void add_roots_to_dag(int whichDag);

task_t **order_tasks_criteria(int num_tasks, task_t **to_order, int (*criteria)(task_t *));

int compute_estimation_completion_time(int numProc, int whichDag);
int average_parallel_nodes(int dagId, int start_time, int stop_time);

void shitty_sort(int *T, int N_arr);

augmented_task_t *new_augmented_task(task_t *task);
void remove_augmented_task(augmented_task_t *aug_task);
augmented_dag_t *new_augmented_dag(dag_t *dag);
void link_augmented_dag(augmented_dag_t *aug_dag);
augmented_dag_t *copy_augmented_dag(augmented_dag_t *origin);
void remove_augmented_dag(augmented_dag_t *aug_dag);
scheduling_dag_t *new_scheduling_dag(dag_t *dag);
void remove_scheduling_dag(scheduling_dag_t *sch_dag);
scheduling_dag_t *copy_scheduling_dag(scheduling_dag_t *origin);
void init_queue(queue_dfs_t *queue);
void remove_queue_dfs(queue_dfs_t *queue);
void enqueue(queue_dfs_t *queue, augmented_task_t *elem);
augmented_task_t *dequeue(queue_dfs_t *queue);
int place_first_element_ready(queue_dfs_t *queue, int cpu, int current_time, int stop_time);
bool lookup_history(augmented_task_t *element, int pn, augmented_task_t *history[historyOfProcessor][numberOfProcessors]);
int get_elem_ready_time(augmented_task_t *elem, int pn);
int are_parents_scheduled(augmented_task_t *elem);
int is_elem_ready(augmented_task_t *elem, int time, int pn);
int execute_elem(augmented_task_t *elem, int time, int pn, augmented_task_t *history[historyOfProcessor][numberOfProcessors], bool write);
bool multi_dfs(int allocated_cpus[numberOfProcessors], queue_dfs_t *queue, int start_time[numberOfProcessors], int stop_time, augmented_task_t *history[historyOfProcessor][numberOfProcessors], bool write, bool *has_executed);
int compute_serial_completion_time(augmented_dag_t *aug_dag);
int compute_cpu_running_time(augmented_dag_t *aug_dag, int cpu, int start_time, int stop_time);
decision_list_t *create_decision_list(int size);
void remove_decision_list(decision_list_t *list);
void add_to_decision_list(decision_list_t *list, scheduling_dag_t *original_dag, int start_time, int stop_time);
int add_cpu_to_first_decision(const decision_list_t *list, int which_cpu, int* which_dag, int start_time, int stop_time, cpus_allocated_t *cpus_allocated);
void sort_decision_list(decision_list_t *list);
void print_decision_list(decision_list_t *list, int start_time, int stop_time);

/////////////////////////////
// MAIN /////////////////////
/////////////////////////////

void scheduler()
{
    int num_tasks = 0;

    int *dag_arrivals = safe_malloc((1 + dagsCount) * sizeof(int));

    scheduling_dag_t **scheduling_dags = safe_malloc(sizeof(scheduling_dag_t *) * dagsCount);

    int max_task_time = 0;
    for (int i = 0; i < dagsCount; i++)
    {
        scheduling_dags[i] = NULL;
        dag_arrivals[i] = input[i]->arrivalTime;
        num_tasks += input[i]->tasksCount;
        link_dependancies_to_tasks(i);
        add_roots_to_dag(i);
        task_t *task = input[i]->firstTask;
        while (task != NULL)
        {
            if (task->executionTime > max_task_time)
            {
                max_task_time = task->executionTime;
            }
            task = task->next;
        }
    }
    printf("Max task time = %i\n", max_task_time);
    dag_arrivals[dagsCount] = INT_MAX;
    shitty_sort(dag_arrivals, dagsCount);

    int start_time = dag_arrivals[0];
    int stop_time = 0;
    int arrival_count = 0;

    augmented_task_t *history[historyOfProcessor][numberOfProcessors];
    for (int i = 0; i < historyOfProcessor; i++)
    {
        for (int j = 0; j < numberOfProcessors; j++)
        {
            history[i][j] = NULL;
        }
    }

    while (stop_time < INT_MAX)
    {

        // --------- PREPARE LIST DAGS ---------------------
        int n_dags_to_schedule = 0;

        // Find new dags ready
        for (int i = 0; i < dagsCount; i++)
        {
            if ((scheduling_dags[i] == NULL) && (input[i]->arrivalTime <= start_time))
            {
                scheduling_dags[i] = new_scheduling_dag(input[i]);
                arrival_count++;
            }
            if (scheduling_dags[i] != NULL && !scheduling_dags[i]->aug_dag->is_completed)
            {
                n_dags_to_schedule++;
            }
        }

        if (n_dags_to_schedule == 0) // if new start time is not the arrival timne of any new dag
        {
            if (arrival_count == dagsCount)
            {
                break;
            }
            start_time = dag_arrivals[arrival_count];
            continue;
        }
        
        cpus_allocated_t cpus_allocated;
        for(int i = 0; i < numberOfProcessors; i++) {
            cpus_allocated.new_start_time[i] = start_time;
            cpus_allocated.dag_idx[i]= -1;
        }

        stop_time = dag_arrivals[arrival_count];
        
        /*if(stop_time < start_time + max_task_time * 2) {
            stop_time = start_time + max_task_time * 2; //dag_arrivals[arrival_count];
        }*/

        //for every subdivision of [new_start_time, stop_time]
        bool has_executed = true;
        while(has_executed)
        {
            has_executed = false;

            if(n_dags_to_schedule == 0)
                break;

            
            for(int i = 0; i < numberOfProcessors; i++) {
                cpus_allocated.dag_idx[i]= -1;
            }

            decision_list_t *decision_list;
            // TODO STOP TIME SELECTION
            
            
            //start_time = new_start_time;
            printf("\n----- start %i - stop %i - dags %i/%i -----\n\n", start_time, stop_time, n_dags_to_schedule, arrival_count);

            // sorted array of dags by priority
            decision_list = create_decision_list(n_dags_to_schedule);

            // Now we are ready
            for (int i = 0; i < dagsCount; i++)
            {
                if (scheduling_dags[i] == NULL || scheduling_dags[i]->aug_dag->is_completed)
                {
                    continue;
                }
                add_to_decision_list(decision_list, scheduling_dags[i], start_time, stop_time);
            }

            // 

            int actually_allocated = 0;
            for (int which_cpu = 0; which_cpu < numberOfProcessors; which_cpu++)
            {
                sort_decision_list(decision_list);
                print_decision_list(decision_list, start_time, stop_time);
                int which_dag = -1;
                add_cpu_to_first_decision(decision_list, which_cpu, &which_dag, cpus_allocated.new_start_time[which_cpu], stop_time, &cpus_allocated);
                // CHECK IF WE ACTUALLY ALLOCATED
                cpus_allocated.dag_idx[which_cpu] = which_dag;
            }        

            sort_decision_list(decision_list);
            printf("-> starts [");
            for(int i = 0; i < numberOfProcessors; i++) {
                printf("%i-", cpus_allocated.new_start_time[i]);
            }
            printf("] end %i size %i : \n", stop_time, n_dags_to_schedule);

            print_decision_list(decision_list, start_time, stop_time);

            // at this point, have a list of cpus that were allocated to this dag


            //if (stop_time != INT_MAX)
            //    stop_time += 1;

            for (int i = 0; i < decision_list->size; i++)
            {
                scheduling_dag_t *actual_dag = decision_list->sch_dag[i];
                // METTRE A JOUR LES NEW START TIMES
                actual_dag->aug_dag->is_completed = multi_dfs(decision_list->cpus[i], &(actual_dag->queue), cpus_allocated.new_start_time, stop_time, history, true, &has_executed);
                if(actual_dag->aug_dag->is_completed)
                    n_dags_to_schedule--;
                //printf("\n");
            }

            //remove_decision_list(decision_list);
        }
        // Ending
        start_time = stop_time;
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
// AUGMENTED TASK ///////////
/////////////////////////////

augmented_task_t *new_augmented_task(task_t *task)
{
    augmented_task_t *result = safe_malloc(sizeof(augmented_task_t));
    result->task = task;
    result->which_pn = -1;
    result->start_time = -1;
    result->exe_time = -1;
    result->num_sons = task->num_sons;
    result->num_parents = task->num_parents;
    result->sons = safe_malloc(result->num_sons * sizeof(augmented_task_t *));
    for (int i = 0; i < result->num_sons; i++)
        result->sons[i] = NULL;
    result->parents = safe_malloc(result->num_parents * sizeof(augmented_task_t *));
    for (int i = 0; i < result->num_parents; i++)
        result->parents[i] = NULL;
    return result;
}

void remove_augmented_task(augmented_task_t *aug_task)
{
    free(aug_task->parents);
    free(aug_task->sons);
    free(aug_task);
}

/////////////////////////////
// AUGMENTED DAG ////////////
/////////////////////////////

augmented_dag_t *new_augmented_dag(dag_t *dag)
{
    augmented_dag_t *result = safe_malloc(sizeof(augmented_dag_t));
    result->dag = dag;
    result->is_completed = false;
    result->size = dag->tasksCount;
    // Deal with the root
    result->root = new_augmented_task(dag->root);
    // Deal with the other tasks
    task_t *task = dag->firstTask;
    int i = 0;
    while (task != NULL)
    {
        result->elems[i++] = new_augmented_task(task);
        task = task->next;
    }
    link_augmented_dag(result);
    return result;
}

void link_augmented_dag(augmented_dag_t *aug_dag)
{
    for (int j = 0; j < aug_dag->root->num_sons; j++)
        for (int k = 0; k < aug_dag->size; k++)
            if (aug_dag->root->task->sons[j]->afterTask->taskID == aug_dag->elems[k]->task->taskID)
                aug_dag->root->sons[j] = aug_dag->elems[k];

    for (int i = 0; i < aug_dag->size; i++)
    {
        for (int j = 0; j < aug_dag->elems[i]->num_sons; j++)
            for (int k = 0; k < aug_dag->size; k++)
                if (aug_dag->elems[i]->task->sons[j]->afterTask->taskID == aug_dag->elems[k]->task->taskID)
                    aug_dag->elems[i]->sons[j] = aug_dag->elems[k];
        for (int j = 0; j < aug_dag->elems[i]->num_parents; j++)
            for (int k = 0; k < aug_dag->size; k++)
                if (aug_dag->elems[i]->task->parents[j]->beforeTask->taskID == aug_dag->elems[k]->task->taskID)
                    aug_dag->elems[i]->parents[j] = aug_dag->elems[k];
    }
}

augmented_dag_t *copy_augmented_dag(augmented_dag_t *origin)
{
    augmented_dag_t *result = new_augmented_dag(origin->dag);
    result->is_completed = origin->is_completed;
    for (int i = 0; i < result->size; i++)
    {
        result->elems[i]->is_scheduled = origin->elems[i]->is_scheduled;
        result->elems[i]->start_time = origin->elems[i]->start_time;
        result->elems[i]->which_pn = origin->elems[i]->which_pn;
    }
    return result;
}

void remove_augmented_dag(augmented_dag_t *aug_dag)
{
    for (int i = 0; i < aug_dag->size; i++)
    {
        remove_augmented_task(aug_dag->elems[i]);
    }
    remove_augmented_task(aug_dag->root);
    free(aug_dag);
}

/////////////////////////////
// SCHEDULING DAG ///////////
/////////////////////////////

scheduling_dag_t *new_scheduling_dag(dag_t *dag)
{
    scheduling_dag_t *result = safe_malloc(sizeof(scheduling_dag_t));
    result->aug_dag = new_augmented_dag(dag);
    init_queue(&result->queue);
    for (int i = 0; i < result->aug_dag->root->num_sons; i++)
    {
        enqueue(&result->queue, result->aug_dag->root->sons[i]);
    }
    for (int i = 0; i < numberOfProcessors; i++)
    {
        result->cpus[i] = -1;
    }
    return result;
}

void remove_scheduling_dag(scheduling_dag_t *sch_dag)
{
    remove_augmented_dag(sch_dag->aug_dag);
    //remove_queue_dfs(sch_dag->queue);
    free(sch_dag);
}

scheduling_dag_t *copy_scheduling_dag(scheduling_dag_t *origin)
{
    scheduling_dag_t *result = safe_malloc(sizeof(scheduling_dag_t));
    for (int i = 0; i < numberOfProcessors; i++)
    {
        result->cpus[i] = origin->cpus[i];
    }
    result->aug_dag = copy_augmented_dag(origin->aug_dag);
    // Double copie queue
    result->queue.first = origin->queue.first;
    result->queue.last = origin->queue.last;
    int i = origin->queue.first;
    while (i != origin->queue.last)
    {
        for (int j = 0; j < result->aug_dag->size; j++)
        {
            if (origin->queue.elems[i]->task->taskID == result->aug_dag->elems[j]->task->taskID)
                result->queue.elems[i] = result->aug_dag->elems[j];
        }
        i++;
        if (i == tasksPerGraph)
            i = 0;
    }
    return result;
}

/////////////////////////////
// QUEUE_DFS ////////////////
/////////////////////////////

void init_queue(queue_dfs_t *queue)
{
    queue->first = 0;
    queue->last = 0;
    for (int i = 0; i < tasksPerGraph; i++)
    {
        queue->elems[i] = NULL;
    }
}

void remove_queue_dfs(queue_dfs_t *queue)
{
    //free(queue);
}

void enqueue(queue_dfs_t *queue, augmented_task_t *elem)
{
    queue->elems[queue->last++] = elem;
    if (queue->last == tasksPerGraph)
        queue->last = 0;
    assert(queue->first != queue->last);
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

// Place the first element to be ready on the front TODO ADD STOP TIME
int place_first_element_ready(queue_dfs_t *queue, int cpu, int current_time, int stop_time)
{
    assert(queue->first != queue->last);

    int current = queue->first;
    int min_ready_time = INT_MAX;
    int min_index = -1;
    while (current != queue->last)
    {
        int this_time = get_elem_ready_time(queue->elems[current], cpu);
        if (min_ready_time > this_time && queue->elems[current]->task->executionTime + current_time < stop_time)
        {
            min_ready_time = this_time;
            min_index = current;
        }
        current += 1;
        if (current == tasksPerGraph)
            current = 0;
    }
    if(min_index == -1) {
        return -1;
    }
    augmented_task_t *tmp = queue->elems[queue->first];
    queue->elems[queue->first] = queue->elems[min_index];
    queue->elems[min_index] = tmp;
    return min_ready_time;
}

/////////////////////////////
// DECISION LIST ////////////
/////////////////////////////

decision_list_t *create_decision_list(int size)
{
    decision_list_t *result = safe_malloc(sizeof(decision_list_t));
    result->size = size;
    //result->cpu_allocated = safe_malloc(size * sizeof(int));
    result->cpu_usage = safe_malloc(size * sizeof(float *));
    result->max_start_time = safe_malloc(size * sizeof(int));
    result->remaining_time = safe_malloc(size * sizeof(int));
    result->sch_dag = safe_malloc(size * sizeof(scheduling_dag_t *));
    result->cpus = safe_malloc(size * sizeof(int *));
    //result->current_time = current_time;
    //result->stop_time = stop_time;
    for (int i = 0; i < size; i++)
    {
        // result->cpu_allocated[i] = 0;
        result->max_start_time[i] = 0;
        result->remaining_time[i] = 0;
        result->sch_dag[i] = NULL;
        result->cpu_usage[i] = malloc(numberOfProcessors * sizeof(float));
        result->cpus[i] = malloc(sizeof(int) * numberOfProcessors);
        for (int j = 0; j < numberOfProcessors; j++)
        {
            result->cpus[i][j] = -1;
            result->cpu_usage[i][j] = -1.0f;
        }
    }
    return result;
}

void remove_decision_list(decision_list_t *list)
{
    //free(list->cpu_allocated);
    free(list->max_start_time);
    free(list->remaining_time);
    free(list->sch_dag);
    free(list);
}

int compute_margin(int deadline, int current_time, int stop_time, int completion_time, int n_cpus) {
    return deadline - stop_time - completion_time/n_cpus + (int) ((float)(deadline - current_time) * 0.2f);
}

void add_to_decision_list(decision_list_t *list, scheduling_dag_t *original_dag, int start_time, int stop_time)
{
    for (int i = 0; i < list->size; i++)
    {
        if (list->sch_dag[i] == NULL)
        {
            list->sch_dag[i] = original_dag;
            // list->cpu_allocated[i] = 0;
            list->max_start_time[i] = compute_margin(original_dag->aug_dag->dag->deadlineTime, start_time, stop_time, compute_serial_completion_time(original_dag->aug_dag), 1);
            list->remaining_time[i] = compute_serial_completion_time(original_dag->aug_dag);
            break;
        }
    }
}

// return stoptime or the end of a completely scheduled dag
// simulate DFS, allocate the CPU if better results 
int add_cpu_to_first_decision(const decision_list_t *list, int which_cpu, int* which_dag, int start_time, int stop_time, cpus_allocated_t *cpus_allocated)
{
    assert(list->size > 0);


    int dag_to_exec = -1;

    int min_deadline = INT_MAX; 
    // Then a dag that has a negative margin
    if (dag_to_exec == -1) {
        for (int i = 0; i < list->size; i++) // Find not respected deadline
        {
            if (list->max_start_time[i] < 0 && list->sch_dag[i]->aug_dag->dag->deadlineTime < min_deadline && list->remaining_time[i] > 0)
            {
                dag_to_exec = i;
                min_deadline = list->sch_dag[i]->aug_dag->dag->deadlineTime; 
            }
        }
    }
    // Then a dag that doesnt have cpu (in order of prio)
    if (dag_to_exec == -1)
    {
        bool found = false; 
        for (int i = 0; i < list->size; i++)
        {
            for (int j = 0 ; j < numberOfProcessors; j++)
            {
                if (list->cpus[i][j] != -1) {
                    found = true;
                    break;
                }
            }
            if (!found && list->remaining_time[i] > 0) {
                dag_to_exec = i;
                break;
            }
        }
    }
    // Then just take the first
    if (dag_to_exec == -1)
    {
        dag_to_exec = 0; // Give to smallest margin
    }

    scheduling_dag_t *copy = copy_scheduling_dag(list->sch_dag[dag_to_exec]);
    // Return dag index
    *which_dag = copy->aug_dag->dag->dagID;


    // Allocate CPUs
    int n_cpus = 0;
    int cpus[numberOfProcessors];
    for (int i = 0; i < numberOfProcessors; i++)
    {
        if (cpus_allocated->dag_idx[i] == *which_dag || which_cpu == i)
        {
            cpus[i] = i;
            n_cpus++;
        }
        else
        {
            cpus[i] = -1;
        }
    }
    // Init history
    augmented_task_t *history[historyOfProcessor][numberOfProcessors];
    for (int i = 0; i < historyOfProcessor; i++)
    {
        for (int j = 0; j < numberOfProcessors; j++)
        {
            history[i][j] = NULL;
        }
    }
    int last_time = list->max_start_time[dag_to_exec];
    // Multi dfs here 
    bool is_completed = copy->aug_dag->is_completed = multi_dfs(cpus, &(copy->queue), cpus_allocated->new_start_time, stop_time, history, false, NULL);

    list->remaining_time[dag_to_exec] = compute_serial_completion_time(copy->aug_dag);
    // Compute cpu usage
    bool cpu_underused = false;
    for (int i = 0; i < numberOfProcessors; i++)
    {
        if (cpus[i] != -1)
        {
            int run_time = compute_cpu_running_time(copy->aug_dag, i, start_time, stop_time);
            //printf("[cpu] %i/%i [run] %i [on] %i ", i, i, run_time, list->stop_time - list->current_time);
            float cpu_usage = -1.0f;
            if (run_time == 0)
            {
                cpu_usage = 0.0f;
                cpu_underused = true;
            }
            else
            {
                cpu_usage = (float)run_time / (float)(stop_time - start_time);
                if (cpu_usage < 0.3f)
                {
                    cpu_underused = true;
                }
            }

            if(cpu_underused && !copy->aug_dag->is_completed) {
                list->max_start_time[dag_to_exec] = INT_MAX;
                return -1;
            }

            list->cpu_usage[dag_to_exec][i] = cpu_usage;
            assert(cpu_usage <= 1.5f);
            //printf("\n");
        }
    }

    //list->cpu_allocated[dag_to_exec] += 1;

    int time_to_completion = compute_serial_completion_time(copy->aug_dag);
    int new_time = compute_margin(copy->aug_dag->dag->deadlineTime, start_time, stop_time, time_to_completion, n_cpus);
    list->max_start_time[dag_to_exec] = new_time;

    if (copy->aug_dag->is_completed || new_time == last_time || cpu_underused)
    {
        list->max_start_time[dag_to_exec] = INT_MAX;
        if (copy->aug_dag->is_completed)
        {
            int endTime = 0;
            for (int k = 0; k < copy->aug_dag->size; k++)
            {
                augmented_task_t *t = copy->aug_dag->elems[k];
                if (t->start_time + t->exe_time > endTime)
                {
                    endTime = t->start_time + t->exe_time;
                }
            }
        }
    }

    // Save the allocated cpus into the decision list
    for(int i = 0; i < numberOfProcessors; i++)
    {
        list->cpus[dag_to_exec][i] = cpus[i];
    }

    return 0;
}

void print_decision_list(decision_list_t *list, int start_time, int stop_time)
{
    for (int i = 0; i < list->size; i++)
    {
        printf("(%i) : [(r) %i, (m) %i, (d) %i {", list->sch_dag[i]->aug_dag->dag->dagID, list->remaining_time[i], list->max_start_time[i], list->sch_dag[i]->aug_dag->dag->deadlineTime);
        for (int j = 0; j < numberOfProcessors; j++)
        {
            if (list->cpus[i][j] != -1)
            {
                printf("(%i) %0.2f, ",list->cpus[i][j], list->cpu_usage[i][j]);
            }
        }
        printf("}] - ");
    }
    printf("\n");
}

void sort_decision_list(decision_list_t *list)
{
    int i, j;
    for (i = 0; i < list->size - 1; i++)
    {
        for (j = i + 1; j < list->size; j++)
        {
            if (list->max_start_time[j] < list->max_start_time[i] && !list->sch_dag[j]->aug_dag->is_completed)
            {
                scheduling_dag_t *temp_dag = list->sch_dag[i];
                int temp_max_start = list->max_start_time[i];
                int temp_remaining_time = list->remaining_time[i];
                float *temp_usages;
                int *temp_cpus;
                temp_usages = list->cpu_usage[i];
                temp_cpus = list->cpus[i];
                list->max_start_time[i] = list->max_start_time[j];
                list->remaining_time[i] = list->remaining_time[j];
                list->sch_dag[i] = list->sch_dag[j];
                list->cpus[i] = list->cpus[j];
                list->cpu_usage[i] = list->cpu_usage[j];
                list->max_start_time[j] = temp_max_start;
                list->remaining_time[j] = temp_remaining_time;
                list->sch_dag[j] = temp_dag;
                list->cpu_usage[j] = temp_usages;
                list->cpus[j] = temp_cpus;
            }
        }
    }
}

/////////////////////////////
// DFS HELPERS //////////////
/////////////////////////////

bool lookup_history(augmented_task_t *element, int pn, augmented_task_t *history[historyOfProcessor][numberOfProcessors])
{
    for (int j = historyOfProcessor - 1; j >= 0; j--)
    {
        if (history[j][pn] == NULL)
            return false;
        if (history[j][pn]->task->taskType == element->task->taskType)
            return true;
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
    if (input[elem->task->whichDag]->arrivalTime > time)
        return false;
    if (elem->num_parents == 0)
        return true;
    if (!are_parents_scheduled(elem))
        return false;
    return get_elem_ready_time(elem, pn) <= time;
}

int get_actual_exec_time(augmented_task_t *elem, int pn, augmented_task_t *history[historyOfProcessor][numberOfProcessors])
{
    if (lookup_history(elem, pn, history))
        return elem->task->executionTime * 9 / 10;
    else
        return elem->task->executionTime;
}

int execute_elem(augmented_task_t *elem, int time, int pn, augmented_task_t *history[historyOfProcessor][numberOfProcessors], bool write)
{
    if (write)
    {
        //printf("(%i-%i) ", pn, elem->task->taskID);
    }
    assert(is_elem_ready(elem, time, pn));
    assert(!elem->is_scheduled);
    elem->is_scheduled = true;
    elem->start_time = time;
    elem->which_pn = pn;

    int exetime = get_actual_exec_time(elem, pn, history);
    elem->exe_time = exetime;

    if (write)
    {
        //assert(time + exetime <= input[elem->task->whichDag]->deadlineTime);
        int n_write = output[pn].numberOfTasks;
        output[pn].taskIDs[n_write] = elem->task->taskID;
        output[pn].startTime[n_write] = time;
        output[pn].exeTime[n_write] = exetime;
        output[pn].numberOfTasks++;
        assert(!elem->task->is_scheduled);
        elem->task->is_scheduled = true;
    }

    return exetime;
}

/////////////////////////////
// DFS  /////////////////////
/////////////////////////////

bool multi_dfs(int _allocated_cpus[numberOfProcessors], queue_dfs_t *queue, int start_time[numberOfProcessors], int stop_time, augmented_task_t *history[historyOfProcessor][numberOfProcessors], bool write, bool *has_executed)
{
    int allocated_cpus[numberOfProcessors];
    int cpu_times[numberOfProcessors];
    augmented_task_t *to_execute[numberOfProcessors];
    int min_cpu_time = INT_MAX;
    for (int i = 0; i < numberOfProcessors; i++)
    {
        allocated_cpus[i] = _allocated_cpus[i];
        cpu_times[i] = start_time[i];
        to_execute[i] = NULL;
        if(cpu_times[i] < min_cpu_time) {
            min_cpu_time = cpu_times[i];
        }
    }

    int current_time = min_cpu_time;

    while (1)
    {
        for (int which_cpu = 0; which_cpu < numberOfProcessors; which_cpu++)
        {
            // Non allocated or busy
            if (allocated_cpus[which_cpu] == -1 || cpu_times[which_cpu] > current_time || cpu_times[which_cpu] >= stop_time)
            {
                continue;
            }
            cpu_times[which_cpu] = current_time;
            // Nothing to run
            if (to_execute[which_cpu] == NULL)
            {
                if (queue->first == queue->last)
                {
                    // Let the other deal with the current_time
                    cpu_times[which_cpu] = -1;
                    continue;
                }

                int min_ready_time = place_first_element_ready(queue, which_cpu, cpu_times[which_cpu], stop_time);
                if (min_ready_time == -1 || min_ready_time > cpu_times[which_cpu])
                {
                    cpu_times[which_cpu] = min_ready_time;
                    continue;
                }

                if (min_ready_time < cpu_times[which_cpu])
                {
                    min_ready_time = cpu_times[which_cpu];
                }

                int end_execution_time = min_ready_time + get_actual_exec_time(queue->elems[queue->first], which_cpu, history);
                if (end_execution_time >= stop_time)
                {
                    cpu_times[which_cpu] = -1;
                    continue;
                }

                to_execute[which_cpu] = dequeue(queue);
            }
            // Run
            cpu_times[which_cpu] = cpu_times[which_cpu] + execute_elem(to_execute[which_cpu], current_time, which_cpu, history, write);
            assert(cpu_times[which_cpu] <= stop_time);

            // Update the start time for the next iteration
            if(write) {
                *has_executed = true;
                start_time[which_cpu] = cpu_times[which_cpu];
            }

            augmented_task_t *executed = to_execute[which_cpu];
            for (int hist = 0; hist < historyOfProcessor - 1; hist++)
            {
                history[hist][which_cpu] = history[hist + 1][which_cpu];
            }
            history[historyOfProcessor - 1][which_cpu] = executed;

            to_execute[which_cpu] = NULL;

            for (int i = 0; i < executed->num_sons; i++)
            {
                if (!are_parents_scheduled(executed->sons[i]))
                {
                    continue;
                }

                int ready_time = get_elem_ready_time(executed->sons[i], which_cpu);
                // TODO RECONSIDER
                if (ready_time < cpu_times[which_cpu])
                {
                    ready_time = cpu_times[which_cpu];
                }

                int end_time = ready_time + get_actual_exec_time(executed->sons[i], which_cpu, history);

                if (ready_time <= cpu_times[which_cpu] && end_time < stop_time && is_elem_ready(executed->sons[i], cpu_times[which_cpu], which_cpu) && to_execute[which_cpu] == NULL)
                {
                    to_execute[which_cpu] = executed->sons[i];
                }
                else
                {
                    enqueue(queue, executed->sons[i]);
                }
            }
        }
        // Find next time
        int next_time = INT_MAX;
        bool no_tasks_to_do = true;
        for (int which_cpu = 0; which_cpu < numberOfProcessors; which_cpu++)
        {
            if (allocated_cpus[which_cpu] == -1 || cpu_times[which_cpu] == -1)
                continue;
            if (to_execute[which_cpu] != NULL)
                no_tasks_to_do = false;
            if (cpu_times[which_cpu] < next_time && cpu_times[which_cpu] > current_time)
                next_time = cpu_times[which_cpu];
        }
        // Nothing happening
        if (next_time >= stop_time || (current_time == next_time && no_tasks_to_do))
        {
            break;
        }
        current_time = next_time;
    }

    if (queue->first == queue->last)
        return true;
    return false;
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
    root->whichDag = whichDag;

    task_t *currentTask = input[whichDag]->firstTask;
    while (currentTask != NULL)
    {
        if (currentTask->num_parents == 0)
        {
            // Create a fake dependancy
            dependancy_t *dep = safe_malloc(sizeof(dependancy_t));
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
// CPU ALLOCATION ///////////
/////////////////////////////

// How much time spent on this graph on this cpu between start and stop?
int compute_cpu_running_time(augmented_dag_t *aug_dag, int cpu, int start_time, int stop_time)
{
    int result = 0;
    for (int i = 0; i < tasksPerGraph; i++)
    {
        if (aug_dag->elems[i] && aug_dag->elems[i]->which_pn == cpu)
        {
            if (aug_dag->elems[i]->start_time >= start_time)
            {
                int exe_time = aug_dag->elems[i]->exe_time;
                if (start_time + exe_time <= stop_time)
                {
                    result += exe_time;
                }
            }
        }
    }
    return result;
}

int compute_serial_completion_time(augmented_dag_t *aug_dag)
{
    int sum = 0;
    for (int i = 0; i < aug_dag->size; i++)
    {
        if (aug_dag->elems[i]->is_scheduled)
            continue;
        sum += aug_dag->elems[i]->task->executionTime;
    }
    return sum;
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
    task_t **result = safe_malloc(sizeof(task_t *) * num_tasks);

    for (int i = 0; i < num_tasks; i++)
    {
        result[i] = to_order[i];
    }

    quickSort(result, 0, num_tasks - 1, criteria);

    return result;
}

task_t **order_tasks_list(int num_tasks, task_t *list, int (*criteria)(task_t *))
{
    task_t **result = safe_malloc(sizeof(task_t *) * num_tasks);
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
    struct queue_item **queue = safe_malloc(sizeof(struct queue_item *) * (input[dagId]->tasksCount + 1)); // Count the root
    for (int i = 0; i < input[dagId]->tasksCount; i++)
    {
        queue[i] = NULL;
    }

    int queue_start = 0;
    int queue_end = 0;

    struct queue_item *root_item = safe_malloc(sizeof(struct queue_item));
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

            struct queue_item *next_item = safe_malloc(sizeof(struct queue_item));
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