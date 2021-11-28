#ifndef SCHEDULER_H__
#define SCHEDULER_H__

#include "utils.h"
#include "dag.h"

struct ProcessorSchedule{
    int numberOfTasks; // place number of tasks scheduled for this processor here
    int taskIDs[N]; // place IDs of tasks scheduled for this processor here, in order of their execution (start filling from taskIDs[0])
    int startTime[N]; // place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers)
    int exeTime[N]; // place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor
    // Added //
    task_t *tasks[N];
    int readyTime;
};

extern struct ProcessorSchedule output[numberOfProcessors];

void scheduler();

//utils

int compute_max_dag_execution_time(int dag_index);

// bool is_ready_to_start(int at_time, int on_dag, task_t *task);

// actual execution time from history
int compute_actual_execution_time(int procID, task_t *task);

// one PN schedules a full DAG without interruption
void PN_schedules_DAG(int PN_index, int dag_index);

#endif