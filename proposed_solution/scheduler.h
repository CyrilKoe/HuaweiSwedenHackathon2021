#ifndef SCHEDULER_H__
#define SCHEDULER_H__

#include "utils.h"

struct ProcessorSchedule{
    int numberOfTasks; // place number of tasks scheduled for this processor here
    int taskIDs[N]; // place IDs of tasks scheduled for this processor here, in order of their execution (start filling from taskIDs[0])
    int startTime[N]; // place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers)
    int exeTime[N]; // place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor
};

extern struct ProcessorSchedule output[numberOfProcessors];

void scheduler();

#endif