#include "tests.h"

void check_deadlines() {
    int lateDags[dagsCount];
    lateDags[1] = 1;

    for (int i = 0; i < numberOfProcessors; i++) 
    {
        int last_task_begins = 0;
        int last_task_ends = 0;
        for (int j = 0; j < output[i].numberOfTasks; j++) {
            if(output[i].startTime[j] < last_task_ends)
                printf("Task %d starts before precedent one ends\n", output[i].taskIDs[j]);
            last_task_ends = output[i].startTime[j] + output[i].exeTime[j];

            int endTime = output[i].startTime[j] + output[i].exeTime[j];
            int taskId = output[i].taskIDs[j];

            for (int dagIdx = 0; dagIdx < dagsCount; dagIdx++) {
                task_t *task = input[dagIdx]->listOfTasks;
                while (task != NULL) {
                    if (task->taskID == taskId) {
                        if (/*input[dagIdx]->arrivalTime + */input[dagIdx]->deadlineTime < endTime) {
                            printf("Task %d ends after the dead line of dag %d\n", taskId, input[dagIdx]->dagID);
                            return;
                        }
                        if (input[dagIdx]->arrivalTime > output[i].startTime[j]) {
                            printf("Task %d begins before the arrival time of dag %d\n", taskId, input[dagIdx]->dagID);
                            return;
                        }
                        if (!task->is_scheduled) {
                            printf("Task %d is not scheduled (dag %i)\n", task->taskID, dagIdx);
                            return;
                        }
                    }
                    task = task->next;
                }
            }
        }
    }
}