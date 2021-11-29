#include "tests.h"

void check_deadlines() {

    int max_print = 10;
    for (int i = 0; i < dagsCount; i++)
    {
        task_t *task = input[i]->firstTask;
        while (task != NULL) {
            if(!task->is_scheduled && (max_print-- > 0)) {
                printf("dag %i task %i is not scheduled\n", input[i]->dagID, task->taskID);
            }
            task = task->next;
        }
    }

    for (int i = 0; i < numberOfProcessors; i++) 
    {
        for (int j = 0; j < output[i].numberOfTasks; j++) {
            int endTime = output[i].startTime[j] + output[i].exeTime[j];
            int taskId = output[i].taskIDs[j];

            for (int dagIdx = 0; dagIdx < dagsCount; dagIdx++) {
                task_t *task = input[dagIdx]->listOfTasks;
                while (task != NULL) {
                    if (task->taskID == taskId) {
                        if (input[dagIdx]->arrivalTime + input[dagIdx]->deadlineTime < endTime) {
                            printf("Task %d ends after the dead line of dag %d\n", taskId, input[dagIdx]->dagID);
                            return;
                        }
                        if (input[dagIdx]->arrivalTime > output[i].startTime[j]) {
                            printf("Task %d begins before the arrival time of dag %d\n", taskId, input[dagIdx]->dagID);
                            return;
                        }
                    }
                    task = task->next;
                }
            }
        }
    }
}