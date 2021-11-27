#include "tests.h"

void check_deadlines() {
    int lateDags[dagsCount];
    lateDags[1] = 1;

    for (int i = 0; i < numberOfProcessors; i++) 
    {
        for (int j = 0; j < output[i].numberOfTasks; j++) {
            int endTime = output[i].startTime[j] + output[i].exeTime[j];
            int taskId = output[i].taskIDs[j];

            
            for (int dagIdx = 0; dagIdx < dagsCount; dagIdx++) {
                task_t *task = input[dagIdx]->listOfTasks;
                while ((task = task->next) != NULL) {
                    if (task->taskID == taskId) {
                        if (input[dagIdx]->arrivalTime + input[dagIdx]->deadlineTime < endTime) {
                            printf("Task %d ends after the dead line of dag %d\n", taskId, input[dagIdx]->dagID);
                        }
                    }
                }
            }
        }
    }
}