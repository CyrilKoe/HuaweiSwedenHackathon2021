#include "in_out.h"

#include "utils.h"
#include "dag.h"
#include "scheduler.h"

#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <json-c/json.h>
#include <string.h> 
#include <math.h>
#include <time.h>

// auxiliary structures for output function (you dont have to read them)
struct TaskWithDeadline{
    int taskId;
    int dagDeadline;
};

struct TaskWithFinishTime{
    int taskId;
    int finishTime;
};

int cmp_aux(const void * arg11, const void * arg22){
    struct TaskWithDeadline *arg1 = (struct TaskWithDeadline *)arg11;
    struct TaskWithDeadline *arg2 = (struct TaskWithDeadline *)arg22;
    if((arg1->taskId) < (arg2->taskId)) return -1;
    if((arg1->taskId) == (arg2->taskId)) return 0;
    return 1;
}

int cmp_aux2(const void * arg11, const void * arg22){
    struct TaskWithFinishTime *arg1 = (struct TaskWithFinishTime *)arg11;
    struct TaskWithFinishTime *arg2 = (struct TaskWithFinishTime *)arg22;
    if((arg1->taskId) < (arg2->taskId)) return -1;
    if((arg1->taskId) == (arg2->taskId)) return 0;
    return 1;
}

void printer_function(char * filename, clock_t begin, clock_t end){
    // call this function once output table is filled, it will automaticly write data to an answer csv file in the correct format
    FILE *f = fopen(filename ,"w");
    for(int i=0;i<numberOfProcessors;i++){
        for(int j=0;j<output[i].numberOfTasks;j++){
            fprintf(f, "%d %d %d,", output[i].taskIDs[j], output[i].startTime[j], output[i].startTime[j] + output[i].exeTime[j]);
        }
        fprintf(f, "\n");
    }
    double worstMakespan = 0;
    for(int i=0;i<dagsCount;i++){
        struct task * current = input[i]->firstTask;
        while(current != NULL){
            worstMakespan += current->executionTime;
            current = current->next;
        }
        struct dependency * dep = input[i]->firstDependency;
        while(dep != NULL){
            worstMakespan += dep->transferTime;
            dep = dep->next;
        }
    }
    ll makespan = 0;
    for(int i=0;i<numberOfProcessors;i++){
        int count = output[i].numberOfTasks;
        if(count == 0) continue;
        if(output[i].startTime[count - 1] + output[i].exeTime[count - 1] > makespan) makespan = output[i].startTime[count - 1] + output[i].exeTime[count - 1];
    }
    fprintf(f, "%lld\n", makespan);

    double sumOfSquares = 0;
    double sum = 0;
    for(int i=0;i<numberOfProcessors;i++){
        double length = 0;
        for(int j=0;j<output[i].numberOfTasks;j++) length += output[i].exeTime[j];
        sumOfSquares += (double)(length * 1.0 / makespan) * (double)(length * 1.0 / makespan);
        sum += length / makespan;
    }
    sumOfSquares /= numberOfProcessors;
    sum /= numberOfProcessors;
    sum *= sum;
    sumOfSquares -= sum;
    double stdev = sqrt(sumOfSquares);
    fprintf(f, "%0.6lf\n", stdev);

    int taskNumber = 0;
    for(int i=0;i<dagsCount;i++){
        taskNumber += input[i]->tasksCount;
    }

    struct TaskWithDeadline table1[N];
    struct TaskWithFinishTime table2[N];
    int done = 0;
    for(int i=0;i<dagsCount;i++){
        struct task * now = input[i]->listOfTasks;
        while(now != NULL){
            struct TaskWithDeadline current;
            current.taskId = now->taskID;
            current.dagDeadline = input[i]->deadlineTime;
            table1[done++] = current;
            now = now->next;
        }
    }
    done = 0;
    for(int i=0;i<numberOfProcessors;i++){
        for(int j=0;j<output[i].numberOfTasks;j++){
            struct TaskWithFinishTime current;
            current.taskId = output[i].taskIDs[j];
            current.finishTime = output[i].startTime[j] + output[i].exeTime[j];
            table2[done++] = current;
        }
    }

    qsort(table1, taskNumber, sizeof(struct TaskWithDeadline), cmp_aux);
    qsort(table2, taskNumber, sizeof(struct TaskWithFinishTime), cmp_aux2);

    int missedDeadlines = 0;
    for(int i=0;i<taskNumber;i++){
        if(table1[i].dagDeadline < table2[i].finishTime) missedDeadlines++;
    }

    double costFunction = (double)makespan / worstMakespan * 10 + stdev;
    costFunction = 1 / costFunction;
    fprintf(f, "%0.3lf\n", costFunction);
    double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
    int spent = (int)(time_spent * 1000);
    fprintf(f, "%d\n", spent);
    fclose(f);
}

void reader_function(char * filename){ // This function reads input from the json file
    FILE * fp;
    char buffer[1024 * 1024]; 
    fp = fopen(filename, "r");
    fread(buffer, 1024 * 1024, sizeof(char), fp);
    fclose(fp);
    
    // gets DAGs descriptions one by one, then parse it into struct
    
    struct json_object * parsedJson = json_tokener_parse(buffer);
    json_object_object_foreach(parsedJson, key, val){
        enum json_type type;
        type = json_object_get_type(val);
        if(type == json_type_object){
            initialize(dagsCount);
            input[dagsCount]->tasksCount = 0;
            json_object_object_foreach(val, keyR, valR){
                enum json_type typeR;
                typeR = json_object_get_type(valR);
                if(typeR == json_type_object){
                    char buffer[20];
                    int taskID = atoi(strncpy(buffer, &keyR[4], 20));
                    
                    struct json_object * executionTime, * taskType;
                    json_object_object_get_ex(valR, "EET", &executionTime);
                    json_object_object_get_ex(valR, "Type", &taskType);
                    int exeTime = json_object_get_int(executionTime);
                    int type = json_object_get_int(taskType);
                    add_task_to_list(dagsCount, taskID, exeTime, type);
                    struct json_object * edges;
                    bool exists = json_object_object_get_ex(valR, "next", &edges);
                    if(exists){
                        json_object_object_foreach(edges, taskTo, time){
                            int transferTime = json_object_get_int(time);
                            char buffer[20];
                            int  afterID = atoi(strncpy(buffer, &taskTo[4], 20));
                            add_dependency_to_list(dagsCount, taskID, afterID, transferTime);
                        }
                    }
                }
            }
            struct json_object * type, * arrivalTime, * relativeDeadline;
            json_object_object_get_ex(val, "Type", &type);
            json_object_object_get_ex(val, "ArrivalTime", &arrivalTime);
            json_object_object_get_ex(val, "Deadline", &relativeDeadline);
            input[dagsCount]->dagType = json_object_get_int(type);
            input[dagsCount]->arrivalTime = json_object_get_int(arrivalTime);
            input[dagsCount]->deadlineTime = json_object_get_int(relativeDeadline) + input[dagsCount]->arrivalTime;
            char buffer[20];
            input[dagsCount]->dagID = atoi(strncpy(buffer, &key[3], 20));
            dagsCount++;
        }
    }
}