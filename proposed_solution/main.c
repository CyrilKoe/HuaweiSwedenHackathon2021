#include "utils.h"
#include "in_out.h"
#include "dag.h"
#include "scheduler.h"
#include <stdlib.h>

#include <time.h>

// Run me from the proposed_solution directory !!

int main(int argc, char *argv[]){
    clock_t begin, end;
    char in_file[30] = "testcases/test0.json", out_file[30] = "out.txt";
    system("pwd");
    reader_function(in_file);
    begin = clock();
    scheduler(); // <- fill this function   
    end = clock();
    printer_function(out_file, begin, end);
    return 0;
}