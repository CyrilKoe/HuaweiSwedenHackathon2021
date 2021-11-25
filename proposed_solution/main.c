#include "utils.h"
#include "in_out.h"
#include "dag.h"
#include "scheduler.h"

#include <time.h>

int main(int argc, char *argv[]){
    clock_t begin, end;
    reader_function(argv[1]);
    begin = clock();
    scheduler(); // <- fill this function   
    end = clock();
    printer_function(argv[2], begin, end);
    return 0;
}