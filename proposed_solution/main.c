#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "dag.h"
#include "in_out.h"
#include "scheduler.h"
#include "utils.h"
#include "tests.h"

// Run me from the proposed_solution directory !!

int main(int argc, char *argv[]) {
  printf("\n--- Starting main ---\n");
  clock_t begin, end;
  char in_file[30] = "", out_file[30] = "";
  int i = atoi(argv[1]);
  sprintf(in_file, "testcases/test%i.json", i);
  sprintf(out_file, "answer%i.csv", i);
  printf("%s\n", in_file);
  fflush(stdout);
  system("pwd");
  reader_function(in_file);
  begin = clock();
  printf("\n--- Starting Scheduler ---\n");
  scheduler();  // <- fill this function
  end = clock();
  printf("\n--- End scheduler : time %f ---\n",
         (double)(end - begin) / CLOCKS_PER_SEC);

    check_deadlines();

  printer_function(out_file, begin, end);
  return 0;
}