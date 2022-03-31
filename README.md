This repository contains the solution of Hilaire Bouaddi and Cyril Koenig for the Huawei Sweden University Challenge 2021.
The goal of this hackaton is to propose a multicore scheduling algorithm. More details can be found in the pdf.
The challenge lasted one week.

The proposed algorithm was written in C. It considers each job modelized by a directed acyclic graph of tasks. At each time of interest it removes all the jobs from the CPUs. It then tries allocates a new CPU to the most prioritary job and simulate
its scheduling based on a "multiagent depth first search". If this scheduling results in a good CPU usage the job receives this new CPU. We repeat these actions until CPUs are available.
The next time of interest is now the first time a CPU is idle, or the time of arrival of a new job.


