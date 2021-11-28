#!/bin/sh
mkdir submission
cd submission 
cp ../dag.c dag.cpp
cp ../dag.h .
cp ../in_out.c in_out.cpp
cp ../in_out.h .
cp ../main.c main.cpp
cp ../scheduler.c scheduler.cpp
cp ../scheduler.h .
cp ../utils.h .

codingame-merge -w . -main main.cpp -o merge.c 

cp ../answer*.csv .
rm answer0.csv 

rm *.cpp *.h 

cd ..

zip -r submission.zip submission 
rm -rf submission