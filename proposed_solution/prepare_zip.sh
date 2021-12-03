#!/bin/sh
mkdir submission

cat utils_8cpu.txt > utils.h
make
./build/apps/App 1
./build/apps/App 2
./build/apps/App 3
./build/apps/App 4
cat utils_6cpu.txt > utils.h
make
./build/apps/App 5
./build/apps/App 6
./build/apps/App 7
./build/apps/App 8
./build/apps/App 9
cat utils_8cpu.txt > utils.h

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