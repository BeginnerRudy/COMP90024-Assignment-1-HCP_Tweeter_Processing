#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=00:10:00
#SBATCH --output=8test2node8core-phy.out
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

module load Python/3.4.3-goolf-2015a
#module load Java/1.8.0_71
#module load mpj/0.44
#mpirun -np 4 01-hello-world
#mpirun -np 4 02-broadcast
time mpirun -np 8 python test.py
#javac -cp .:$MPJ_HOME/lib/mpj.jar HelloWorld.java
#mpirun.sh -np 4 HelloWorld
