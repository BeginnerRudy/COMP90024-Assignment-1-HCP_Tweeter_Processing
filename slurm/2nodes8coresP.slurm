#!/bin/bash

#SBATCH --partition=physical
#SBATCH --time=00:10:00
#SBATCH --output=../output/2node8core-phy.out
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1

cd ../src
module load Python/3.6.1-intel-2017.u2
time mpirun python main.py ../../../bigTwitter.json
