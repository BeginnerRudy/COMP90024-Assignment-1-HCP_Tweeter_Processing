#!/bin/bash

#SBATCH --partition=physical
#SBATCH --time=00:20:00
#SBATCH --output=../../outputOnlyRead/1node1core-phy.out
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1

cd ../../src
module load Python/3.6.1-intel-2017.u2
time mpirun python main_only_reading.py ~/bigTwitter.json
