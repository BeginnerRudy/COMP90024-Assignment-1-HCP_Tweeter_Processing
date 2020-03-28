#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=00:10:00
#SBATCH --output=2node8core-phy.out
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

module load Python/3.6.1-intel-2017-u2
time mpirun -np 8 python test.py
