#!/bin/bash

#SBATCH --partition=physical
#SBATCH --time=00:10:00
#SBATCH --output=../output/1node8cores-phy.out
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1

cd ../src
module load Python/3.6.1-intel-2017.u2
time mpirun python main.py ~/bigTwitter.json ../data/language-codes-full_csv.csv
