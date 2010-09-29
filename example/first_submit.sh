#!/bin/bash
#PBS -l nodes=1:ppn=8,walltime=20:00:00
#PBS -N 1m56WT0

cd $PBS_O_WORKDIR

module load gcc/gcc-4.4.0
module load python       
module load hdf5/184-p1-v18-serial
source ~/ENV/bin/activate

python /home/dacaplan/projects/pydr/pydr.py -j $PBS_JOBID --pbs-nodefile $PBS_NODEFILE -s >> logs/$PBS_JOBID.log 2>&1

