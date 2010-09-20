#!/bin/bash
#PBS -l nodes=4:ib:ppn=8,walltime=10:00:00,os=centos53computeA
#PBS -N testpydr

MPIFLAGS="-mca btl_sm_num_fifos 7 -mca mpi_paffinity_alone 1 -mca btl_openib_eager_limit 32767 -np $(wc -l $PBS_NODEFILE | gawk '{print $1}') -machinefile $PBS_NODEFILE"

cd $PBS_O_WORKDIR

module load gcc/gcc-4.4.0
module load python       
module load hdf5/184-p1-v18-serial
source ~/ENV/bin/activate

python /home/dacaplan/projects/pydr/pydr.py -j $PBS_JOBID -s
