#!/bin/bash
#PBS -l nodes=${nodes}:ib:ppn=${ppn},walltime=${walltime}${pbs_extra}
#PBS -N ${job_name}

# $PBS_O_WORKDIR
# $PBS_JOBID

cd $job_dir

module load gcc/gcc-4.4.0
module load python       
module load hdf5/184-p1-v18-serial
source ~/ENV/bin/activate

python ${pydr_path} -j $PBS_JOBID --pbs-nodefile $PBS_NODEFILE >> logs/$PBS_JOBID.log 2>&1
