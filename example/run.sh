#!/bin/sh
#
# Test run script for PyDR
# In this example, NAMD is run given a set of environment variables that are used to generate the NAMD conf script.
#
# Environment Variables:
#   $id
#   $sequence
#   $coordinate
#   $k
#   $pdb
#   $PBS_NODEFILE
#   $PBS_O_WORKDIR
#   $PBS_JOBID

BASE_DIR=`pwd`
SHM_DIR=/dev/shm/dacaplan
MPIRUN=/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/bin/mpirun
NAMD=~/bin/namd2

PDB=$BASE_DIR/$pdb

#MPIFLAGS="-mca btl_sm_num_fifos 7 -mca mpi_paffinity_alone 1 -mca btl_openib_eager_limit 32767 -np $(wc -l $PBS_NODEFILE | gawk '{print $1}') -machinefile $PBS_NODEFILE"
MPIFLAGS="-mca btl_sm_num_fifos 7 -mca mpi_paffinity_alone 1 -np $(wc -l $PBS_NODEFILE | gawk '{print $1}')"
export PATH=/home/dacaplan/ENV/bin:/home/dacaplan/bin:/project/pomes/dacaplan/gromacs/gromacs-git/exec/bin:/scinet/gpc/compilers/gcc/bin/:/usr/lib64/qt-3.3/bin:/usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:/usr/lpp/mmfs/bin:/opt/torque/bin:/opt/torque/sbin:/usr/lpp/mmfs/bin:/opt/torque/bin:/opt/torque/sbin:/scinet/gpc/intel/Compiler/11.1/056/bin/intel64/:/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/bin/:$PATH
export LD_LIBRARY_PATH=/scinet/gpc/tools/Python/Python262/lib:/project/pomes/dacaplan/gromacs/gromacs-git/exec/lib:/scinet/gpc/lib/mpfr/lib:/scinet/gpc/compilers/gcc/lib64:/scinet/gpc/compilers/gcc/lib:/scinet/gpc/intel/Compiler/11.1/056/lib/intel64/:/scinet/gpc/intel/Compiler/11.1/056/mkl/lib/em64t/:/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/lib:/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/lib/openmpi:$LD_LIBRARY_PATH
export PYTHONPATH=/home/dacaplan/lib/python:/home/dacaplan/lib/python2.6/site-packages:/home/dacaplan/lib/python2.4/site-packages:/scinet/gpc/tools/Python/Python262/lib/python2.6/:/scinet/gpc/tools/Python/Python262/lib/python2.6/lib-dynload/:/scinet/gpc/tools/Python/Python262/lib/python2.6/site-packages/:/scinet/gpc/tools/Python/Python262/lib/python2.6/site-packages/Scientific/linux2/:/scinet/gpc/tools/Python/Python262/lib/python2.6/lib-tk/

# Create the replica/sequence dir in the current directory (the dir containing this file)
OUTPUT_DIR=$BASE_DIR/$id/$sequence
echo "Creating replica/sequence dir $OUTPUT_DIR"
if [ -e "$OUTPUT_DIR" ]; then
    echo "$OUTPUT_DIR already exists, backing it up and creating a new one..."
    mv $OUTPUT_DIR $OUTPUT_DIR.backup
fi
mkdir -p $OUTPUT_DIR

if [ ! -e "$SHM_DIR" ]; then
    echo "Creating temporary directory $SHM_DIR..."
    mkdir -p $SHM_DIR
fi

echo "Chdir to $SHM_DIR"
cd $SHM_DIR

# depending on the sequence number, use the correct input files
if [ "$sequence" == "0" ]; then
    CONF=md.conf
else
    # setup the restart files if sequence > 0
    CONF=md.restart.conf
    # TODO: validate restart files
    let previous=$sequence-1
    echo "Using restart data from previous run: $previous"
    cp $OUTPUT_DIR/../$previous/restart.tar.gz $SHM_DIR/
    tar xzvf $SHM_DIR/restart.tar.gz
    rm $SHM_DIR/restart.tar.gz
fi
    
cp $PDB $SHM_DIR/md.pdb.gz
gunzip $SHM_DIR/md.pdb.gz

cp $BASE_DIR/md.psf $SHM_DIR/ 
# Generate the input files using the environment variables
cp $BASE_DIR/tclforces.tcl $SHM_DIR/
cat $BASE_DIR/$CONF | sed -e "s/__CHI1_K__/$k/" | sed -e "s/__CHI1_COORD__/$coordinate/" > $SHM_DIR/$CONF

RETURN_CODE=0

function analysis {
    ANALYSIS=/home/dacaplan/projects/labwork/mdanalysis/cco_analysis.py
    . /etc/bashrc
    module purge
    module load gcc/gcc-4.4.0
    module load python
    module load hdf5/184-p1-v18-serial
    source /home/dacaplan/ENV/bin/activate
    python $ANALYSIS -o $OUTPUT_DIR/../analysis.h5 md.psf md.pdb md.dcd
}

function finish {
    printf "Saving results from directory $SHM_DIR to $OUTPUT_DIR on "; date
    gzip md.log
    mv md.log.gz $OUTPUT_DIR/
    mv $SHM_DIR/md.*.conf $OUTPUT_DIR/

    if [ -e  "restart.coor" ]; then
        rm $SHM_DIR/restart.*.old
        tar zcf $OUTPUT_DIR/restart.tar.gz restart.*
        if [ -e "md.dcd" ]; then
            analysis
        fi
        #mv $SHM_DIR/md.dcd $OUTPUT_DIR/md.dcd
    else
        echo "No restart file! run must have crashed"
        RETURN_CODE=1
    fi

    printf "Saving results complete on "; date
    printf "Cleaning up temporary directory $SHM_DIR at "; date
    rm -rf $SHM_DIR
    printf "done at "; date
}

function trap_term {
    printf "Trapped term (soft kill) signal on "; date
    finish && exit $RETURN_CODE
}

trap "trap_term" TERM

printf "Starting run at "; date
echo "Running: $MPIRUN $MPIFLAGS $NAMD $SHM_DIR/$CONF >> $SHM_DIR/md.log"
if [ "$MPIRUN" == "" ]; then
    $NAMD +p4 $SHM_DIR/$CONF >> $SHM_DIR/md.log
else
    $MPIRUN $MPIFLAGS $NAMD $SHM_DIR/$CONF >> $SHM_DIR/md.log
fi
printf "Run ended cleanly at "; date

# TODO: run analysis (eg: extract dihedrals)

finish && exit $RETURN_CODE

