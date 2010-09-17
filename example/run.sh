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

BASE_DIR=`pwd`
SHM_DIR=/dev/shm/testpydr
MPIRUN=/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/bin/mpirun
NAMD=$HOME/bin/namd2

export PATH=/home/dacaplan/ENV/bin:/home/dacaplan/bin:/project/pomes/dacaplan/gromacs/gromacs-git/exec/bin:/scinet/gpc/compilers/gcc/bin/:/usr/lib64/qt-3.3/bin:/usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:/usr/lpp/mmfs/bin:/opt/torque/bin:/opt/torque/sbin:/usr/lpp/mmfs/bin:/opt/torque/bin:/opt/torque/sbin:/scinet/gpc/intel/Compiler/11.1/056/bin/intel64/:/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/bin/:$PATH
export LD_LIBRARY_PATH=/scinet/gpc/tools/Python/Python262/lib:/project/pomes/dacaplan/gromacs/gromacs-git/exec/lib:/scinet/gpc/lib/mpfr/lib:/scinet/gpc/compilers/gcc/lib64:/scinet/gpc/compilers/gcc/lib:/scinet/gpc/intel/Compiler/11.1/056/lib/intel64/:/scinet/gpc/intel/Compiler/11.1/056/mkl/lib/em64t/:/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/lib:/scinet/gpc/mpi/openmpi/1.4.1-intel-v11.0-ofed/lib/openmpi:$LD_LIBRARY_PATH

# test settings
MPIRUN=
NAMD=/usr/local/bin/namd2
SHM_DIR=/tmp/testrun

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

# depending on the sequence number, use the correct input files
if [ "$sequence" == "0" ]; then
    CONF=md.conf
    cp $OUTPUT_DIR/../md.pdb $SHM_DIR/md.pdb
else
    # setup the restart files if sequence > 0
    CONF=md.restart.conf
    # TODO: validate restart files
    let previous=$sequence-1
    echo "Using restart data from previous run: $previous"
    cp $OUTPUT_DIR/../$previous/restart.tar.gz $SHM_DIR/
    tar xzvf $SHM_DIR/restart.tar.gz
fi

cp $BASE_DIR/md.psf $SHM_DIR/ 
# Generate the input files using the environment variables
cp $BASE_DIR/tclforces.tcl $SHM_DIR/
cat $BASE_DIR/$CONF | sed -e "s/__CHI1_K__/$k/" | sed -e "s/__CHI1_COORD__/$coordinate/" > $SHM_DIR/$CONF

function finish {
    printf "Saving results from directory $SHM_DIR to $OUTPUT_DIR on "; date
    tar zcf $OUTPUT_DIR/restart.tar.gz $SHM_DIR/restart.*
    tar zcf $OUTPUT_DIR/md.log.gz $SHM_DIR/md.log
    mv $SHM_DIR/md.dcd $OUTPUT_DIR/md.dcd
    mv $SHM_DIR/md.*.conf $OUTPUT_DIR/
    printf "Saving results complete on "; date
    printf "Cleaning up temporary directory $SHM_DIR at "; date
    rm -rf $SHM_DIR
    printf "done at "; date
}

# function trap_term {
#     printf "Trapped term (soft kill) signal on "; date
#     finish && exit 0
# }
#
# TODO: catch the term signal (propagate it through the client)
# trap "trap_term" TERM

printf "Starting run at "; date
echo "Running: $MPIRUN $MPIFLAGS $NAMD $SHM_DIR/$CONF >> $SHM_DIR/md.log"
if [ "$MPIRUN" == "" ]; then
    $NAMD +p4 $SHM_DIR/$CONF >> $SHM_DIR/md.log
else
    $MPIRUN $MPIFLAGS $NAMD $SHM_DIR/$CONF >> $SHM_DIR/md.log
fi
printf "Run ended cleanly at "; date

# TODO: run analysis (eg: extract dihedrals)

finish && exit 0

