#!/bin/bash

#SBATCH --time=06:00:00
#SBATCH --job-name=H5_to_GDF

###### #SBATCH --chdir=/home/s6119265/
#SBATCH --nodes=1
#SBATCH  --mem-per-cpu=4000
#SBATCH --ntasks=2

# send me mails on BEGIN, END, FAIL, REQUEUE, ALL,
# TIME_LIMIT, TIME_LIMIT_90, TIME_LIMIT_80 and/or TIME_LIMIT_50
#SBATCH --mail-type=ALL
#SBATCH --mail-user=a.koehler@hzdr.de
#SBATCH --array=150

##unset  XDG_RUNTIME_DIR

export PYTHONPATH="$HOME/anaconda3/"
export PATH="$PYTHONPATH/bin:$PATH"

iteration=${SLURM_ARRAY_TASK_ID}000
# copy this file to here from original data
input_file="simData_filtered_%T.h5"
# this is the output file that can be used for gpt etc.
#output_file="reduced_data-from-PIC-run006-${iteration}ts.gdf"

echo "iteration $iteration"
echo "filtering h5 file for selected macro particle IDs"
#python filter-particle-ids-of-h5.py ${iteration}
echo "staring reduction"
#cp -v simData_filtered_${iteration}.h5  particle_reduction/$input_file
#### file from run 006, 150000TS has 985862 particles
# ratio for N=5000: 0.9949282962524166
# ratio for N=100k: 0.8985659250483333
python particle_reduction/reduction_main.py -hdf ${input_file} -hdf_re reduced_data%Tts-N100000.h5 -iteration ${iteration} -ratio_deleted_particles 0.8985659250483333 -algorithm random
#mv -v particle_reduction/reduced_data${iteration}ts.h5 .
echo "writing txt file for GPT"
python hdf-to-txt.py  ${iteration}
#asci2gdf -o ${output_file} reduced-${iteration}ts.txt  
#rm reduced-${iteration}ts.txt 
echo "DONE"

