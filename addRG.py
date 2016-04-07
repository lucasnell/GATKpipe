#!/usr/local/apps/anaconda/3-2.2.0/bin/python


# Call variants with HaplotypeCaller
# With some minor arguments, this compiles the command and runs it

import sys
import multiprocessing as mp
from time import strftime


command = '''java -Xmx2g -jar /usr/local/apps/picard/1.135/dist/picard.jar \\
AddOrReplaceReadGroups \\
CREATE_INDEX=true \\
INPUT=${1} \\
OUTPUT=${1/.bam/P.bam} \\
RGID=LANE1 \\
RGLB=${1/.bam/} \\
RGPL=ILLUMINA \\
RGPU=ILLUMINA \\
RGSM=${1/.bam/}'''


comm2 = \
'''#PBS -S /bin/bash
#PBS -q batch
#PBS -N RG_d<SUFF>
#PBS -l nodes=1:ppn=1:AMD
#PBS -l walltime=20:00:00
#PBS -l mem=3gb

# -----------------------
# THE ONLY THINGS YOU NEED TO CHANGE BETWEEN *.BAM
export sub=dom
export suff=<SUFF>
# -----------------------

module load java/jdk1.7.0_67
export addRG=${HOME}/GATKpipe/addRG.sh
module load samtools/latest

cd /lustre1/lan/M_m_${sub}/BAM_${suff/q/}

for f in ${sub}*_${suff}.bam
do
    $addRG $f
    samtools index -b ${f/.bam/P.bam}
done

qsub ${HOME}/mDups/mD_d${suff}.sh

'''