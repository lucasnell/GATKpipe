#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Call variants with HaplotypeCaller.

Requires about ~12gb RAM per thread.

May take 1-2 days.


HaplotypeCaller can be parallel via `-nct ${cores}`, but...
"Many users have reported issues running HaplotypeCaller with the -nct argument, 
so we recommend using Queue to parallelize HaplotypeCaller instead of multithreading."
So be advised...
"""


'''
import subprocess as sp
sp.call('cd ~/uga/Python/GATKpipe && scp callVariants.py base.py \
lan@xfer2.gacrc.uga.edu:~/tools/GATKpipe', shell = True)
'''

import argparse as ap
import base


# =====================
#  Setting up parser
# =====================
ScriptDescript = 'Call variants with GATK HaplotypeCaller.'

Parser = ap.ArgumentParser(description = ScriptDescript)
Parser.add_argument('-r', '--reference', metavar = 'R', 
                    help = "Path to uncompressed reference fasta file. It is assumed " + \
                           "that all input BAM files are aligned to this reference.")
Parser.add_argument('-c', '--cores', type = int, metavar = 'C', default = 1, 
                    help = "Maximum number of cores to use. Defaults to 1.")
Parser.add_argument('files', metavar = 'F', nargs = '+',
                    help = "BAM input file(s).")


# =====================
# Reading the arguments
# =====================
args = vars(Parser.parse_args())
ref = args['reference']
cores = args['cores']
files = args['files']
if files.__class__ == str:
    files = [files]

assert ref.endswith('.fa') or ref.endswith('.fasta'), \
    'Reference is not an uncompressed fasta file.'

assert all([x.endswith('.bam') or x.endswith('.BAM') for x in files]), \
    'Not all input files are BAM files.'



# =====================
# Making cores list
# =====================

# Because `RealignerTargetCreator` allows multithreading for a single sample, we've add 
# the ability to specify more cores than samples, which will be passed to that step.

# For sample(s) that get 1 core, we'll skip the `-nct` argument altogether.

# Also have to adjust `cores` object for Pool function if it's larger than # files 

coreStrList, cores = base.makeCoreStringList(cores, files, 'nct')




# =====================
# Function to run command
# =====================

def runCallVariants(filePath, coreString):
    
    """Run command to call variants on BAM file."""
    
    directory, filename = base.splitPath(filePath)
    
    command = base.callVariants % {'bam': filename, 'ref': ref, 'corS': coreString}
    
    logFileName = '_'.join(filePath.split('_')[:-1] + ['cV.log'])
    
    # Run command and save output to log file
    base.cleanRun(commandString = command, logFile = logFileName,
                  workingDir = directory, logOpenMode = 'wt')
    
    return



# =====================
# Run command on all file(s)
# =====================

if __name__ ==  '__main__':
    base.poolRunFun(function = runCallVariants, cores = cores, 
                    inerable = zip(files, coreStrList))

