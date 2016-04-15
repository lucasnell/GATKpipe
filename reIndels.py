#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Local realignment around indels via GATK RealignerTargetCreator and IndelRealigner.

Requires ~15gb RAM per thread for mice AND stickleback.

Takes from ~12 hrs to 2 days.
"""



'''
import subprocess as sp
sp.call('cd ~/uga/Python/GATKpipe && scp reIndels.py base.py \
lan@xfer2.gacrc.uga.edu:~/tools/GATKpipe', shell = True)
'''


import argparse as ap
import base


# =====================
#  Setting up parser
# =====================
ScriptDescript = 'Local realignment around indels via GATK RealignerTargetCreator ' + \
                 'and IndelRealigner.'

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

assert cores > 0 and 'int' in str(cores.__class__), '`cores` must be an integer > 0.'


# =====================
# Making cores list
# =====================

# Because `RealignerTargetCreator` allows multithreading for a single sample, we've add 
# the ability to specify more cores than samples, which will be passed to that step.

# For sample(s) that get 1 core, we'll skip the `-nt` argument altogether.

# Also have to adjust `cores` object for Pool function if it's larger than # files 

coreStrList, cores = base.makeCoreStringList(cores, files, 'nt')




# =====================
# Function to run command
# =====================

def runReIndels(filePath, coreString):
    
    """Run command to do local realignment around indels on BAM file."""
    
    directory, filename = base.splitPath(filePath)
    
    command = base.reIndels % {'bam': filename, 'ref': ref, 'corS': coreString}
    
    logFileName = '_'.join(filePath.split('_')[:-1] + ['rI.log'])
    
    # Run command and save output to log file
    base.cleanRun(commandString = command, logFile = logFileName,
                  workingDir = directory, logOpenMode = 'wt')
    
    return


# =====================
# Run command on all file(s)
# =====================

if __name__ ==  '__main__':
    base.poolRunFun(function = runReIndels, cores = cores, 
                    inerable = zip(files, coreStrList))



