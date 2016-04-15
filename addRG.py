#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Add read groups to bam files using PICARD AddOrReplaceReadGroups.

9 cores used 5.870788 Gb RAM and took 1 hr, 13 min."""


import argparse as ap
import base


'''
import subprocess as sp
sp.call('cd ~/uga/Python/GATKpipe && scp addRG.py base.py \
lan@xfer2.gacrc.uga.edu:~/tools/GATKpipe', shell = True)
'''

# =====================
#  Setting up parser
# =====================
ScriptDescript = 'Add read groups to bam files using PICARD AddOrReplaceReadGroups.'

Parser = ap.ArgumentParser(description = ScriptDescript)
Parser.add_argument('-c', '--cores', type = int, metavar = 'C', default = 1,
                    help = "Maximum number of cores to use. Defaults to 1.")
Parser.add_argument('files', metavar = 'F', nargs = '+',
                    help = "BAM input file(s).")

# =====================
# Reading the arguments
# =====================
args = vars(Parser.parse_args())
cores = args['cores']
files = args['files']
if files.__class__ == str:
    files = [files]

assert all([x.endswith('.bam') or x.endswith('.BAM') for x in files]), \
    'Not all input files are BAM files.'



# =====================
# Function to run command
# =====================

def runAddRG(filePath):
    
    """Run command to add read groups to BAM file and index new one."""
    
    directory, filename = base.splitPath(filePath)
    
    command = base.addRG % {'bam': filename}
    
    logFileName = filePath.replace('.bam', '_rG.log')
    
    # Run command and save output to log file
    base.cleanRun(commandString = command, logFile = logFileName,
                  workingDir = directory, logOpenMode = 'wt')
    
    return




# =====================
# Run command on all file(s)
# =====================

if __name__ ==  '__main__':
    base.poolRunFun(function = runAddRG, cores = cores, inerable = files)


