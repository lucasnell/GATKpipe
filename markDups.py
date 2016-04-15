#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Mark duplicates in BAM files via PICARD MarkDuplicates.

Note: Multithreaded is only useful if you have tons of RAM (>=20 GB per thread). It may
be easiest to run an array job.

Uses ~20GB per thread, and took around 3 hrs for castaneus sample "castEiJ", which had 
an 18GB BAM file.
"""



import argparse as ap
import base



'''
import subprocess as sp
sp.call('cd ~/uga/Python/GATKpipe && scp markDups.py base.py \
lan@xfer2.gacrc.uga.edu:~/tools/GATKpipe', shell = True)
'''

# =====================
#  Setting up parser
# =====================
ScriptDescript = 'Mark duplicates in BAM files via PICARD MarkDuplicates.'

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

def runMarkDups(filePath):
    
    """Run command to mark duplicates in BAM file."""
    
    directory, filename = base.splitPath(filePath)
    
    command = base.markDups % {'bam': filename}
    
    logFileName = '_'.join(filePath.split('_')[:-1] + ['mD.log'])
    
    # Run command and save output to log file
    base.cleanRun(commandString = command, logFile = logFileName,
                  workingDir = directory, logOpenMode = 'wt')
    return




# =====================
# Run command on all file(s)
# =====================

if __name__ ==  '__main__':
    base.poolRunFun(function = runMarkDups, cores = cores, inerable = files)



