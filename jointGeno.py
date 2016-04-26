#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Perform joint genotyping on gVCF files via GenotypeGVCFs.

Required ~ 3gb RAM per thread.

Takes around 10-20 minutes."""



import argparse as ap
import base

'''
import subprocess as sp
sp.call('cd ~/uga/Python/GATKpipe && scp jointGeno.py base.py \
lan@xfer2.gacrc.uga.edu:~/tools/GATKpipe', shell = True)
'''

# =====================
#  Setting up parser
# =====================
ScriptDescript = 'Joint genotyping via GATK GenotypeGVCFs.'

Parser = ap.ArgumentParser(description = ScriptDescript)
Parser.add_argument('-r', '--reference', metavar = 'R', required = True,
                    help = "Path to uncompressed reference fasta file. It is assumed " + \
                           "that all input files are aligned to this reference.")
# >>>>>
Parser.add_argument('-o', '--outputName', metavar = 'O', required = True, 
                    help = "Name of output file, not including extensions or " + \
                           "'_jG' suffix.")
Parser.add_argument('-c', '--cores', type = int, metavar = 'C', default = 1, 
                    help = "Maximum number of cores to use. Defaults to 1.")
# >>>>>
Parser.add_argument('-m', '--moreOptions', metavar = 'M', default = '', 
                    help = "A single string with additional options to pass " + \
                           "to GenotypeGVCFs. If it contains spaces, be sure " + \
                           "to wrap in double-quotes.")
# >>>>>
Parser.add_argument('files', metavar = 'F', nargs = '+',
                    help = "gVCF input file(s) that will be jointly genotyped.")


# =====================
# Reading the arguments
# =====================
args = vars(Parser.parse_args())
ref = args['reference']
outName = args['outputName']
cores = args['cores']
moreOpts = args['moreOptions'].strip() + ' '
files = args['files']
if files.__class__ == str:
    files = [files]

assert ref.endswith('.fa') or ref.endswith('.fasta'), \
    'Reference is not an uncompressed fasta file.'

# >>>>>
assert all([x.endswith('.g.vcf') for x in files]), \
    'Not all input files are gVCF files.'



# =====================
# Making cores list
# =====================

# `GenotypeGVCFs` allows multithreading, but if we specify 1 core, we'll skip the 
# `-nt` argument altogether.

if cores > 1:
    coreString = ''
else:
    coreString = '-nt %i' % cores


# =====================
# Making variant list
# =====================


varString = '--variant %s' % ' \\\n--variant '.join(files)



# =====================
# Function to run command
# =====================

def runJointGeno(filePath):
    
    """Run command to do joint genotyping on gVCF files."""
    
    directory, filename = base.splitPath(filePath)
    
    command = base.jointGeno % {'varS': varString, 'ref': ref, 'coreS': coreString, 
                                'out': filename, 'moreOpts': moreOpts}
    
    logFileName = filePath + '_jG.log'
    
    # Run command and save output to log file
    base.cleanRun(commandString = command, logFile = logFileName,
                  workingDir = directory, logOpenMode = 'wt')
    
    return



# =====================
# Run command on all files
# =====================

if __name__ ==  '__main__':
    runJointGeno(outName)


