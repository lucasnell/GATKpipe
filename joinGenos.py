#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Perform joint genotyping on gVCF files produced by HaplotypeCaller.

Required ~ 3gb RAM per thread.

Takes around 10-20 minutes."""



import argparse as ap
import base


# =====================
#  Setting up parser
# =====================
ScriptDescript = 'Local realignment around indels via GATK RealignerTargetCreator ' + \
                 'and IndelRealigner.'

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
                           "to GenotypeGVCFs.")
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
moreOpts = args['moreOptions']
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
    coreStr = ''
else:
    coreStr = '-nt %i ' % cores


# =====================
# Making variant list
# =====================


varStr = '--variant %s ' % ' \\\n--variant '.join(files)




# {'varStr': varStr, 'ref': ref, 'coreStr': coreStr, 'out': outName, 
# 'moreOpts': moreOpts}
Command = \
'''export reference=%(ref)s

module load java/latest\n

java -jar /usr/local/apps/gatk/latest/GenomeAnalysisTK.jar \\
-T GenotypeGVCFs \\
-R ${reference} \\
%(coreStr)s\\
%(varStr)s\\
%(moreOpts)s\\
-o %(out)s_jG.vcf'''

# Mike also added the following for stickleback...
'''-stand_emit_conf 10 -stand_call_conf 30'''


# Left off: below is from callVariants, which obv needs to be adapted to here.

# def runCallVariants(filePath, coreString):
#     
#     """Run command to call variants on BAM file."""
#     
#     directory, filename = base.splitPath(filePath)
#     
#     command = base.callVariants % {'bam': filename, 'ref': ref, 'corS': coreString}
#     
#     logFileName = '_'.join(filePath.split('_')[:-1] + ['cV.log'])
#     
#     # Run command and save output to log file
#     base.cleanRun(commandString = command, logFile = logFileName,
#                   workingDir = directory, logOpenMode = 'wt')
#     
#     return
# 
# 
# 
# # =====================
# # Run command on all file(s)
# # =====================
# 
# if __name__ ==  '__main__':
#     base.poolRunFun(function = runCallVariants, cores = cores, 
#                     inerable = zip(files, coreStrList))
























import os
import argparse as ap

import base as lan


__author__ = 'Lucas Nell'



###############################
#  Arguments needed...
###############################
# ID
# number of cores to use (optional)

###############################
#  Example dictionary for 'Command' below
###############################
# Dict = {'sub': 'cast',
#         'chr': 'Xb9',
#         'ass': '/lustre1/lan/MouseGenome/chrX_mm9.fa',
#         'cores': str(4),
#         'dir': '/lustre1/lan/M_m_cast/BAM_X9',
#         'vars': VariantString}  # See below for this object

###############################
#  Command
###############################
Command = \
'''module load java/jdk1.7.0_67\n

cd %(dir)s\n

java -jar /usr/local/apps/gatk/3.4.0/GenomeAnalysisTK.jar \\
-T GenotypeGVCFs \\
-R %(ass)s \\%(cores)s
%(vars)s
-o %(sub)s_%(chr)sjG.vcf'''






###############################
#  Read command line args
###############################

# Setting up parser
ScriptDescript = \
'''Perform joint genotyping on gVCF files produced by HaplotypeCaller
With some minor arguments, this compiles the command and runs it'''

Parser = ap.ArgumentParser(description = ScriptDescript)
Parser.add_argument('ID', type = str, metavar = 'I',
                    help = 'ID as based on naming scheme')
Parser.add_argument('-c', '--cores', type = int, metavar = 'C',
                    help = 'number of cores to use when running program', default = 1,
                    required = False)

# Now reading the arguments
args = vars(Parser.parse_args())
ID = args['ID']
Cores = args['cores']
# If cores set to 1, then we don't need this line for final command
if int(Cores) == 1:
    Cores = ''
else:
    Cores = str('\n-nt %s \\' % Cores)


###############################
#  Making other variables
###############################

Assembly, Subspp, Chr, Directory, Dict = lan.GetObjs(ID)

# Making combined string for variants
VariantList = lan.GetBAMs(Directory, Subspp, Chr + 'cV.g.vcf')

for v in range(len(VariantList)):
    VariantList[v] = str('--variant %s \\' % VariantList[v])
VariantString =  '\n'.join(VariantList)


###############################
#  Creating the new command
###############################

Dict['sub'] = Subspp
Dict['chr'] = Chr
Dict['cores'] = Cores
Dict['vars'] = VariantString

NewComm = str(Command % Dict)

if __name__ == '__main__':
    os.system(NewComm)




# os.system('''scp /Users/lucasnell/uga/Python/GATKpipe/joinGenos.py \
# lan@xfer2.gacrc.uga.edu:~/GATKpipe''')


# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # for testing
#
# ID = 'cast_X9'
#
# print(NewComm)
#
# VariantList = ['castH12_X9.g.vcf', 'castH28_X9.g.vcf', 'castH34_X9.g.vcf',
#                'castH15_X9.g.vcf']
# Cores='4'
#
#
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
