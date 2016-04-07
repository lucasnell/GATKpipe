#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Perform joint genotyping on gVCF files produced by HaplotypeCaller.

With some minor arguments, this compiles the command and runs it.

Required ~ 3gb RAM per thread.

Takes around 10-20 minutes."""




import os
import argparse as ap

import baseFuns as lan


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
