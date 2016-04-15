#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Select variants with SelectVariants. Extract fields with VariantsToTable.

Select a subset of variants from a larger callset, then extract specific fields from a
VCF file to a tab-delimited table.

Uses 4-9gb RAM per thread.
Takes 5-10 minutes.


Example submission script.

#PBS -S /bin/bash
#PBS -q batch
#PBS -N toTab_dom
#PBS -l nodes=1:ppn=8:AMD
#PBS -l walltime=10:00:00
#PBS -l mem=80gb
#PBS -M lucnell@gmail.com
#PBS -m ae

export py=${HOME}/GATKpipe/vcfToTab.py

$py -t 8 -d /lustre1/lan/musDNA/dom/GATK dom_Xb9jG.vcf:Xb9 dom_19b9jG.vcf:19b9

"""



import argparse as ap
from multiprocessing import Pool
# from time import strftime
import os

import base as lan


__author__ = 'Lucas Nell'


# Basic script
selTabComm = \
'''module load java/jdk1.7.0_67\n

export inFile=%(infile)s
export samp=%(samp)s
export ref=%(ref)s

outBase=%(out)s

java -jar /usr/local/apps/gatk/3.4.0/GenomeAnalysisTK.jar \\
-T SelectVariants \\
-R ${ref} \\
-V ${inFile} \\
-o ${outBase}.vcf \\
-sn ${samp}\n

java -jar /usr/local/apps/gatk/3.4.0/GenomeAnalysisTK.jar \\
-T VariantsToTable \\
-R ${ref} \\
-V ${outBase}.vcf \\
-F POS -GF GT -GF PL -GF GQ \\
-o ${outBase}.table

# gzip ${outBase}.vcf
# gzip ${outBase}.table

'''




# dict(ref = '/lustre1/lan/', infile = '', samp = '')


# =============================
# Setting up parser
# =============================

ScriptDescript = '''Select a subset of variants from a larger callset'''

Parser = ap.ArgumentParser(description = ScriptDescript)
Parser.add_argument('-d', '--directory',  type = str, metavar = 'D',
                    help = 'Full path to vcf file(s)', required = True)
Parser.add_argument('-t', '--threads',  type = int, metavar = 'T',
                    required = False, default = 1,
                    help = 'Max # threads to use (default = 1). It is not useful to'+\
                    ' provide threads > # samples in all files passed.')
Parser.add_argument('fileChrs', type = str, metavar = 'F', nargs = '+',
                    help = 'vcf file(s) that you would like to split by sample, ' + \
                        'along with the chromosome it is aligned to, separated by ":". '+\
                        'Example: "dom_Xb9jG.vcf:Xb9".')




# -----------------------------
# Now reading the arguments
# -----------------------------
args = vars(Parser.parse_args())

# Changing to vcf directory
os.chdir(args['directory'])

# Reading other parameters
vcfFiles = [x.split(':')[0] for x in args['fileChrs']]
Chrs = [x.split(':')[1] for x in args['fileChrs']]
maxCores = args['threads']


# Assembly files, derived from the chromosome name
Assemblies = [lan.getAssem(c) for c in Chrs]



def vcfNames(vcfFile):
    """Extract sample names from a vcf file."""
    begParts = ['#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT']
    with open(vcfFile, mode = 'rt') as f:
        for line in f:
            if line.startswith('#CHROM'):
                sampNames = [x for x in line.strip().split('\t') if x not in begParts]
                return sampNames


def makeBaseOut(f, s):
    baseOut = f.replace('_', s + '_').replace('jG', '').replace('.vcf', '')
    return baseOut




# So we don't have to run serial within files, I'm making lists of all samples and
# associated files and assemblies; indexes will align

allSamps = []
sampFiles = []
sampAssems = []
allOuts = []


for i in range(len(vcfFiles)):
    samps = vcfNames(vcfFiles[i])
    fls = [vcfFiles[i]] * len(samps)
    assms = [Assemblies[i]] * len(samps)
    outs = [makeBaseOut(x, y) for x,y in zip(fls, samps)]
    allSamps += samps
    sampFiles += fls
    sampAssems += assms
    allOuts += outs




def doSelect(j):
    """Simple function to run command using previously created lists."""
    dct = dict(infile = sampFiles[j],
               samp = allSamps[j],
               ref = sampAssems[j],
               out = allOuts[j]
               )
    Comm = str(selTabComm % dct)
    os.system(Comm)
    return



def cleanVCF(dirtyFile):
    """Gets rid of the unnecessary parts of names for samples.

    Note: must include old naming for chrs, as some were run before new scheme.
    Note 2: This has already been run on dom+cast vcf files, so no need to implement here.
    I kept it around simply for future reference."""
    getRid = ['_Xb9', '_19b9', '_X9', '_19-9', 'dom', 'cast']
    makeTo = [''] * 4 + ['D', '']
    outName = dirtyFile.replace('.vcf', '_clean.vcf')
    with open(dirtyFile, 'rt') as inFile, open(outName, 'wt') as outFile:
        for line in inFile:
            if line.startswith('#CHROM'):
                l = line
                for s in range(len(getRid)):
                    l = l.replace(getRid[s], makeTo[s])
                outFile.write(l)
            else:
                outFile.write(line)
    os.system('mv %s %s' % (outName, dirtyFile))
    return



# f = "dom_Xb9jG.vcf"
# s = vcfNames(f)[0]
# out = f.replace('_', s.split('_')[0].replace(
#     'dom', '').replace('cast', '') + '_').replace('jG', '').replace('.vcf', '')




if __name__ == '__main__':
    if maxCores > 1:
        # start 'maxCores' worker processes
        with Pool(processes = maxCores) as pool:
            pool.map(doSelect, range(len(allSamps)))
    # If it's not in parallel, might as well not use Pool, to avoid overhead
    else:
        for j in range(len(allSamps)):
            doSelect(j)






# os.system('''scp /Users/lucasnell/uga/Python/GATKpipe/vcfToTab.py \
# lan@xfer2.gacrc.uga.edu:~/GATKpipe''')
