#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Basic commands used by multiple GATKpipe scripts."""



import functools
import os, sys

__author__ = 'Lucas Nell'


# Get assembly file from chromosome name
def getAssem(Chromosome):
    assemDict = {'X': 'chrX.fa',
                 'Y': 'chrY.fa',
                 'Xb9': 'chrXb9.fa',
                 '19': 'chr19.fa',
                 '19b9': 'chr19b9.fa'}
    return '/lustre1/lan/musGenome/fasta/' + assemDict[Chromosome]


# Return assembly fasta file, Subspecies, and Suffix from an ID
def GetObjs(ID):
    ID = ID.split('_')
    Subspp = ID[0]
    # Getting chromosome aligned to (getting rid of filtering suffixes)
    repl = ('q', ''), ('x', '')
    Chr = functools.reduce(lambda a, kv: a.replace(*kv), repl, ID[1])
    # Assembly
    Assembly = getAssem(Chr)
    # Directory for bam files
    Directory = str('/lustre1/lan/musDNA/%s/GATK' % Subspp)
    # Common components for later replacing parts of 'Command'
    Dict = {'ass': Assembly,
            'dir': Directory}
    return Assembly, Subspp, Chr, Directory, Dict


# File search for bam files
def GetBAMs(Directory, Subspp, FullSuff):
    allFiles = os.listdir(Directory)
    bamFiles = [x for x in allFiles if x.startswith(Subspp) and x.endswith(FullSuff)]
    return bamFiles



# Create new command AND RUN, using...
# input bam filename,
# old command string with %()s to replace, and
# dictionary from which to make %()s replacements
def DoComm(bam, cmd, dct, test=False):
    d = dct
    d['input'] = bam
    Comm = str(cmd % d)
    if test:
        return Comm
    else:
        os.system(Comm)
        return





# os.system('''scp /Users/lucasnell/uga/Python/GATKpipe/baseFuns.py \
# lan@xfer2.gacrc.uga.edu:~/GATKpipe''')p
