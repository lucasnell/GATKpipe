#!/usr/local/apps/anaconda/3-2.2.0/bin/python

"""Prepare reference fasta for GATK pipeline."""


import argparse as ap
import base
import subprocess as sp

'''
import subprocess as sp
sp.call('cd ~/uga/Python/GATKpipe && scp prepRef.py base.py \
lan@xfer2.gacrc.uga.edu:~/tools/GATKpipe', shell = True)
'''

# =====================
#  Setting up parser
# =====================

Parser = ap.ArgumentParser(description = 'Prepare reference fasta for GATK pipeline.')
Parser.add_argument('-r', '--reference', metavar = 'R', 
                    help = "Path to uncompressed reference fasta file.")

args = vars(Parser.parse_args())
reference = args['reference']

if __name__ == '__main__':
    sp.call(base.prepRef % reference)